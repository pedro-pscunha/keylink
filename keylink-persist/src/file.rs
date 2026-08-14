//! The local-file adapter. One directory, one file per state.
//!
//! ```text
//! <root>/warehouse/sp-01.snapshot     the bytes
//! <root>/warehouse/sp-01.lock         the claim, held by an open file
//! ```
//!
//! # What it promises, exactly
//!
//! **The state survives a restart of the process, and a power cut, on this
//! machine.** It does not survive the loss of the disk, and it says nothing
//! about a second machine. That is the whole difference between this adapter and
//! one that talks to a bucket, and it is the reason to reach for the other one.
//!
//! # Atomicity is four steps, and the fourth is the one people skip
//!
//! 1. Write the bytes to a temporary file beside the target.
//! 2. `fsync` the temporary file, so its contents are on the disk.
//! 3. `rename` it over the target. POSIX makes that atomic: a reader sees the
//!    old file or the new one, never a mixture.
//! 4. **`fsync` the directory**, so the rename itself is on the disk.
//!
//! Without step 4 the rename lives in the directory's page cache and a power cut
//! can lose it, leaving the previous snapshot. That is not corruption, but it is
//! silently weaker than the claim this module makes, so the step is here and it
//! is why the platform note below exists.
//!
//! # The claim is a file lock, and that choice has a reason
//!
//! The obvious alternative is a lock file created with `create_new`, which
//! leaves a stale lock behind after a crash and needs an operator to clear it.
//! `File::try_lock` is released by the operating system when the process ends,
//! however it ends. A crashed writer never blocks its own replacement.
//!
//! It is an advisory lock **within one machine**. Two processes on two machines
//! sharing a network filesystem are not excluded by this and must not be
//! arranged.
//!
//! # End to end
//!
//! Store, claim, wrap, load, store. This example lives here rather than on
//! [`Versioned`](crate::versioned::Versioned) because it names `Files`, and a
//! doctest on the crate's feature-independent core that cannot compile without
//! the `file` feature is a coupling `.mise/tasks/features` exists to prevent —
//! one that end reported green for, because `--lib` excludes doctests.
//!
//! ```no_run
//! # use keylink_persist::file::Files;
//! # use keylink_persist::versioned::{Snapshot, Versioned};
//! # use keylink_persist::{Durable, RestoreError, Snapshots, StateId};
//! # struct Counter(u64);
//! # impl Durable for Counter {
//! #     const VERSION: u32 = 1;
//! #     fn snapshot(&self) -> Vec<u8> { self.0.to_le_bytes().to_vec() }
//! #     fn restore(_: &[u8]) -> Result<Self, RestoreError> { Ok(Self(0)) }
//! #     fn migrate(_: u32, _: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> { Ok(None) }
//! # }
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let files = Files::open("/var/lib/keylink").await?;
//! let id = StateId::new("counters/hits")?;
//!
//! let mut state = Versioned::new(files.claim(&id).await?);
//! let mut counter = state.load::<Counter>().await?.unwrap_or(Counter(0));
//!
//! counter.0 += 1;
//! state.store(&Snapshot::of(&counter)).await?;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::{Claim, Entries, EntryFault, EntryKey, Error, Held, Result, Snapshots, StateId};

/// Snapshots as files under one directory.
#[derive(Debug, Clone)]
pub struct Files {
    root: PathBuf,
    limit: u64,
}

impl Files {
    /// The default cap on one snapshot: 64 MiB.
    ///
    /// A judgement. Large enough for any state that belongs in memory, small
    /// enough that a runaway state hits a wall rather than a full disk.
    pub const DEFAULT_LIMIT: u64 = 64 * 1024 * 1024;

    /// Store snapshots under `root`, creating it if it is not there.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when the directory cannot be created.
    pub async fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        tokio::fs::create_dir_all(&root).await?;
        Ok(Self {
            root,
            limit: Self::DEFAULT_LIMIT,
        })
    }

    /// Refuse a write larger than this.
    ///
    /// Counted on what reaches this adapter, which for anything written through
    /// [`Versioned`](crate::versioned::Versioned) is the payload plus
    /// [`FRAME_LEN`](crate::versioned::FRAME_LEN). Headroom for one snapshot is
    /// therefore `bytes - FRAME_LEN`.
    ///
    /// # The floor
    ///
    /// **`FRAME_LEN` is the smallest limit that can ever store anything.**
    /// Below it the subtraction above is negative: `limit(0)` — a plausible
    /// default, and one typo away from a real number — leaves a store that
    /// refuses every write forever, empty ones included, with
    /// [`Error::TooLarge`] naming twelve bytes against a limit of zero.
    ///
    /// It is not rejected here, because this is a builder that returns `Self`
    /// and the alternatives are worse: clamping upward is a silent fallback,
    /// and panicking on a configuration value is a crash where a refused store
    /// already says the same thing at the same volume. So the floor is a
    /// documented number, and
    /// `a_limit_under_the_frame_refuses_even_an_empty_snapshot` in
    /// `keylink-persist/tests/versioned.rs` is what keeps this paragraph true.
    #[must_use]
    pub fn limit(mut self, bytes: u64) -> Self {
        self.limit = bytes;
        self
    }

    /// Where this state lives, and the lock that guards it.
    ///
    /// Built from segments the constructor already validated, so there is
    /// nothing to escape and nothing to check. The suffix is the caller's:
    /// `.snapshot` for one whole value, `.log` for a set of entries.
    ///
    /// **One lock for the base rather than one per suffix**, so a name cannot be
    /// claimed twice by asking for it through two different ports. Both ports
    /// mean "this state, written by one process", and that is one claim.
    fn paths(&self, id: &StateId) -> (PathBuf, PathBuf) {
        let mut base = self.root.clone();
        for segment in id.segments() {
            base.push(segment);
        }
        let mut lock = base.clone().into_os_string();
        lock.push(".lock");
        (base, PathBuf::from(lock))
    }

    /// Take the lock, which is the first thing both ports do.
    ///
    /// Returns the locked file — whose existence *is* the claim — with the base
    /// path and the directory holding it, because every caller needs all three
    /// and computing them twice is how they drift.
    async fn locked(&self, id: &StateId) -> Result<(std::fs::File, PathBuf, PathBuf)> {
        let (base, lock) = self.paths(id);
        let parent = base
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.root.clone());
        tokio::fs::create_dir_all(&parent).await?;

        let id = id.clone();
        // Opening and locking are blocking calls. `try_lock` in particular parks
        // nothing, but the open can, on a cold or networked filesystem.
        let file = tokio::task::spawn_blocking(move || {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&lock)?;
            match file.try_lock() {
                Ok(()) => Ok(file),
                // Every other kind of failure is an I/O fault and says so.
                Err(std::fs::TryLockError::WouldBlock) => Err(Error::Busy { id }),
                Err(std::fs::TryLockError::Error(err)) => Err(Error::Io(err)),
            }
        })
        .await
        .map_err(|join| Error::Io(std::io::Error::other(join)))??;
        Ok((file, base, parent))
    }
}

impl Snapshots for Files {
    type Claim = FileClaim;

    async fn claim(&self, id: &StateId) -> Result<FileClaim> {
        let (lock, base, parent) = self.locked(id).await?;
        let mut snapshot = base.into_os_string();
        snapshot.push(".snapshot");
        Ok(FileClaim {
            _lock: lock,
            snapshot: PathBuf::from(snapshot),
            parent,
            limit: self.limit,
        })
    }
}

/// Exclusive access to one state's file, held by an open, locked file.
///
/// The lock is released when this is dropped, and by the operating system if the
/// process ends first.
#[derive(Debug)]
pub struct FileClaim {
    /// Never read. Its existence *is* the claim: dropping it unlocks.
    _lock: std::fs::File,
    snapshot: PathBuf,
    parent: PathBuf,
    limit: u64,
}

impl Claim for FileClaim {
    async fn load(&self) -> Result<Option<Vec<u8>>> {
        match tokio::fs::read(&self.snapshot).await {
            Ok(bytes) => Ok(Some(bytes)),
            // Never stored is not a failure. It is the first run.
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(Error::Io(err)),
        }
    }

    async fn store(&mut self, bytes: &[u8]) -> Result<()> {
        let offered = bytes.len() as u64;
        if offered > self.limit {
            return Err(Error::TooLarge {
                offered,
                limit: self.limit,
            });
        }

        // The temporary name carries the process id, so two writers that somehow
        // got past the claim still cannot corrupt each other's temporary file.
        // They would still race on the rename, which is why the claim exists.
        let mut temp = self.snapshot.clone().into_os_string();
        temp.push(format!(".tmp.{}", std::process::id()));
        let temp = PathBuf::from(temp);

        let snapshot = self.snapshot.clone();
        let parent = self.parent.clone();
        let bytes = bytes.to_vec();

        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            use std::io::Write as _;

            // 1 and 2: write it, then put it on the disk.
            let mut file = std::fs::File::create(&temp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);

            // 3: the atomic step. A reader sees the old bytes or the new ones.
            if let Err(err) = std::fs::rename(&temp, &snapshot) {
                // Leave nothing behind on a failed replace. The previous
                // snapshot is untouched, which is the promise `store` makes
                // about its own failure.
                let _ = std::fs::remove_file(&temp);
                return Err(err);
            }

            // 4: the step people skip. Without it the rename can be lost to a
            // power cut and the previous snapshot comes back — which is not
            // corruption, but it is weaker than what this module claims.
            sync_dir(&parent)
        })
        .await
        .map_err(|join| Error::Io(std::io::Error::other(join)))??;

        Ok(())
    }
}

// ───────────────────────────────────────────── entries, as one append-only log

/// What a set of entries starts with, so that anything else is a loud error
/// rather than a payload read as garbage. The trailing byte is the layout
/// version: change the layout, change the byte.
const LOG_MAGIC: [u8; 8] = *b"keylink\x01";

/// This entry replaces whatever was under its key.
const OP_PUT: u8 = 1;

/// This entry's key holds nothing from here on.
const OP_REMOVE: u8 = 2;

/// One operation byte and two big-endian `u32` lengths.
const RECORD_HEADER: usize = 1 + 4 + 4;

/// One record's bytes: the operation, the two lengths, then the two payloads.
fn encode(op: u8, key: &EntryKey, value: &[u8]) -> Vec<u8> {
    let key = key.as_bytes();
    let mut record = Vec::with_capacity(RECORD_HEADER + key.len() + value.len());
    record.push(op);
    // `as` is wrong here and `try_from` is right: a length that did not fit
    // would be written truncated and read back as a different record.
    // `EntryKey::MAX_LEN` and `Files::limit` both bound this well below
    // `u32::MAX`, so the fallback is unreachable and still typed.
    record.extend_from_slice(&u32::try_from(key.len()).unwrap_or(u32::MAX).to_be_bytes());
    record.extend_from_slice(&u32::try_from(value.len()).unwrap_or(u32::MAX).to_be_bytes());
    record.extend_from_slice(key);
    record.extend_from_slice(value);
    record
}

/// Read a log back into the set it describes.
///
/// Last write wins, a remove takes its key out, and the surviving entries come
/// back in the order they were last written in.
///
/// **A short read at the end stops the walk in silence.** That is a `put` that
/// was interrupted by the loss of the process, and the caller's `put` never
/// resolved, so the caller already treats it as unwritten. Refusing the whole
/// log for it would turn every crash into an unrecoverable state, which is the
/// opposite of what a log is for.
fn replay(id: &StateId, bytes: &[u8]) -> Result<Vec<(EntryKey, Vec<u8>)>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let fault = |fault| Error::NotALog {
        id: id.clone(),
        fault,
    };
    if bytes.len() < LOG_MAGIC.len() || bytes[..LOG_MAGIC.len()] != LOG_MAGIC {
        return Err(fault(EntryFault::WrongMagic));
    }

    // Tombstoned in place rather than removed, so one pass stays linear: a key
    // written twice frees its first slot and takes a new one at the end.
    let mut slots: Vec<Option<(EntryKey, Vec<u8>)>> = Vec::new();
    let mut index: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut at = LOG_MAGIC.len();

    while at + RECORD_HEADER <= bytes.len() {
        let op = bytes[at];
        // **A run of zeros to the end of the file is not a record.** A
        // size-extending append can reach the disk ahead of the bytes it was
        // extended for, so a power cut mid-`put` leaves a zero-filled tail
        // rather than a short one — and a zero op with zero lengths passes
        // every length check while naming an empty key. Read as a record it
        // refuses the whole log, which would leave a service unable to start
        // over the one entry its own caller already treats as unwritten.
        //
        // Checked as "the rest is zeros" rather than as "the last record", so a
        // zero op in the middle of a file that continues is still the loud
        // `UnknownOperation` it should be.
        if op == 0 && bytes[at..].iter().all(|byte| *byte == 0) {
            break;
        }
        if op != OP_PUT && op != OP_REMOVE {
            return Err(fault(EntryFault::UnknownOperation { op }));
        }
        let key_len = read_len(&bytes[at + 1..at + 5]);
        let value_len = read_len(&bytes[at + 5..at + 9]);
        let body = at + RECORD_HEADER;
        // `checked_add` because both lengths are read off the disk: this is
        // sound on 64-bit and wraps on a 32-bit target in a release build, and
        // the guard below would then pass on a wrapped value.
        let Some(end) = key_len
            .checked_add(value_len)
            .and_then(|span| body.checked_add(span))
        else {
            break;
        };
        if end > bytes.len() {
            break;
        }
        let key = EntryKey::new(&bytes[body..body + key_len])
            .map_err(|reason| fault(EntryFault::BadKey(reason)))?;
        let taken = index.insert(key.as_bytes().to_vec(), slots.len());
        if let Some(previous) = taken {
            slots[previous] = None;
        }
        // Exhaustive against the two operations checked above, so a third one
        // added without an arm here is a compile error rather than a silent
        // skip.
        match op {
            OP_PUT => slots.push(Some((key, bytes[body + key_len..end].to_vec()))),
            _ => {
                index.remove(key.as_bytes());
                slots.push(None);
            }
        }
        at = end;
    }

    Ok(slots.into_iter().flatten().collect())
}

/// One big-endian `u32`, as a `usize`.
///
/// The slice is always four bytes at every call site, and `unwrap_or` rather
/// than `expect` because this crate denies both `expect` and `unwrap` outside
/// tests. A malformed length cannot be produced here: it is read back exactly as
/// [`encode`] wrote it, and a short read is caught by the bounds check above.
fn read_len(four: &[u8]) -> usize {
    let mut bytes = [0_u8; 4];
    bytes.copy_from_slice(&four[..4]);
    u32::from_be_bytes(bytes) as usize
}

/// Entries as append-only logs under one directory, one log per state.
///
/// [`Files`] carries **both** ports: [`Snapshots::claim`] replaces one blob
/// wholesale and [`Entries::hold`] keys many entries, under one root, one lock
/// protocol and one limit.
///
/// There used to be an `EntryFiles(Files)` newtype here, and it existed for one
/// reason: both traits named their operation `claim`, so a concrete type
/// carrying both made `files.claim(&id)` ambiguous (`E0034`) for any caller
/// that had imported both. Renaming the entry port's verb removed the
/// collision, and the newtype with it — it was a large interface over a thin
/// implementation, where `open` and `limit` each delegated and a third setter
/// on `Files` would have needed a third delegate.
///
/// The two ports share one root safely. A state's lock guards its base name, so
/// `treasury/ledger` held here excludes `treasury/ledger` claimed as a
/// snapshot, which is the answer a caller wants: one state, one writer.
impl Entries for Files {
    type Held = FileLog;

    /// Take hold of this state's entry log, creating it if it is not there.
    async fn hold(&self, id: &StateId) -> Result<FileLog> {
        let (lock, base, parent) = self.locked(id).await?;
        let mut log = base.into_os_string();
        log.push(".log");
        let log = PathBuf::from(log);

        let opening = log.clone();
        let creating = parent.clone();
        let handle = tokio::task::spawn_blocking(move || -> std::io::Result<std::fs::File> {
            use std::io::Write as _;

            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&opening)?;
            // A fresh log gets its magic, and both the magic and the directory
            // entry that holds it reach the disk before the first entry does.
            if file.metadata()?.len() == 0 {
                file.write_all(&LOG_MAGIC)?;
                file.sync_all()?;
                sync_dir(&creating)?;
            }
            Ok(file)
        })
        .await
        .map_err(|join| Error::Io(std::io::Error::other(join)))??;

        Ok(FileLog {
            _lock: lock,
            id: id.clone(),
            log,
            parent,
            limit: self.limit,
            handle: Some(handle),
        })
    }
}

/// One state's entries, as an append-only log under a held lock.
///
/// ```text
/// <root>/treasury/ledger.log      the entries, newest last
/// <root>/treasury/ledger.lock     the claim, held by an open file
/// ```
///
/// # Why a log and not a file per entry
///
/// A key here is a caller's bytes, and a caller's bytes are not a filename.
/// One file per entry would need an escaping scheme, would put a directory
/// entry's cost on every record, and would make recovery a directory walk. A
/// log length-prefixes both halves, so there is nothing to escape, one `fsync`
/// per write, and recovery is one sequential read.
///
/// The cost is that removed and replaced entries stay on the disk until
/// [`compact`](Held::compact) rewrites the file. That is what `compact` is for,
/// and it is why the port says calling it is never required for correctness.
#[derive(Debug)]
pub struct FileLog {
    /// Never read. Its existence *is* the claim: dropping it unlocks.
    _lock: std::fs::File,
    id: StateId,
    log: PathBuf,
    parent: PathBuf,
    limit: u64,
    /// The append handle, taken while a blocking write owns it and put back
    /// whether that write succeeded or failed — a failed `put` must leave a
    /// store that can still be written to.
    handle: Option<std::fs::File>,
}

impl FileLog {
    /// Append one record and put it on the disk.
    async fn append(&mut self, record: Vec<u8>) -> Result<()> {
        let offered = record.len() as u64;
        if offered > self.limit {
            return Err(Error::TooLarge {
                offered,
                limit: self.limit,
            });
        }
        let Some(mut file) = self.handle.take() else {
            // Unreachable: every path below puts the handle back. Typed rather
            // than unwrapped, because "unreachable" is a claim about today's
            // call sites and `unwrap` would be a claim about every future one.
            return Err(Error::Io(std::io::Error::other(
                "the entry log's write handle was not returned by an earlier write",
            )));
        };

        let (file, wrote) = tokio::task::spawn_blocking(move || {
            use std::io::Write as _;

            let wrote = file.write_all(&record).and_then(|()| file.sync_data());
            (file, wrote)
        })
        .await
        .map_err(|join| Error::Io(std::io::Error::other(join)))?;

        self.handle = Some(file);
        wrote.map_err(Error::Io)
    }
}

impl Held for FileLog {
    async fn load(&self) -> Result<Vec<(EntryKey, Vec<u8>)>> {
        let bytes = match tokio::fs::read(&self.log).await {
            Ok(bytes) => bytes,
            // Never written is not a failure. It is the first run.
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(Error::Io(err)),
        };
        replay(&self.id, &bytes)
    }

    async fn put(&mut self, key: &EntryKey, bytes: &[u8]) -> Result<()> {
        // Refused before it is encoded, because the record's own length fields
        // are `u32`: `Files::limit` takes any `u64`, so a caller that raised it
        // past 4 GiB could otherwise write a truncated length and make every
        // record after this one unreadable.
        if u32::try_from(bytes.len()).is_err() {
            return Err(Error::TooLarge {
                offered: bytes.len() as u64,
                limit: u64::from(u32::MAX),
            });
        }
        self.append(encode(OP_PUT, key, bytes)).await
    }

    async fn remove(&mut self, key: &EntryKey) -> Result<()> {
        self.append(encode(OP_REMOVE, key, &[])).await
    }

    /// Rewrite the log holding only what is still there.
    ///
    /// The four steps [`Claim::store`] takes, for the same reason: write a
    /// temporary file, put it on the disk, rename it over the log, and put the
    /// rename on the disk. A crash at any point leaves the previous log, which
    /// is a longer file saying the same thing.
    async fn compact(&mut self) -> Result<()> {
        let live = self.load().await?;
        let mut rewritten = Vec::from(LOG_MAGIC);
        for (key, value) in &live {
            rewritten.extend_from_slice(&encode(OP_PUT, key, value));
        }

        let mut temp = self.log.clone().into_os_string();
        temp.push(format!(".tmp.{}", std::process::id()));
        let temp = PathBuf::from(temp);
        let log = self.log.clone();
        let parent = self.parent.clone();
        // Dropped inside the blocking task, so the old handle is closed before
        // the rename rather than left pointing at an unlinked file.
        let previous = self.handle.take();

        let handle = tokio::task::spawn_blocking(move || -> std::io::Result<std::fs::File> {
            use std::io::Write as _;

            drop(previous);
            let mut file = std::fs::File::create(&temp)?;
            file.write_all(&rewritten)?;
            file.sync_all()?;
            drop(file);

            if let Err(err) = std::fs::rename(&temp, &log) {
                // Leave nothing behind on a failed replace.
                let _ = std::fs::remove_file(&temp);
                return Err(err);
            }
            sync_dir(&parent)?;

            std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&log)
        })
        .await
        .map_err(|join| Error::Io(std::io::Error::other(join)))?;

        match handle {
            Ok(file) => {
                self.handle = Some(file);
                Ok(())
            }
            // The log on the disk is intact either way — the rename is atomic
            // and the reopen changes nothing — but this handle can no longer
            // write, and `append` says so rather than pretending.
            Err(err) => Err(Error::Io(err)),
        }
    }
}

/// The step people skip: without it a rename can be lost to a power cut.
///
/// Opening a directory as a file is a POSIX behaviour. On a platform without
/// it, the write and the rename still hold and only the durability of the
/// rename itself is at the mercy of the filesystem.
fn sync_dir(parent: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::File::open(parent)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = parent;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    /// A scratch directory that removes itself. Small enough to not be worth a
    /// dependency, and it keeps the test suite hermetic.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "keylink-persist-{name}-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            let _ = std::fs::remove_dir_all(&path);
            Self(path)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn id(name: &str) -> StateId {
        StateId::new(name).expect("a valid id")
    }

    async fn files(name: &str) -> (Scratch, Files) {
        let scratch = Scratch::new(name);
        let files = Files::open(&scratch.0).await.expect("opens");
        (scratch, files)
    }

    #[tokio::test]
    async fn a_state_that_was_never_stored_loads_as_none() {
        let (_scratch, files) = files("never").await;
        let claim = files.claim(&id("warehouse/sp-01")).await.expect("claims");
        assert_eq!(claim.load().await.expect("loads"), None);
    }

    #[tokio::test]
    async fn what_is_stored_is_what_comes_back() {
        let (_scratch, files) = files("roundtrip").await;
        let mut claim = files.claim(&id("warehouse/sp-01")).await.expect("claims");

        claim.store(b"muda-de-ipe-4471").await.expect("stores");
        assert_eq!(
            claim.load().await.expect("loads").as_deref(),
            Some(&b"muda-de-ipe-4471"[..])
        );

        // And a replacement replaces, rather than appending or merging.
        claim.store(b"shorter").await.expect("stores");
        assert_eq!(
            claim.load().await.expect("loads").as_deref(),
            Some(&b"shorter"[..])
        );
    }

    /// The claim is the whole point. Made red by deleting the `try_lock` call.
    #[tokio::test]
    async fn a_second_claim_on_one_state_is_refused() {
        let (_scratch, files) = files("busy").await;
        let held = files.claim(&id("warehouse/sp-01")).await.expect("claims");

        let error = files
            .claim(&id("warehouse/sp-01"))
            .await
            .expect_err("somebody already holds it");
        assert!(matches!(error, Error::Busy { .. }), "got {error:?}");

        // And it is available again once the first holder lets go.
        drop(held);
        files
            .claim(&id("warehouse/sp-01"))
            .await
            .expect("the claim was released");
    }

    #[tokio::test]
    async fn two_states_do_not_block_each_other() {
        let (_scratch, files) = files("two").await;
        let _a = files.claim(&id("warehouse/sp-01")).await.expect("claims");
        let _b = files.claim(&id("warehouse/rj-02")).await.expect("claims");
    }

    /// A store leaves the directory holding exactly what it should.
    #[tokio::test]
    async fn a_store_leaves_no_temporary_file_behind() {
        let (scratch, files) = files("clean").await;
        let mut claim = files.claim(&id("obra/4471")).await.expect("claims");
        claim.store(b"done").await.expect("stores");

        let left: Vec<String> = std::fs::read_dir(scratch.0.join("obra"))
            .expect("reads")
            .map(|entry| entry.expect("entry").file_name().to_string_lossy().into())
            .collect();

        assert!(
            !left.iter().any(|name| name.contains(".tmp.")),
            "a temporary file survived the store: {left:?}"
        );
    }

    /// An interrupted store cannot be observed. A temporary file left by a
    /// crashed writer is not the snapshot, and `load` does not see it.
    #[tokio::test]
    async fn a_temporary_file_from_a_crashed_writer_is_not_the_state() {
        let (scratch, files) = files("interrupted").await;
        let mut claim = files.claim(&id("obra/4471")).await.expect("claims");
        claim.store(b"the committed one").await.expect("stores");

        // Exactly what a writer that died between steps 1 and 3 leaves.
        std::fs::write(
            scratch.0.join("obra").join("4471.snapshot.tmp.99999"),
            b"the half-written one",
        )
        .expect("writes");

        assert_eq!(
            claim.load().await.expect("loads").as_deref(),
            Some(&b"the committed one"[..]),
            "load must read the renamed file, never a temporary one"
        );
    }

    #[tokio::test]
    async fn a_snapshot_over_the_limit_is_refused_and_the_previous_one_survives() {
        let (_scratch, files) = files("limit").await;
        let files = files.limit(8);
        let mut claim = files.claim(&id("obra/4471")).await.expect("claims");
        claim.store(b"small").await.expect("stores");

        let error = claim
            .store(b"far too large for the limit")
            .await
            .expect_err("over the limit");
        assert!(matches!(error, Error::TooLarge { .. }), "got {error:?}");

        assert_eq!(
            claim.load().await.expect("loads").as_deref(),
            Some(&b"small"[..]),
            "a refused store must not disturb what was there"
        );
    }

    // ─────────────────────────────────────────────────────── the entry log

    async fn logs(name: &str) -> (Scratch, Files) {
        let scratch = Scratch::new(name);
        let files = Files::open(&scratch.0).await.expect("opens");
        (scratch, files)
    }

    fn record(text: &str) -> EntryKey {
        EntryKey::new(text.as_bytes()).expect("a valid key")
    }

    /// Everything a log holds, as pairs a test can compare against a literal.
    async fn entries(log: &FileLog) -> Vec<(Vec<u8>, Vec<u8>)> {
        log.load()
            .await
            .expect("loads")
            .into_iter()
            .map(|(key, value)| (key.as_bytes().to_vec(), value))
            .collect()
    }

    #[tokio::test]
    async fn a_log_that_was_never_written_loads_as_nothing() {
        let (_scratch, files) = logs("empty").await;
        let log = files.hold(&id("treasury/ledger")).await.expect("claims");
        assert_eq!(entries(&log).await, Vec::new());
    }

    #[tokio::test]
    async fn a_second_put_under_one_key_replaces_the_first() {
        let (_scratch, files) = logs("replace").await;
        let mut log = files.hold(&id("treasury/ledger")).await.expect("claims");

        log.put(&record("order-1"), b"paid").await.expect("puts");
        log.put(&record("order-2"), b"held").await.expect("puts");
        log.put(&record("order-1"), b"refunded")
            .await
            .expect("puts");

        assert_eq!(
            entries(&log).await,
            vec![
                (b"order-2".to_vec(), b"held".to_vec()),
                (b"order-1".to_vec(), b"refunded".to_vec()),
            ],
            "a replaced key keeps one entry, and takes its place at the end"
        );
    }

    #[tokio::test]
    async fn a_removed_entry_is_gone_and_its_neighbours_are_not() {
        let (_scratch, files) = logs("remove").await;
        let mut log = files.hold(&id("treasury/ledger")).await.expect("claims");

        log.put(&record("order-1"), b"paid").await.expect("puts");
        log.put(&record("order-2"), b"held").await.expect("puts");
        log.remove(&record("order-1")).await.expect("removes");

        assert_eq!(
            entries(&log).await,
            vec![(b"order-2".to_vec(), b"held".to_vec())]
        );

        // And removing what is not there is not an error.
        log.remove(&record("order-9")).await.expect("removes");
    }

    /// What a caller reads after the process died mid-`put`. The entry being
    /// written is dropped and everything before it survives, which is the whole
    /// reason a partial record is not [`Error::NotALog`].
    #[tokio::test]
    async fn a_record_cut_in_half_is_dropped_and_the_rest_survives() {
        let (scratch, files) = logs("truncated").await;
        let path = scratch.0.join("treasury").join("ledger.log");
        {
            let mut log = files.hold(&id("treasury/ledger")).await.expect("claims");
            log.put(&record("order-1"), b"paid").await.expect("puts");
            log.put(&record("order-2"), b"half-written")
                .await
                .expect("puts");
        }

        let whole = std::fs::metadata(&path).expect("reads").len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("opens")
            .set_len(whole - 5)
            .expect("truncates");

        let log = files.hold(&id("treasury/ledger")).await.expect("claims");
        assert_eq!(
            entries(&log).await,
            vec![(b"order-1".to_vec(), b"paid".to_vec())],
            "the interrupted record goes and the settled one stays"
        );
    }

    /// A zero-filled tail is a torn write, not a corrupt log.
    ///
    /// A size-extending append can reach the disk ahead of the bytes it was
    /// extended for, so this is what a power cut mid-`put` leaves on an
    /// ext4-family filesystem — and a zero operation with zero lengths passes
    /// every length check while naming an empty key. Read as a record it
    /// refuses the whole log, and a service cannot start over the one entry its
    /// own caller already treats as unwritten.
    ///
    /// Made red by deleting the all-zeros check in `replay`.
    #[tokio::test]
    async fn a_zero_filled_tail_is_dropped_and_the_rest_survives() {
        let (scratch, files) = logs("zero-tail").await;
        let path = scratch.0.join("treasury").join("ledger.log");
        {
            let mut log = files.hold(&id("treasury/ledger")).await.expect("claims");
            log.put(&record("order-1"), b"paid").await.expect("puts");
        }

        let whole = std::fs::metadata(&path).expect("reads").len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("opens")
            .set_len(whole + 64)
            .expect("extends with zeros");

        let log = files.hold(&id("treasury/ledger")).await.expect("claims");
        assert_eq!(
            entries(&log).await,
            vec![(b"order-1".to_vec(), b"paid".to_vec())],
            "the zeros are an unwritten tail and the record before them stands"
        );
    }

    /// A zero operation is only a torn tail when the rest of the file is zeros.
    /// One in the middle of a file that continues is a fault, and says so.
    #[tokio::test]
    async fn a_zero_operation_in_the_middle_is_still_refused() {
        let (scratch, files) = logs("zero-middle").await;
        let path = scratch.0.join("treasury").join("ledger.log");
        {
            let mut log = files.hold(&id("treasury/ledger")).await.expect("claims");
            log.put(&record("order-1"), b"paid").await.expect("puts");
        }

        let mut bytes = std::fs::read(&path).expect("reads");
        // The operation byte of the one record, blanked, with a real record's
        // worth of bytes still after it.
        bytes[LOG_MAGIC.len()] = 0;
        std::fs::write(&path, &bytes).expect("writes");

        let log = files.hold(&id("treasury/ledger")).await.expect("claims");
        let error = log.load().await.expect_err("not a torn tail");
        assert!(
            matches!(
                error,
                Error::NotALog {
                    fault: EntryFault::UnknownOperation { op: 0 },
                    ..
                }
            ),
            "got {error:?}"
        );
    }

    /// Made red by deleting the magic check: the garbage then reads as an empty
    /// log, which is the silent-success failure the magic exists to prevent.
    #[tokio::test]
    async fn bytes_that_are_not_a_log_are_refused_rather_than_read() {
        let (scratch, files) = logs("garbage").await;
        std::fs::create_dir_all(scratch.0.join("treasury")).expect("creates");
        std::fs::write(
            scratch.0.join("treasury").join("ledger.log"),
            b"written by something else entirely",
        )
        .expect("writes");

        let log = files.hold(&id("treasury/ledger")).await.expect("claims");
        let error = log.load().await.expect_err("not a log this build reads");
        assert!(
            matches!(
                error,
                Error::NotALog {
                    fault: EntryFault::WrongMagic,
                    ..
                }
            ),
            "got {error:?}"
        );
    }

    #[tokio::test]
    async fn compacting_shortens_the_file_and_changes_nothing_it_holds() {
        let (scratch, files) = logs("compact").await;
        let path = scratch.0.join("treasury").join("ledger.log");
        let mut log = files.hold(&id("treasury/ledger")).await.expect("claims");

        for round in 0..20_u32 {
            log.put(&record("order-1"), &round.to_be_bytes())
                .await
                .expect("puts");
        }
        log.put(&record("order-2"), b"held").await.expect("puts");
        log.remove(&record("order-2")).await.expect("removes");

        let before = std::fs::metadata(&path).expect("reads").len();
        let held = entries(&log).await;
        log.compact().await.expect("compacts");
        let after = std::fs::metadata(&path).expect("reads").len();

        assert_eq!(entries(&log).await, held, "compaction is not a change");
        assert!(after < before, "{after} is not shorter than {before}");

        // And the handle still writes afterwards, which is the half a rewrite
        // is most likely to lose.
        log.put(&record("order-3"), b"new").await.expect("puts");
        assert_eq!(entries(&log).await.len(), 2);
    }

    #[tokio::test]
    async fn a_second_claim_on_one_log_is_refused() {
        let (_scratch, files) = logs("busy-log").await;
        let held = files.hold(&id("treasury/ledger")).await.expect("claims");

        let error = files
            .hold(&id("treasury/ledger"))
            .await
            .expect_err("somebody already holds it");
        assert!(matches!(error, Error::Busy { .. }), "got {error:?}");

        drop(held);
        files
            .hold(&id("treasury/ledger"))
            .await
            .expect("the claim was released");
    }

    /// The two ports share one lock per state, so a name claimed through one is
    /// not available through the other.
    #[tokio::test]
    async fn a_snapshot_claim_and_a_log_claim_on_one_name_exclude_each_other() {
        let scratch = Scratch::new("both-ports");
        let snapshots = Files::open(&scratch.0).await.expect("opens");
        let logs = Files::open(&scratch.0).await.expect("opens");

        let held = Snapshots::claim(&snapshots, &id("treasury/ledger"))
            .await
            .expect("claims");
        let error = Entries::hold(&logs, &id("treasury/ledger"))
            .await
            .expect_err("the snapshot port holds it");
        assert!(matches!(error, Error::Busy { .. }), "got {error:?}");

        drop(held);
        Entries::hold(&logs, &id("treasury/ledger"))
            .await
            .expect("the claim was released");
    }

    #[tokio::test]
    async fn an_entry_over_the_limit_is_refused_and_the_log_survives() {
        let (_scratch, files) = logs("log-limit").await;
        let files = files.limit(32);
        let mut log = files.hold(&id("treasury/ledger")).await.expect("claims");
        log.put(&record("order-1"), b"paid").await.expect("puts");

        let error = log
            .put(&record("order-2"), &[0_u8; 64])
            .await
            .expect_err("over the limit");
        assert!(matches!(error, Error::TooLarge { .. }), "got {error:?}");

        assert_eq!(
            entries(&log).await,
            vec![(b"order-1".to_vec(), b"paid".to_vec())],
            "a refused put must not disturb what was there"
        );
        // And the store still works, which a taken-and-not-returned handle
        // would have broken.
        log.put(&record("order-3"), b"ok").await.expect("puts");
    }

    #[test]
    fn a_record_key_is_bounded_and_never_empty() {
        assert!(EntryKey::new(Vec::new()).is_err());
        assert!(EntryKey::new(vec![b'x'; EntryKey::MAX_LEN]).is_ok());
        assert!(EntryKey::new(vec![b'x'; EntryKey::MAX_LEN + 1]).is_err());
        // Arbitrary bytes, on purpose: a key is not a path and needs no escape.
        assert!(EntryKey::new(vec![0, 255, b'/', b'.']).is_ok());
    }

    /// The id is a path, and the path stays under the root. This is the test
    /// for a guard that does not exist, because `StateId` deleted it.
    #[test]
    fn an_id_cannot_climb_out_of_the_root() {
        for escape in ["../etc/passwd", "warehouse/../../etc", "..", "./x", "a/./b"] {
            assert!(
                StateId::new(escape).is_err(),
                "{escape:?} must not be a valid id"
            );
        }
    }

    #[test]
    fn an_id_is_a_readable_path_and_not_a_hash() {
        let id = id("warehouse/sp-01");
        assert_eq!(
            id.segments().collect::<Vec<_>>(),
            vec!["warehouse", "sp-01"]
        );
    }
}

// The round trip through `Durable`, the migration chain and the frame are in
// `keylink-persist/tests/versioned.rs`, from outside the crate — which is where
// `#[non_exhaustive]` on the loader's verdicts is actually in force. Nothing
// about them belongs here: an adapter stores the bytes it is handed and never
// learns that versions exist.
