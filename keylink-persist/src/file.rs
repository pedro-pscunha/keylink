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

use std::path::{Path, PathBuf};

use crate::{Claim, Error, Result, Snapshots, StateId};

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
    #[must_use]
    pub fn limit(mut self, bytes: u64) -> Self {
        self.limit = bytes;
        self
    }

    /// Where this state's files live. Built from segments the constructor
    /// already validated, so there is nothing to escape and nothing to check.
    fn paths(&self, id: &StateId) -> (PathBuf, PathBuf) {
        let mut base = self.root.clone();
        for segment in id.segments() {
            base.push(segment);
        }
        let mut snapshot = base.clone().into_os_string();
        snapshot.push(".snapshot");
        let mut lock = base.into_os_string();
        lock.push(".lock");
        (PathBuf::from(snapshot), PathBuf::from(lock))
    }
}

impl Snapshots for Files {
    type Claim = FileClaim;

    async fn claim(&self, id: &StateId) -> Result<FileClaim> {
        let (snapshot, lock) = self.paths(id);
        let parent = snapshot
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.root.clone());
        tokio::fs::create_dir_all(&parent).await?;

        let id = id.clone();
        let limit = self.limit;
        // Opening and locking are blocking calls. `try_lock` in particular parks
        // nothing, but the open can, on a cold or networked filesystem.
        tokio::task::spawn_blocking(move || {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&lock)?;
            match file.try_lock() {
                Ok(()) => Ok(FileClaim {
                    _lock: file,
                    snapshot,
                    parent,
                    limit,
                }),
                // Every other kind of failure is an I/O fault and says so.
                Err(std::fs::TryLockError::WouldBlock) => Err(Error::Busy { id }),
                Err(std::fs::TryLockError::Error(err)) => Err(Error::Io(err)),
            }
        })
        .await
        .map_err(|join| Error::Io(std::io::Error::other(join)))?
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
            //
            // Opening a directory as a file is a POSIX behaviour. On a platform
            // without it, steps 1 to 3 still hold and only the durability of the
            // rename itself is at the mercy of the filesystem.
            #[cfg(unix)]
            {
                std::fs::File::open(&parent)?.sync_all()?;
            }
            #[cfg(not(unix))]
            {
                let _ = &parent;
            }
            Ok(())
        })
        .await
        .map_err(|join| Error::Io(std::io::Error::other(join)))??;

        Ok(())
    }
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
