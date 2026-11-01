//! The one adapter the [`Observer`] port is owed: a state written after every
//! turn, in front of the reply.
//!
//! # "In front of the reply" means in front of **every** answer a turn produces
//!
//! An ordinary `rpc`, a `blob`, a `recv`, a feed item: the handler returns, the
//! observer chain runs, the store completes, and only then is the caller told
//! anything. An answered call is a stored call.
//!
//! **A `#[defer]` method is one of those, and it was the exception until
//! `parking.rs`.** [`Deferred::send`] used to hand the value
//! straight to the caller from inside the running handler — *"this turn's
//! observers neither see it nor gate it"* — so a caller held its answer for the
//! whole of the ~7.6 ms below and a crash in that window lost state somebody was
//! already told about. It now parks the value instead: the answer waits for the
//! observer chain of the turn that produced it and is released with the same
//! verdict that turn's own caller is answered by, so a store that fails takes the
//! receipt with it. `tests/persist.rs::
//! a_deferred_reply_whose_turn_was_not_stored_is_refused` is the same vault, the
//! same failing store, and the opposite assertion to the one it used to make.
//!
//! # The one thing left of the exception
//!
//! A [`Deferred`] is `Send` and `'static`, so it can be sent
//! from a spawned task, or reached from a
//! [`with_state`](crate::LocalActor::read) closure through interior mutability.
//! Those sends cross **immediately**, and they are outside the claim rather than
//! a hole in it: no turn ran, no observer saw anything, and there is no commit
//! for the answer to wait for. Withholding them would hang a caller on a turn
//! that may never come.
//!
//! So the claim, exactly: *an answered call is a stored call, for every answer a
//! **turn** produced.* Answer from a turn, and the reply is behind the store.
//! Answer from a task you spawned, and it is not — which is the same sentence the
//! `with_state` paragraph on the type below makes about a read.
//! `tests/defer.rs::a_deferred_sent_from_another_task_is_not_gated_by_the_turn_that_spawned_it`
//! pins it.
//!
//! `keylink-persist` states the standard this module exists to meet — a port is
//! justified by one adapter written on purpose — and until this file existed
//! every implementor of [`Observer`] in the workspace was a test or a
//! compile-time probe. This is the consumer the port was designed for, and the
//! shape of [`Observer::after_turn`] is the shape it needed.
//!
//! # Where it lives, and why it is not a third crate
//!
//! A `keylink-durable` crate whose entire content is one adapter is a seam
//! nobody asked for. The dependency-direction objection does not survive
//! either: the hexagon's rule is that the **port** must not know the framework,
//! and `keylink → keylink-persist` is the application depending on the port,
//! which is the direction the arrow is supposed to point. `keylink-persist` has
//! no idea this file exists and still compiles alone.
//!
//! What was left was the browser, and that is a feature problem rather than a
//! crate problem. `keylink-persist` arrives here `optional = true,
//! default-features = false`, so:
//!
//! | feature | what it adds | browser |
//! |---|---|---|
//! | `persist` | this module, over the port | **builds** |
//! | `persist-file` | `persist` plus the file adapter | `compile_error!` |
//!
//! `persist` reaches nothing but the frame and the two port traits, so it
//! compiles for `wasm32-unknown-unknown` and `.mise/tasks/wasm` builds it there.
//! `persist-file` turns on `tokio/fs`, which does not exist in a browser, and
//! `lib.rs` refuses it on that target beside the same refusal for `blocking`,
//! `fs` and `env`.
//!
//! # Two strategies, one model
//!
//! A persisted actor is **a snapshot plus the log of what changed after it, under
//! one exclusive claim.** [`Persist`] has a third type parameter that picks how a
//! turn reaches it:
//!
//! | `Persist<D, S, _>` | what a completed turn stores | the state is |
//! |---|---|---|
//! | [`Whole`] | the whole state, replacing the snapshot | a plain [`Durable`](keylink_persist::Durable) |
//! | [`Deltas`] | what the turn changed, appended to the log | a [`Journaled`](keylink_persist::Journaled) |
//!
//! Both open through the same restore, so a log left by one is **refused** by the
//! other rather than silently ignored: opening a journaled state under [`Whole`]
//! is `RestoreError::UnreplayedLog`, because the snapshot alone is the state from
//! before every record that follows it.
//!
//! `Whole` is today's behaviour. Its cost grows with the state, because every turn
//! re-encodes and rewrites all of it; the 7.6 ms table below is that cost.
//! `Deltas` is for a state that grows: a turn costs the size of its change, and
//! every so often a [`Compaction`](keylink_persist::versioned::Compaction) policy folds the
//! log into a snapshot.
//!
//! | what is flushed, per turn | `Whole` | `Deltas` |
//! |---|---|---|
//! | the turn changed something | the temp file, the rename, the directory (two flushes) | one `sync_data` on the journal |
//! | the turn changed nothing | two flushes | **no I/O** |
//! | a compaction turn | n/a | four flushes (snapshot two, journal rewrite two), and an in-turn encode of the whole state |
//!
//! **Bytes per turn are computed from the format, and
//! `tests/journal.rs::deltas_keep_the_bytes_per_turn_constant_while_the_state_grows`
//! measures them**: a `Deltas` turn writes 20 (the frame) plus 12 (the record's
//! version, count and the delta's length) plus the delta; a `Whole` turn writes 20
//! plus the whole state.
//!
//! **Time per turn, `Whole` against `Deltas`, measured back to back.** Same
//! machine as the figures below (Apple M5 Max, `Mac17,6`, APFS on the internal
//! SSD, macOS build 26B118), 2026-11-01, over `Files`. 200 timed turns after a
//! warm-up turn, three runs per cell, **alternating which strategy goes first**.
//! The shelf is pre-filled to the stated number of 32-byte items and grows by 200
//! during the run.
//!
//! | items | `Whole`, µs per turn (3 runs) | `Deltas`, µs per turn (3 runs) | median ratio | bytes per turn, mean over the timed turns |
//! |---|---|---|---|---|
//! | 1 | 9,421 / 10,098 / 10,827 | 3,485 / 4,590 / 4,695 | 0.45 | 3,300 against 64 |
//! | 1,000 | 11,080 / 11,103 / 11,731 | 4,359 / 5,886 / 6,660 | 0.53 | 35,268 against 64 |
//! | 100,000 | 15,699 / 16,402 / 18,248 | 5,234 / 5,932 / 5,978 | 0.36 | 3,203,268 against 64 |
//!
//! **Bytes per turn are the same quantity in both columns**: the mean number of
//! bytes a timed turn wrote. For `Whole` that is the mean snapshot size, because
//! every turn rewrites all of it and it grows by one item a turn (the column used
//! to hold the *last* turn's size, which is not a mean). For `Deltas` it is the
//! journal's growth divided by the turns: a 64-byte frame (a 20-byte header, a
//! 12-byte record header and the 32-byte delta).
//!
//! **What these figures do not cover.** The machine was loaded by other sessions'
//! builds while they ran (load average 45 to 66 at the end), so the `Whole`
//! figures are slower than the 7.6 ms below in some cells and faster in others, and
//! the spread inside a cell is the noise. Read the ratio, not the microseconds.
//! They are macOS, where `sync_data` is `F_FULLFSYNC`; **nothing was run on
//! Linux**. A fold turn is not in them by construction: 200 turns of 44 bytes never
//! reach the threshold at these sizes. And a mean says nothing about the tail.
//! `tests/journal.rs::the_per_turn_cost_whole_against_deltas` reproduces the table.
//!
//! The saving is not 2x at a small state, because the `Whole` turn is already
//! less than two full flushes. It grows with the state, because `Whole` pays for
//! the encode and the write of all of it and `Deltas` does not.
//!
//! **A compaction turn is a latency spike by construction**: it encodes the whole
//! state inside the turn and flushes four times. Bounding it would need a
//! consistent copy of the state outside the turn, which means `Clone`,
//! copy-on-write or `A: Sync`, and this design keeps all three out.
//!
//! # What it costs per turn — measured, on a named machine, on a stated day
//!
//! RFC-06 §7 asked for this number and never got one. It is much larger than
//! anybody guessed, so it is the first thing in this file after the placement.
//!
//! **This section is `Whole`'s cost.** `Deltas` appears in the two-strategies
//! section above and in the A/B table below it.
//!
//! **7.6 ms per turn**, against **18 µs** for the same turn with no observer.
//! The store is essentially all of it: **~7.6 ms**, or **about 130 turns per
//! second per actor**.
//!
//! | profile | no observer | with `Persist` | runs |
//! |---|---|---|---|
//! | debug (`cargo test`) | 16.5 – 20.2 µs | 7,520 / 7,652 / 7,927 µs | 3 |
//! | release | 12.8 µs | 7,083 µs | 1 |
//!
//! Apple M5 Max (`Mac17,6`), macOS 27.0 build 26A5378n, APFS on the internal
//! SSD, 2026-08-07. 1,000 turns of a fixed-size 8-byte state, so the payload is
//! 20 framed bytes on every turn and the number is not a function of how far the
//! run got. The debug and release columns agree to within 8%, which is the
//! evidence that this is a syscall cost and not a code-generation one.
//!
//! **7.6 ms is a mean, and there is no tail in this table and no bound
//! anywhere.** Nothing wraps [`Observer::after_turn`] in a timeout — not this
//! adapter, not the loop — so the store's worst case is the actor's worst case.
//! A store that hangs wedges the single loop that owns the state: no further
//! turn runs, no feed item is taken, and `stop().await` never returns because
//! it waits for the loop to reach its exit. The only escape is
//! [`abort`](crate::LocalActor::abort), which cancels the task at its next
//! await and therefore skips [`Observer::finish`] for every observer.
//!
//! That is a property of a filesystem `fsync` on a healthy local disk, where it
//! is a fair trade. It is a different property against a network store, an NFS
//! mount, or a full one — and an adapter for any of those should carry its own
//! deadline, because there is nowhere else to put one.
//!
//! **It is two `fsync`s and a `rename`, and it is *supposed* to be expensive.**
//! `keylink_persist::file::Files` syncs the temporary file and then the
//! directory, which is what makes "an answered call is a stored call" survive a
//! power cut rather than merely a process restart. An adapter that skipped the
//! second `fsync` would report a much better number and quietly mean something
//! weaker.
//!
//! So the number is not a defect to fix here; it is the price of the promise, and
//! the knob that trades it away is a debounce — which nothing here implements
//! and nothing here half-implements; see the type's own paragraph on why not.
//! A caller who wants the other trade today has it without any new machinery:
//! write a state whose turns are cheap to lose, or reach for an adapter whose
//! `store` does not sync.
//!
//! Reproduce it with
//! `cargo test -p keylink --test persist -- --ignored --nocapture`, which prints
//! the same numbers and asserts nothing about any of them. There is no
//! wall-clock assertion anywhere in this wave.

use std::marker::PhantomData;

use keylink_persist::versioned::{Compaction, Snapshot, TornTail, Versioned};
use keylink_persist::{Durable, Journaled, Snapshots, StateId};

use crate::{Observer, Stop, TurnFut, TurnInfo, TurnOutcome};

/// Stores the state after every turn it accepts, and refuses a turn that
/// panicked.
///
/// The third parameter, `M`, is the strategy: [`Whole`] stores the whole state
/// every turn, [`Deltas`] appends what the turn changed. It has no default, on
/// purpose: in expression position a defaulted type parameter is inferred and
/// not defaulted, so `Persist::<Ledger, _>::open` would be ambiguous between the
/// two `open`s, and every call site names the decision it is making.
///
/// One of these owns one [`Claim`](keylink_persist::Claim) — the actor owns the
/// observer, so the claim dies with the actor and is free before
/// [`stop`](crate::LocalActor::stop) returns. That is what [`Observer`]'s "the
/// actor owns its observers" paragraph is for, and it is why this type is not
/// `Clone` and cannot be shared between two actors: two writers of one state is
/// the situation the claim exists to make impossible.
///
/// ```no_run
/// # use keylink::{Caller, Persist, Whole};
/// # use keylink_persist::{Durable, RestoreError, StateId};
/// # use keylink_persist::file::Files;
/// # keylink::service! { service Ledger; rpc Post(entry: u64) -> u64; }
/// # #[derive(Default)]
/// # struct Ledger { entries: Vec<u64> }
/// # impl ledger::Actor for Ledger {
/// #     async fn post(&mut self, _cx: Caller<'_>, entry: u64) -> u64 {
/// #         self.entries.push(entry);
/// #         self.entries.len() as u64
/// #     }
/// # }
/// # impl Durable for Ledger {
/// #     const VERSION: u32 = 1;
/// #     fn snapshot(&self) -> Vec<u8> { Vec::new() }
/// #     fn restore(_: &[u8]) -> Result<Self, RestoreError> { Ok(Self::default()) }
/// #     fn migrate(_: u32, _: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> { Ok(None) }
/// # }
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let store = Files::open("/var/lib/keylink").await?;
/// let id = StateId::new("ledgers/sp-01")?;
///
/// // The claim is taken here and held until the actor ends. `Ledger` is named
/// // because nothing in the arguments says which state is being claimed — see
/// // `open`. `start_from` is the only way to reach the observer, and it hands
/// // over the restored state in the same expression.
/// let (state, saver) = Persist::<Ledger, _, Whole>::open(&store, &id)
///     .await?
///     .start_from(Ledger::default);
/// let ledger = ledger::setup(state).observe(saver).actor();
///
/// // The store is in front of the reply, so this returning `Ok` means the bytes
/// // are fsynced. A `#[defer]` method gets the same thing: its parked answer
/// // waits for the same chain — see this module's header for the one send that
/// // does not, which is the one made from a task you spawned yourself.
/// ledger.post(1).await?;
/// # Ok(())
/// # }
/// ```
///
/// # The same ledger, journaled
///
/// A state that grows pays for its whole size on every `Whole` turn. A
/// [`Journaled`] one pays for its change. The handler changes the state through
/// [`apply`](Journaled::apply), the code a restore runs, and records the change:
///
/// ```no_run
/// # use keylink::{Caller, Deltas, Persist};
/// # use keylink_persist::{Changes, Durable, Journaled, RestoreError, StateId};
/// # use keylink_persist::file::Files;
/// # keylink::service! { service Ledger; rpc Post(entry: u64) -> u64; }
/// #[derive(Clone)]
/// struct Posted(u64);
/// # impl Durable for Posted {
/// #     const VERSION: u32 = 1;
/// #     fn snapshot(&self) -> Vec<u8> { self.0.to_le_bytes().to_vec() }
/// #     fn restore(_: &[u8]) -> Result<Self, RestoreError> { Ok(Self(0)) }
/// #     fn migrate(_: u32, _: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> { Ok(None) }
/// # }
/// #[derive(Default)]
/// struct Ledger { entries: Vec<u64>, changes: Changes<Posted> }
/// # impl Durable for Ledger {
/// #     const VERSION: u32 = 1;
/// #     fn snapshot(&self) -> Vec<u8> { Vec::new() }
/// #     fn restore(_: &[u8]) -> Result<Self, RestoreError> { Ok(Self::default()) }
/// #     fn migrate(_: u32, _: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> { Ok(None) }
/// # }
/// impl Journaled for Ledger {
///     type Delta = Posted;
///     fn changes(&self) -> &Changes<Posted> { &self.changes }
///     fn apply(&mut self, delta: Posted) { self.entries.push(delta.0); }
/// }
///
/// impl ledger::Actor for Ledger {
///     async fn post(&mut self, _cx: Caller<'_>, entry: u64) -> u64 {
///         let delta = Posted(entry);
///         self.apply(delta.clone());
///         self.changes.record(delta);
///         self.entries.len() as u64
///     }
/// }
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let store = Files::open("/var/lib/keylink").await?;
/// let id = StateId::new("ledgers/sp-01")?;
///
/// let opened = Persist::<Ledger, _, Deltas>::open(&store, &id).await?;
/// if let Some(torn) = opened.torn_tail() {
///     eprintln!("recovery cut {} bytes after record {}", torn.bytes, torn.after);
/// }
/// let (state, saver) = opened.start_from(Ledger::default)?;
/// let ledger = ledger::setup(state).observe(saver).actor();
/// ledger.post(1).await?;
/// # Ok(())
/// # }
/// ```
///
/// **An unrecorded mutation is lost on restart, silently.** Nothing can check
/// that a delta matches what the turn changed. The guard is a test that reopens
/// the state and compares its `snapshot()` with the live one, as
/// `tests/journal.rs` does.
///
/// # What is stored is exactly what was answered — **both** directions
///
/// The first direction is the wave's headline: a turn is written before anybody
/// is told anything, so an answered `rpc` is a stored `rpc` — and so is a
/// `#[defer]` method's parked answer, which waits for the same chain. The
/// converse matters just as much and is easier to lose: **a turn that was not
/// answered is not stored either.**
///
/// A `store` that returns `Err` refuses the turn, which ends the actor and
/// hands the caller
/// [`Withheld::Refused`](crate::Withheld::Refused) — whose text is *"it ran and
/// was not committed"*, and which crosses the wire to a peer as that same
/// value, naming `persist`, under the same sentence. That is a promise, and
/// the only thing the caller has to act on. A caller who believes it replays;
/// an operator who reads it replays.
///
/// So **this adapter does not write anything on the way out.** It implements
/// no [`Observer::finish`], and it keeps no flag that would give one something
/// to do. A state written after the actor ended could only be the state of the
/// turn whose store just failed — the turn somebody was told was not committed
/// — and writing it turns one lost turn into a **double apply** on the replay
/// the refusal invites. For a `Post(entry)` on a ledger that is money.
///
/// It is the same trade the panicked turn takes, one line up: losing a turn is
/// the conservative loss, and reproducing one on restart is not.
///
/// **A feed turn has no caller, so retrying its failed store would be
/// harmless** — and this adapter still does not, because it cannot tell the two
/// apart. [`TurnInfo`] carries the outcome and the method name and nothing
/// about who is owed a reply. Fail closed.
///
/// # RFC-06 §7's debounce is **not** partly built here, on purpose
///
/// A debounce returns `Ok` from `after_turn` without writing, so the very first
/// thing it gives up is the property above: an answered call stops being a
/// stored call. Its `finish` is therefore answering a different question than
/// this one would — "which of these several answered-and-unwritten turns do I
/// still owe the disk, and what do I do when the last of them is also the one
/// that failed" — and a `finish` written here in advance would be an answer to
/// the question nobody asked yet. It would also have been wrong: the version
/// this replaced retried the failed turn.
///
/// # `with_state` is a read, and a state that lies about that is not stored
///
/// A generated `Handle::with_state` — [`LocalActor::read`](crate::LocalActor)
/// underneath — runs a closure against the state without going through the
/// observer chain, because a read has nothing for an observer to accept or
/// refuse. Nothing enforces that it *is* a read: a state whose fields are
/// mutable through `&self` — a [`Cell`], a [`RefCell`], an [`AtomicU64`] — can
/// be changed by that closure, and the change is answered and never stored.
///
/// This is not hypothetical for this crate. `tests/persist.rs`'s `Counter` holds
/// exactly those types, on purpose, to prove `D: Sync` is not required — and its
/// `Durable::snapshot` reads the `Cell`. A `with_state` closure bumping it would
/// be lost.
///
/// The same sentence covers a [`Deferred`](crate::Deferred) reached that way:
/// a reply answered from a read is answered from outside every turn, so it
/// crosses immediately rather than waiting for a chain that is not running. Both
/// are the same fact — **a read is not a turn** — and this module's header states
/// it as the one thing left of the `#[defer]` exception.
///
/// Stated rather than guarded, because the alternative is `A: Sync`-shaped: the
/// property "this closure does not mutate" is not one the type system holds for
/// interior mutability, and running the observer chain on every read would pay
/// the ~7.6 ms below for something that changed nothing. **Mutate in a turn.**
///
/// [`Cell`]: std::cell::Cell
/// [`RefCell`]: std::cell::RefCell
/// [`AtomicU64`]: std::sync::atomic::AtomicU64
///
/// # Put it **last** in the chain
///
/// The first observer to return `Err` stops the rest, so an observer placed
/// before this one can refuse a turn and prevent the write. One placed *after*
/// it cannot: the write has already happened, and the caller is still told
/// [`Withheld::Refused`](crate::Withheld::Refused). That asymmetry is the
/// chain's, not this type's — but "last" is the position in which it means what
/// a reader expects, so it is written here rather than left to be discovered.
///
/// # Three parameters, and why the second one is the **store**
///
/// `D` is the state, `S` is where its snapshots live and `M` is the strategy. The claim type would be
/// the narrower parameter, and it is deliberately not the one here:
/// `Versioned<C>` demands `C: Claim`, a bound this file would have to name, and
/// `keylink_persist::Claim` is the trait whose one method writes **unframed**
/// bytes. `.mise/tasks/framing` reads exactly that — a file that never brings the
/// trait into scope cannot call its methods, whatever it calls its variables — so
/// naming the store instead of the claim keeps the strongest guarantee available
/// about this file: it has no route to an unframed write at all.
/// `Versioned<S::Claim>` needs no bound, because [`Snapshots`] already promises
/// one.
pub struct Persist<D, S: Snapshots, M> {
    /// The claim, wrapped so that everything written through it is framed.
    /// Owned, with no way back out to the raw claim.
    ///
    /// **The only field with a value in it, and that is the design.** An
    /// adapter that remembered anything about the last turn would have to
    /// decide what to do with that memory on the way out, and the one decision
    /// available to it — write the state one last time — is the decision
    /// [`Observer::finish`] is deliberately not used for here. See this type's
    /// "what is stored is what was answered" paragraph.
    durable: Versioned<S::Claim>,
    /// `fn() -> D` rather than `D`, so `Send` and `Sync` here are properties of
    /// the claim and nothing else — the same reason
    /// [`Snapshot`] gives for its own.
    ///
    /// `D` is in the type rather than only in the impl because a `Persist` is
    /// bound to one state type: the frame it writes stamps
    /// [`D::VERSION`](Durable::VERSION), and an adapter that could observe two
    /// different states would write two shapes into one claim under one version
    /// number.
    state: PhantomData<fn() -> D>,
    /// How a completed turn reaches the claim: [`Whole`] or [`Deltas`].
    strategy: M,
}

/// Store the whole state after every completed turn.
///
/// A [`Persist`] strategy and nothing else: it has no value to construct. See
/// the module header for what each strategy costs.
#[derive(Debug)]
pub struct Whole(());

/// Store what each turn changed, and fold the log into a snapshot by a
/// [`Compaction`] policy.
///
/// Reached through [`Opened::compaction`] when the default is not wanted.
#[derive(Debug)]
pub struct Deltas(Compaction);

impl<D: Durable, S: Snapshots> Persist<D, S, Whole> {
    /// Claim the state, read whatever is stored, and keep the claim.
    ///
    /// **This is the only way to build one, and it does not hand one over on its
    /// own.** A constructor that took a claim and skipped the read would let a
    /// caller start an actor from `Default` and have the first turn overwrite a
    /// perfectly good stored state — silently, and exactly once per deploy.
    ///
    /// This used to return `(Option<D>, Self)` under a sentence claiming that
    /// pairing the two "makes the read impossible to leave out". It did not:
    /// `Result` is `#[must_use]` but **a tuple element is not**, so
    /// `let (_restored, saver) = ..` compiled clean and was the shortest form to
    /// copy. It returns an [`Opened`] instead, which is one value, and the only
    /// method that yields a `Persist` yields the state in the same expression.
    ///
    /// The claim is exclusive from here until that value is dropped. A second
    /// `open` on one [`StateId`] is [`Error::Busy`](keylink_persist::Error::Busy),
    /// not a second writer.
    ///
    /// # `D` appears only in the return, so it usually has to be named
    ///
    /// `Persist::<Ledger, _, Whole>::open(&store, &id)`. Nothing in the arguments says
    /// which state is being claimed, and inference reaches the observer end but
    /// not always the state end: handing the restored value to a generic
    /// `setup<A: Actor>` leaves `D` ambiguous even where exactly one type
    /// implements that service's `Actor`, because rustc will not pick an impl to
    /// resolve a type parameter. The turbofish reads as "open the Ledger's
    /// state", which is what the line means anyway.
    ///
    /// # Errors
    ///
    /// - [`Error::Busy`](keylink_persist::Error::Busy) when another holder — in
    ///   this process or another — already owns this state. **Not** a transient
    ///   error to retry in a loop.
    /// - [`Error::Io`](keylink_persist::Error::Io) when the store cannot be
    ///   claimed or read.
    /// - [`Error::Restore`](keylink_persist::Error::Restore) when the stored
    ///   bytes are not a frame this build can read, are from a newer build, or
    ///   [`Durable::restore`] refused them. Loud, and the stored bytes are left
    ///   alone.
    pub async fn open(store: &S, id: &StateId) -> keylink_persist::Result<Opened<D, S, Whole>> {
        let recovered = Versioned::open::<D>(store.claim(id).await?).await?;
        let torn = recovered.torn();
        let (restored, durable) = recovered.whole()?;
        Ok(Opened {
            restored,
            torn,
            saver: Self {
                durable,
                state: PhantomData,
                strategy: Whole(()),
            },
        })
    }
}

impl<D: Journaled, S: Snapshots> Persist<D, S, Deltas> {
    /// Claim the state, read its snapshot, and replay every delta after it.
    ///
    /// The same claim, restore and exclusivity as the [`Whole`] `open`, and the
    /// same turbofish for the same reason. What differs is the log: it is applied
    /// here, in order, through [`Journaled::apply`]. A cut that a crash
    /// interrupted is finished, and a torn end is cut and reported by
    /// [`Opened::torn_tail`]; neither changes the state the claim holds.
    ///
    /// # Errors
    ///
    /// As the [`Whole`] `open`, plus whatever the log reports:
    /// [`Error::NotAJournal`](keylink_persist::Error::NotAJournal) for a journal
    /// this build cannot read or one that is damaged before its end, and, through
    /// [`Error::Restore`](keylink_persist::Error::Restore), `Gap`, `BadRecord`
    /// and `Delta` for a log that does not replay.
    pub async fn open(store: &S, id: &StateId) -> keylink_persist::Result<Opened<D, S, Deltas>> {
        let recovered = Versioned::open::<D>(store.claim(id).await?).await?;
        let torn = recovered.torn();
        let (restored, durable) = recovered.replayed()?;
        Ok(Opened {
            restored,
            torn,
            saver: Self {
                durable,
                state: PhantomData,
                strategy: Deltas(Compaction::DEFAULT),
            },
        })
    }
}

/// A claimed state, read, with the adapter that will keep writing it.
///
/// What [`Persist::open`] returns, and the reason it is a type rather than a
/// tuple: **there is no way to take the observer without taking the state.**
/// [`start_from`](Self::start_from) is the only method that hands over a
/// [`Persist`], and it hands over the state in the same expression, so the
/// shortest thing a reader can copy is the correct one.
///
/// Holding one of these holds the claim, which is what
/// `tests/persist.rs::a_second_open_on_one_state_busy` leans on.
#[must_use = "this holds the claim AND the state that was read back; dropping it \
              starts the next run from nothing"]
pub struct Opened<D, S: Snapshots, M> {
    restored: Option<D>,
    torn: Option<TornTail>,
    saver: Persist<D, S, M>,
}

impl<D, S: Snapshots, M> Opened<D, S, M> {
    /// Whether anything was stored. `false` is the first ever run, which is not
    /// a failure.
    ///
    /// Takes `&self`, so asking cannot lose the answer — the distinction an
    /// operator logs and a test asserts, without a route to the state.
    #[must_use]
    pub fn was_stored(&self) -> bool {
        self.restored.is_some()
    }

    /// What recovery cut from the end of the log, if anything. Log it.
    ///
    /// A crash during an append leaves a partly written record. It is removed
    /// and its size reported here. Nothing acknowledged is lost: the append never
    /// resolved, so its caller was never told it had happened.
    #[must_use]
    pub fn torn_tail(&self) -> Option<TornTail> {
        self.torn
    }
}

impl<D, S: Snapshots> Opened<D, S, Whole> {
    /// The state to start the actor from, and the observer that will keep it
    /// current.
    ///
    /// What was stored, migrated through [`Durable::migrate`] if it is older
    /// than this build — or `first()` when nothing ever was. `first` is a
    /// closure rather than a value so a restart does not build a starting state
    /// it then throws away.
    ///
    /// ```ignore
    /// let (state, saver) = Persist::<Ledger, _, Whole>::open(&store, &id).await?.start_from(Ledger::default);
    /// let ledger = ledger::setup(state).observe(saver).actor();
    /// ```
    pub fn start_from(self, first: impl FnOnce() -> D) -> (D, Persist<D, S, Whole>) {
        (self.restored.unwrap_or_else(first), self.saver)
    }
}

impl<D: Journaled, S: Snapshots> Opened<D, S, Deltas> {
    /// The state to start the actor from, and the observer that will keep it
    /// current. As for `Whole`, and one thing more: **the state's change buffer is
    /// bound to this store here**, before its first turn.
    ///
    /// Until it is, the buffer records nothing; from now on only this store can
    /// drain it. That is what stops a second store, attached to the same actor, from
    /// emptying changes this one is about to write.
    ///
    /// ```ignore
    /// let (state, saver) = Persist::<Ledger, _, Deltas>::open(&store, &id).await?.start_from(Ledger::default)?;
    /// let ledger = ledger::setup(state).observe(saver).actor();
    /// ```
    ///
    /// # Errors
    ///
    /// [`Error::ForeignChanges`](keylink_persist::Error::ForeignChanges) when the
    /// state `first` built already belongs to another store: a state has one.
    pub fn start_from(
        self,
        first: impl FnOnce() -> D,
    ) -> keylink_persist::Result<(D, Persist<D, S, Deltas>)> {
        let state = self.restored.unwrap_or_else(first);
        self.saver.durable.adopt(&state)?;
        Ok((state, self.saver))
    }
}

impl<D, S: Snapshots> Opened<D, S, Deltas> {
    /// Fold the log by this policy rather than [`Compaction::DEFAULT`].
    pub fn compaction(mut self, when: Compaction) -> Self {
        self.saver.strategy = Deltas(when);
        self
    }
}

impl<D, S: Snapshots, M> std::fmt::Debug for Opened<D, S, M> {
    /// Whether there was anything to restore, and nothing else. The state is the
    /// state's business.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Opened")
            .field("was_stored", &self.restored.is_some())
            .field("torn", &self.torn)
            .finish_non_exhaustive()
    }
}

impl<D, S> Observer<D> for Persist<D, S, Whole>
where
    D: Durable + 'static,
    S: Snapshots,
{
    fn name(&self) -> &'static str {
        "persist"
    }

    /// Take the snapshot synchronously, then write it before the reply.
    ///
    /// # The snapshot is taken **outside** the future, and that is the design
    ///
    /// [`Observer::after_turn`] is a plain `fn` returning a boxed future rather
    /// than an `async fn` precisely so this line can run before the future
    /// exists. The naive spelling —
    /// `Box::pin(async move { self.durable.store(&Snapshot::of(state)).await })` —
    /// holds `&D` across an await, so the future is `Send` only when `D: Sync`,
    /// and that bound would then be demanded of **every** state in the crate.
    /// Wave 0 probed the trap and Wave 2d hit it again on
    /// [`finish`](Observer::finish); `tests/persist.rs` keeps a state that is
    /// `Send` and not `Sync` and persists it, which is what makes this paragraph
    /// checkable rather than remembered.
    ///
    /// # A panicked turn is refused and never stored
    ///
    /// A handler that panicked half-way through leaves the state half-applied,
    /// and a half-applied state written to the disk is worse than the turn being
    /// lost: a restart would reproduce it. So the turn is refused and the actor
    /// ends.
    ///
    /// The [`Stop`] reason is not what an operator reads, on purpose. The loop
    /// folds a refusal of a *panicked* turn into
    /// [`Ended::Panicked`](crate::Ended::Panicked) carrying the handler's own
    /// message, because reporting the durability adapter for a fault in a
    /// `service!` method points at the wrong code.
    ///
    /// # A `match`, and not an `if let`, because the default here is **write**
    ///
    /// [`TurnOutcome`] is exhaustive and deliberately not `#[non_exhaustive]`.
    /// An `if let` on the one outcome that must not be stored puts every future
    /// outcome in the `else` branch, and this `else` branch commits bytes to a
    /// disk. A third outcome — `Cancelled`, say — would be persisted under a
    /// rule nobody wrote and nothing would say so. The `match` makes it `E0004`
    /// here, in keylink, at the line that decides; `representability.rs`'s
    /// `every_outcome_is_named` holds the same tripwire for a build without
    /// this feature.
    fn after_turn<'a>(&'a mut self, state: &'a D, turn: TurnInfo<'a>) -> TurnFut<'a> {
        let snapshot = match turn.outcome {
            TurnOutcome::Completed => Snapshot::of(state),
            TurnOutcome::Panicked { .. } => {
                return Box::pin(async {
                    Err(Stop::new(
                        "a panicked turn is not stored: the state may be half-applied",
                    ))
                })
            }
        };

        Box::pin(async move {
            self.durable.store(&snapshot).await?;
            Ok(())
        })
    }

    // No `finish`. The default — do nothing — is this adapter's policy, and the
    // paragraph on the type says why: a state written after the actor ended
    // could only be a state some caller was told was refused.
}

impl<D, S> Observer<D> for Persist<D, S, Deltas>
where
    D: Journaled + 'static,
    S: Snapshots,
{
    fn name(&self) -> &'static str {
        "persist"
    }

    /// Drain what the turn changed synchronously, then append it before the reply.
    ///
    /// Everything [`Persist<D, S, Whole>`]'s `after_turn` says about the snapshot
    /// being taken outside the future holds here for the same reason:
    /// [`Versioned::stage`] borrows the state, and the future it leaves behind
    /// holds only the bytes to write.
    ///
    /// # A panicked turn appends nothing, and drops what it recorded
    ///
    /// A handler that recorded a change and then panicked left the state
    /// half-applied. Its changes are drained and thrown away, so a restart
    /// reproduces the state from before the turn and not the half of it.
    fn after_turn<'a>(&'a mut self, state: &'a D, turn: TurnInfo<'a>) -> TurnFut<'a> {
        let staged = match turn.outcome {
            TurnOutcome::Completed => match self.durable.stage(state, self.strategy.0) {
                Ok(staged) => staged,
                // Another store owns this state's changes, or the log has no
                // positions left. Refused at the turn that finds out.
                Err(error) => return Box::pin(async move { Err(Stop::from(error)) }),
            },
            TurnOutcome::Panicked { .. } => {
                // Drained and thrown away. If this store does not own the changes
                // they are left alone: the turn is refused all the same.
                let _ = self.durable.discard(state);
                return Box::pin(async {
                    Err(Stop::new(
                        "a panicked turn is not stored: the state may be half-applied",
                    ))
                });
            }
        };

        Box::pin(async move {
            match staged.commit().await {
                Ok(()) => Ok(()),
                // The record may be stored. A refusal that said "it was not
                // committed" would invite a retry that applies it twice.
                Err(error @ keylink_persist::Error::Indeterminate { .. }) => {
                    Err(Stop::indeterminate(error))
                }
                Err(error) => Err(Stop::from(error)),
            }
        })
    }

    // No `finish`, for the reason the `Whole` observer gives.
}

impl<D, S: Snapshots, M> std::fmt::Debug for Persist<D, S, M> {
    /// The name and nothing else, because there is nothing else. The claim is
    /// an adapter's business, the snapshot bytes are the state's, and this type
    /// deliberately remembers nothing between turns.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Persist").finish_non_exhaustive()
    }
}
