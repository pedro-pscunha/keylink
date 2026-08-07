//! The one adapter the [`Observer`] port is owed: a state written after every
//! turn, in front of the reply.
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
//! # What it costs per turn — measured, on a named machine, on a stated day
//!
//! RFC-06 §7 asked for this number and never got one. It is much larger than
//! anybody guessed, so it is the first thing in this file after the placement.
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
//! **It is two `fsync`s and a `rename`, and it is *supposed* to be expensive.**
//! `keylink_persist::file::Files` syncs the temporary file and then the
//! directory, which is what makes "an answered call is a stored call" survive a
//! power cut rather than merely a process restart. An adapter that skipped the
//! second `fsync` would report a much better number and quietly mean something
//! weaker.
//!
//! So the number is not a defect to fix here; it is the price of the promise, and
//! the knob that trades it away is a debounce — which [`Observer::finish`] is
//! already the guaranteed final flush for. A caller who wants the other trade
//! today has it without any new machinery: write a state whose turns are cheap to
//! lose, or reach for an adapter whose `store` does not sync.
//!
//! Reproduce it with
//! `cargo test -p keylink --test persist -- --ignored --nocapture`, which prints
//! the same numbers and asserts nothing about any of them. There is no
//! wall-clock assertion anywhere in this wave.

use std::marker::PhantomData;

use keylink_persist::versioned::{Snapshot, Versioned};
use keylink_persist::{Durable, Snapshots, StateId};

use crate::{Ended, FinishFut, Observer, Stop, TurnFut, TurnInfo, TurnOutcome};

/// Stores the state after every turn it accepts, and refuses a turn that
/// panicked.
///
/// One of these owns one [`Claim`](keylink_persist::Claim) — the actor owns the
/// observer, so the claim dies with the actor and is free before
/// [`stop`](crate::LocalActor::stop) returns. That is what [`Observer`]'s "the
/// actor owns its observers" paragraph is for, and it is why this type is not
/// `Clone` and cannot be shared between two actors: two writers of one state is
/// the situation the claim exists to make impossible.
///
/// ```no_run
/// # use keylink::{Caller, Persist};
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
/// // `open`.
/// let (restored, saver) = Persist::<Ledger, _>::open(&store, &id).await?;
/// let ledger = ledger::actor_with(restored.unwrap_or_default(), vec![Box::new(saver)]);
///
/// ledger.post(1).await?;   // stored, and fsynced, before this returns
/// # Ok(())
/// # }
/// ```
///
/// # Put it **last** in the chain
///
/// The first observer to return `Err` stops the rest, so an observer placed
/// before this one can refuse a turn and prevent the write. One placed *after*
/// it cannot: the write has already happened, and the caller is still told
/// [`TurnLost::Refused`](crate::TurnLost::Refused). That asymmetry is the
/// chain's, not this type's — but "last" is the position in which it means what
/// a reader expects, so it is written here rather than left to be discovered.
///
/// # Two parameters, and why the second one is the **store**
///
/// `D` is the state and `S` is where its snapshots live. The claim type would be
/// the narrower parameter, and it is deliberately not the one here:
/// `Versioned<C>` demands `C: Claim`, a bound this file would have to name, and
/// `keylink_persist::Claim` is the trait whose one method writes **unframed**
/// bytes. `.mise/tasks/framing` reads exactly that — a file that never brings the
/// trait into scope cannot call its methods, whatever it calls its variables — so
/// naming the store instead of the claim keeps the strongest guarantee available
/// about this file: it has no route to an unframed write at all.
/// `Versioned<S::Claim>` needs no bound, because [`Snapshots`] already promises
/// one.
pub struct Persist<D, S: Snapshots> {
    /// The claim, wrapped so that everything written through it is framed.
    /// Owned, with no way back out to the raw claim.
    durable: Versioned<S::Claim>,
    /// **There is a committed change that is not on the disk yet.**
    ///
    /// Set before the write and cleared after it, so a write that failed leaves
    /// it standing — which is the one way [`Persist::finish`] finds work to do
    /// today, and the flag a debounce would set for every turn it skipped.
    ///
    /// This is **not** RFC-06 §4.2's deleted `Durable::revision`, which was an
    /// implementor-supplied counter with nothing connecting it to a mutation.
    /// This is set by the one place that knows a turn happened.
    dirty: bool,
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
}

impl<D: Durable, S: Snapshots> Persist<D, S> {
    /// Claim the state, read whatever is stored, and keep the claim.
    ///
    /// `Ok((None, _))` is the first ever run: nothing has been stored, which is
    /// not a failure. `Ok((Some(state), _))` is a restart, migrated through
    /// [`Durable::migrate`] if what was stored is older.
    ///
    /// **This is the only way to build one, and the reason is the pair it
    /// returns.** A constructor that took a claim and skipped the read would let
    /// a caller start an actor from `Default` and have the first turn overwrite
    /// a perfectly good stored state — silently, and exactly once per deploy.
    /// Handing the restored state back with the adapter that will keep writing it
    /// makes the read impossible to leave out.
    ///
    /// The claim is exclusive from here until this value is dropped. A second
    /// `open` on one [`StateId`] is [`Error::Busy`](keylink_persist::Error::Busy),
    /// not a second writer.
    ///
    /// # `D` appears only in the return, so it usually has to be named
    ///
    /// `Persist::<Ledger, _>::open(&store, &id)`. Nothing in the arguments says
    /// which state is being claimed, and inference reaches the observer end but
    /// not always the state end: handing the restored value to a generic
    /// `actor_with<A: Actor>` leaves `D` ambiguous even where exactly one type
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
    pub async fn open(store: &S, id: &StateId) -> keylink_persist::Result<(Option<D>, Self)> {
        let durable = Versioned::new(store.claim(id).await?);
        let restored = durable.load::<D>().await?;
        Ok((
            restored,
            Self {
                durable,
                dirty: false,
                state: PhantomData,
            },
        ))
    }
}

impl<D, S> Observer<D> for Persist<D, S>
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
    /// lost: a restart would reproduce it. So the turn is refused, the actor
    /// ends, and the flag below is not set.
    ///
    /// The [`Stop`] reason is not what an operator reads, on purpose. The loop
    /// folds a refusal of a *panicked* turn into
    /// [`Ended::Panicked`](crate::Ended::Panicked) carrying the handler's own
    /// message, because reporting the durability adapter for a fault in a
    /// `service!` method points at the wrong code.
    fn after_turn<'a>(&'a mut self, state: &'a D, turn: TurnInfo<'a>) -> TurnFut<'a> {
        if let TurnOutcome::Panicked { .. } = turn.outcome {
            return Box::pin(async {
                Err(Stop::new(
                    "a panicked turn is not stored: the state may be half-applied",
                ))
            });
        }

        let snapshot = Snapshot::of(state);
        // Before the write, so a write that fails leaves the flag standing.
        self.dirty = true;
        Box::pin(async move {
            self.durable.store(&snapshot).await?;
            self.dirty = false;
            Ok(())
        })
    }

    /// One last write, if the last one did not land.
    ///
    /// **Today this costs nothing on every path but one.**
    /// [`after_turn`](Observer::after_turn) stores on every turn it accepts and
    /// clears the flag, so an actor that stopped normally arrives here clean and
    /// this returns without touching the store. The one path that arrives dirty
    /// is a `store` that returned `Err`: that ends the actor immediately, so the
    /// state is complete and consistent and only the write is missing, and one
    /// more attempt before it is dropped is the difference between losing the
    /// last committed turn and not.
    ///
    /// `finish` runs **before** the state is dropped and before any waiter is
    /// told the actor ended, which is what makes that attempt possible at all.
    ///
    /// # The `Ended::Panicked` arm looks redundant, and a debounce is why it is not
    ///
    /// With a store on every turn, a dirty flag at this point implies the last
    /// turn *completed* — a panicked turn is refused above without setting it,
    /// and a failed store ends the actor before another turn can panic. So the
    /// check cannot fire today.
    ///
    /// It is here because it is the rule a debounce needs, and RFC-06 §7's
    /// debounce is the reason this method exists. The moment `after_turn` stops
    /// writing on every turn, "turn n completed and is unwritten, turn n+1
    /// panicked" becomes reachable, the flag is standing over a half-applied
    /// state, and [`Ended::Panicked`](crate::Ended::Panicked) is the only thing
    /// here that says so. Losing one committed turn is the correct trade against
    /// writing a state a restart would reproduce.
    ///
    /// A failure here is dropped rather than reported. There is nothing left to
    /// refuse — [`Observer::finish`] is infallible because the actor has already
    /// ended and its reason is already decided.
    fn finish<'a>(&'a mut self, state: &'a D, ended: &'a Ended) -> FinishFut<'a> {
        if !self.dirty || matches!(ended, Ended::Panicked { .. }) {
            return Box::pin(async {});
        }

        let snapshot = Snapshot::of(state);
        Box::pin(async move {
            if self.durable.store(&snapshot).await.is_ok() {
                self.dirty = false;
            }
        })
    }
}

impl<D, S: Snapshots> std::fmt::Debug for Persist<D, S> {
    /// The flag and nothing else. The claim is an adapter's business and the
    /// snapshot bytes are the state's.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Persist")
            .field("pending", &self.dirty)
            .finish_non_exhaustive()
    }
}
