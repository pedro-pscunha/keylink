//! The durability adapter, from outside the crate that owns it.
//!
//! Wave 5's whole subject: `keylink::Persist` is an [`Observer`] that writes the
//! state after every turn it accepts, so "an answered call is a stored call" is a
//! property of the loop rather than of a caller's discipline. Every test here
//! drives the **real** file store — no mock of the ledger, per the house rule —
//! and a "new process" is every handle to the old actor being dropped, which is
//! all a restart is as far as a claim can tell.
//!
//! The example in `keylink/examples/durable_ledger.rs` is the same story with a
//! served node in it, and it used to carry the caller loop this file makes
//! unnecessary.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use keylink::{
    ActorId, Allowlist, Caller, Ended, Eviction, Observer, Persist, Server, Stop, TurnFut, TurnInfo,
};
use keylink_persist::file::Files;
use keylink_persist::versioned::FRAME_LEN;
use keylink_persist::{Durable, RestoreError, Snapshots, StateId};

mod common;
use common::local_node;

keylink::service! {
    service Ledger;

    rpc Post(entry: u64) -> u64;
    rpc Entries() -> Vec<u64>;
    /// Applies the entry and **then** panics, so a half-applied turn is
    /// reachable and "not stored" is a claim with something behind it.
    rpc Bad(entry: u64) -> u64;
}

/// The state does not know it is durable. `Durable` is a second impl and
/// `Persist` is a third party to both.
#[derive(Default)]
struct Ledger {
    entries: Vec<u64>,
}

impl ledger::Actor for Ledger {
    async fn post(&mut self, _cx: Caller<'_>, entry: u64) -> u64 {
        self.entries.push(entry);
        self.entries.len() as u64
    }

    async fn entries(&mut self, _cx: Caller<'_>) -> Vec<u64> {
        self.entries.clone()
    }

    async fn bad(&mut self, _cx: Caller<'_>, entry: u64) -> u64 {
        self.entries.push(entry);
        panic!("bad entry");
    }
}

/// Eight bytes per entry, little-endian. The encoding is the state's decision and
/// neither the port nor the adapter looks inside one.
impl Durable for Ledger {
    const VERSION: u32 = 1;

    fn snapshot(&self) -> Vec<u8> {
        self.entries.iter().flat_map(|n| n.to_le_bytes()).collect()
    }

    fn restore(bytes: &[u8]) -> Result<Self, RestoreError> {
        if !bytes.len().is_multiple_of(8) {
            return Err(RestoreError::Malformed(format!(
                "a ledger is whole u64s, and this is {} bytes",
                bytes.len()
            )));
        }
        Ok(Self {
            entries: bytes
                .chunks_exact(8)
                .map(|c| u64::from_le_bytes(c.try_into().expect("chunks_exact(8)")))
                .collect(),
        })
    }

    fn migrate(_from: u32, _bytes: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> {
        Ok(None)
    }
}

// ─────────────────────────────────────────────────────────────────── fixtures

/// A scratch directory that removes itself.
struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("keylink-persist-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn store(name: &str) -> (Scratch, Files) {
    let scratch = Scratch::new(name);
    let files = Files::open(&scratch.0).await.expect("opens");
    (scratch, files)
}

fn state_id() -> StateId {
    StateId::new("ledgers/sp-01").expect("valid")
}

/// The snapshot file's path, built the way the adapter's documentation says the
/// layout is: one file per state, named by the id's segments.
///
/// Read raw, on purpose. Reading it back through `Versioned::load` would need a
/// claim, and the claim is exactly what a running actor is holding — so the only
/// way to look at what is on the disk *while* the actor lives is to look at the
/// bytes.
fn snapshot_path(scratch: &Scratch) -> std::path::PathBuf {
    scratch.0.join("ledgers").join("sp-01.snapshot")
}

/// An observer that refuses everything, so the chain's short-circuit is
/// reachable from a test.
struct Refusing;

impl Observer<Ledger> for Refusing {
    fn name(&self) -> &'static str {
        "refusing"
    }

    fn after_turn<'a>(&'a mut self, _state: &'a Ledger, _turn: TurnInfo<'a>) -> TurnFut<'a> {
        Box::pin(async { Err(Stop::new("refusing, always")) })
    }
}

// ──────────────────────────────────────────── the state outlives its process

/// **The criterion: an actor's state survives its own process.**
///
/// No caller loop, no `with_state`, no explicit commit. `Persist` is handed to
/// the actor and the turns are ordinary turns.
///
/// Made red by having `Persist::after_turn` return `Ok(())` without storing: the
/// second run then comes back empty.
#[tokio::test]
async fn a_state_survives_the_process_that_built_it() {
    let (_scratch, files) = store("survives").await;
    let id = state_id();

    // ── run one
    {
        let (restored, saver) = Persist::<Ledger, _>::open(&files, &id)
            .await
            .expect("claims and loads");
        assert!(restored.is_none(), "the first run has nothing stored");

        let ledger = ledger::actor_with(Ledger::default(), vec![Box::new(saver)]);
        for entry in [1, 2, 3] {
            ledger.post(entry).await.expect("running");
        }
        ledger.stop().await;
    }

    // ── run two: as far as the store can tell, a new process
    {
        let (restored, saver) = Persist::<Ledger, _>::open(&files, &id)
            .await
            .expect("claims and loads");
        let state = restored.expect("run one stored something");
        assert_eq!(state.entries, vec![1, 2, 3]);

        let ledger = ledger::actor_with(state, vec![Box::new(saver)]);
        ledger.post(4).await.expect("running");
        ledger.stop().await;
    }

    // ── run three, which stores nothing, so what it reads is run two's write
    let (restored, _saver) = Persist::<Ledger, _>::open(&files, &id)
        .await
        .expect("claims and loads");
    assert_eq!(restored.expect("stored").entries, vec![1, 2, 3, 4]);
}

/// **The criterion: an answered call is a stored call — and the bytes are 4c's
/// frame.**
///
/// Both halves in one test, because both are read off the same file. `post`
/// returning means the observer chain already ran and its write already
/// completed, so there is no sleep here and nothing to wait for: the assertion
/// runs on the next line.
///
/// The frame half is read byte by byte rather than through `Versioned::load`,
/// which would need the claim the actor is holding. Magic, layout, version and
/// then the payload — the table in `keylink-persist/src/versioned.rs`.
///
/// Made red by storing `state.snapshot()` through the raw claim instead of a
/// `Snapshot` — the magic disappears — or by dropping the store, when the file
/// does not exist at all.
#[tokio::test]
async fn an_answered_call_is_a_stored_call_and_its_bytes_are_framed() {
    let (scratch, files) = store("answered").await;
    let (_restored, saver) = Persist::open(&files, &state_id())
        .await
        .expect("claims and loads");

    let ledger = ledger::actor_with(Ledger::default(), vec![Box::new(saver)]);
    assert_eq!(ledger.post(0x2A).await.expect("running"), 1);

    // Nothing between the awaited call and this read.
    let framed = std::fs::read(snapshot_path(&scratch)).expect("the reply implies the write");

    assert_eq!(&framed[..6], b"klsnap", "the magic 4c prepends");
    assert_eq!(
        &framed[6..8],
        &1u16.to_le_bytes(),
        "the header's own layout"
    );
    assert_eq!(
        &framed[8..12],
        &Ledger::VERSION.to_le_bytes(),
        "the version of the type that produced the payload"
    );
    assert_eq!(framed.len(), FRAME_LEN + 8, "one entry, eight bytes");
    assert_eq!(
        Ledger::restore(&framed[FRAME_LEN..])
            .expect("restores")
            .entries,
        vec![0x2A]
    );

    ledger.stop().await;
}

// ─────────────────────────────────────────────── what is deliberately not stored

/// **The criterion: a panicked turn is not stored, and it ends the actor.**
///
/// `bad` applies the entry and *then* panics, so the state on the actor's task
/// really does hold it when the observer looks. Wave 2a is what makes the refusal
/// precise: the fault is an explicit `TurnOutcome::Panicked`, so the adapter
/// refuses on the outcome rather than inferring anything.
///
/// Two assertions, and both matter. The file still holds the turn before the
/// fault, which is the "not stored" half. And `ended` is `Ended::Panicked`
/// carrying the handler's message and **not** `Ended::Observer` naming the
/// adapter — the loop folds a refusal of a panicked turn into the fault, because
/// reporting `persist` here would point an operator at the wrong code.
///
/// **The disk is read before `ended` is awaited, and that order is the test's.**
/// Reading it afterwards makes the break below *hang* instead of failing: an
/// adapter that accepts a panicked turn does not end the actor, so `ended()`
/// never resolves and a red-test reports nothing at all. The store assertion runs
/// first, where it fails loudly, and the ending is checked second. Nothing races:
/// the observer chain runs before the caller is answered, so a panic that has
/// reached this task is a panic the observer has already seen.
///
/// Made red by making the `TurnOutcome::Panicked` arm never fire — the file then
/// holds `[1, 99]` — and, coarsely, by having it refuse a *completed* turn
/// instead, when the first `post` is refused and the actor is already gone.
#[tokio::test]
async fn a_panicked_turn_is_not_stored_and_ends_the_actor() {
    let (scratch, files) = store("panicked").await;
    let (_restored, saver) = Persist::open(&files, &state_id())
        .await
        .expect("claims and loads");

    let ledger = ledger::actor_with(Ledger::default(), vec![Box::new(saver)]);
    ledger.post(1).await.expect("running");

    // Spawned, because the panic is resumed in whoever awaited the call.
    let boom = {
        let ledger = ledger.clone();
        tokio::spawn(async move { ledger.bad(99).await })
    };
    assert!(boom
        .await
        .expect_err("the panic reaches the caller")
        .is_panic());

    let framed = std::fs::read(snapshot_path(&scratch)).expect("the first post was stored");
    assert_eq!(
        Ledger::restore(&framed[FRAME_LEN..])
            .expect("restores")
            .entries,
        vec![1],
        "the half-applied turn must not be on the disk"
    );

    match ledger.ended().await {
        Ended::Panicked { message } => assert!(message.contains("bad entry"), "got {message}"),
        other => panic!("got {other:?}"),
    }
}

/// An observer **before** `Persist` that refuses stops the write.
///
/// The chain's first `Err` stops the rest, so this is what the adapter's "put it
/// last" paragraph rests on: a turn some other observer refused was never
/// committed, and it must not be on the disk either.
///
/// Made red by swapping the two observers, which is the mistake the paragraph
/// exists to prevent: `Persist` then writes before `Refusing` gets a say, and the
/// file holds an entry the caller was told was refused.
#[tokio::test]
async fn a_refusal_before_persist_prevents_the_store() {
    let (scratch, files) = store("refused").await;
    let (_restored, saver) = Persist::open(&files, &state_id())
        .await
        .expect("claims and loads");

    let ledger = ledger::actor_with(Ledger::default(), vec![Box::new(Refusing), Box::new(saver)]);
    assert!(ledger.post(7).await.is_err(), "the turn was refused");

    match ledger.ended().await {
        Ended::Observer { name, .. } => assert_eq!(name, "refusing"),
        other => panic!("got {other:?}"),
    }
    assert!(
        !snapshot_path(&scratch).exists(),
        "a refused turn must leave nothing on the disk"
    );
}

// ─────────────────────────────────────────────────────────────────── the claim

/// **The criterion: the claim is free the instant `stop().await` returns.**
///
/// This is what `run_loop`'s three-drop teardown buys, and `drop(hooks)` being
/// first is what buys it: the observers — and the claim one of them owns — are
/// released before any waiter is told the actor ended. There is no sleep here and
/// there must not be one; a claim that is free "shortly afterwards" is a claim a
/// restarting process races.
///
/// Red-tested by leaking the hooks instead of dropping them
/// (`std::mem::forget(hooks)` in `keylink/src/local.rs`), which is the shape of
/// any teardown that publishes the ending first.
#[tokio::test]
async fn the_claim_is_free_the_instant_stop_returns() {
    let (_scratch, files) = store("claim").await;
    let id = state_id();
    let (_restored, saver) = Persist::open(&files, &id).await.expect("claims and loads");

    let ledger = ledger::actor_with(Ledger::default(), vec![Box::new(saver)]);
    ledger.post(1).await.expect("running");
    ledger.stop().await;

    // The very next thing, on the same task.
    files
        .claim(&id)
        .await
        .expect("the claim must be free when stop() has returned");
}

/// **The criterion: two `open`s on one `StateId` — the second is `Error::Busy`.**
///
/// Not a silent second writer, and not a transient error to retry: it means
/// something else believes it owns this state, which is the whole reason the port
/// has a claim in it at all.
#[tokio::test]
async fn a_second_open_on_one_state_is_busy() {
    let (_scratch, files) = store("busy").await;
    let id = state_id();

    let (_restored, _held) = Persist::<Ledger, _>::open(&files, &id)
        .await
        .expect("claims and loads");

    // A `match` and not `expect_err`, because the success arm carries a `Ledger`
    // and a `Persist`, and neither has a reason to be `Debug` for a test's sake.
    match Persist::<Ledger, _>::open(&files, &id).await {
        Err(refused) => assert!(
            matches!(refused, keylink_persist::Error::Busy { .. }),
            "got {refused:?}"
        ),
        Ok(_) => panic!("a second open on a claimed state must be refused"),
    }
}

// ───────────────────────────────────────────────────────── the keyed half

/// **The criterion Wave 3 made possible: a KEYED actor's state survives its own
/// process.**
///
/// Say plainly why this could not be written before. A keyed actor used to run a
/// second loop, generated into the user's crate, which handed the dispatch an
/// **empty** hook slice — so a keyed actor had no observers, no `finish` and
/// therefore no route to durability at all. Wave 3 merged the two loops into
/// `run_loop`, and `spawn_wired` is the door that carries the triple. This test
/// is the payoff: the same `Persist`, on the keyed path, over two real nodes and
/// the generated `KeyedClient`, because a keyed actor answering a peer is the
/// only shape a keyed actor has.
///
/// Made red the same way the local test is — a `Persist::after_turn` that does
/// not store — and it goes red for the keyed reason too: put `Vec::new()` where
/// `spawn_wired` passes its observers on and the second mount comes back empty.
#[tokio::test]
async fn a_keyed_actors_state_survives_its_own_process() {
    let (_scratch, files) = store("keyed").await;
    let id = state_id();
    let actor = ActorId::new("ledgers/sp-01").expect("valid");

    let server = local_node().await;
    let client_node = local_node().await;

    for (run, entry, expected) in [(1u8, 10u64, vec![10u64]), (2, 20, vec![10, 20])] {
        let (restored, saver) = Persist::<Ledger, _>::open(&files, &id)
            .await
            .expect("claims and loads");
        let system = ledger::System::builder(Eviction::Never).capacity(4).build();
        system
            .mount(
                actor.clone(),
                ledger::spawn_wired(
                    restored.unwrap_or_default(),
                    vec![Box::new(saver)],
                    ledger::Feeds::new(),
                ),
            )
            .expect("mounts");

        let running = Server::builder(&server)
            .service(ledger::keyed_service(&system, Allowlist::anyone()).build())
            .spawn()
            .expect("one service, no duplicate ALPN");
        let client = ledger::KeyedClient::connect(&client_node, server.endpoint().addr())
            .await
            .expect("the client dials the actor system");

        ledger::at(&client, actor.clone())
            .post(entry)
            .await
            .expect("the keyed actor answers");
        assert_eq!(
            ledger::at(&client, actor.clone())
                .entries()
                .await
                .expect("the keyed actor answers"),
            expected,
            "run {run}"
        );

        // Ends the actor, which drops its observers, which releases the claim —
        // the same teardown the local path runs, because it is the same loop.
        assert!(system.stop(&actor).await, "the actor was mounted");
        drop(running);
    }

    let (restored, _saver): (Option<Ledger>, _) =
        Persist::open(&files, &id).await.expect("claims and loads");
    assert_eq!(
        restored.expect("the keyed runs stored something").entries,
        vec![10, 20],
        "a keyed actor's state must outlive its process exactly as a local one's does"
    );
}

// ───────────────────────────────────────────────── the bound that is NOT there

/// A state holding a `Cell` is `Send` and **not** `Sync`, and it is persisted.
///
/// This is the trap the whole shape of `Observer::after_turn` exists to avoid.
/// The naive adapter —
/// `Box::pin(async move { versioned.store(&Snapshot::of(state)).await })` —
/// holds `&A` across an await, so its future is `Send` only when `A: Sync`, and
/// the bound then propagates to every actor in the crate. `Persist` takes the
/// snapshot **synchronously**, before the future exists, so it needs nothing of
/// the kind.
///
/// The state below holds a real `Cell<u64>` *and* a `RefCell<u64>`, so neither
/// member's removal can make the claim vacuous — the house rule a unit-struct
/// stand-in earned the hard way.
///
/// # Two breaks, and they fail in two different places, which is the useful part
///
/// - Add `Sync` to the impl's bound (`D: Durable + Sync + 'static` in
///   `keylink/src/persist.rs`) and **this** test stops compiling, naming the
///   state: `error[E0277]: `Cell<u64>` cannot be shared between threads safely`,
///   `required because it appears within the type `Counter``. Every `Ledger` test
///   in this file still passes, which is what makes the break discriminating
///   rather than merely red.
/// - Write the naive `after_turn` — `store(&Snapshot::of(state))` inside the
///   `async move` — and **keylink itself** stops compiling, before any test is
///   reached: `error: future cannot be sent between threads safely`, at
///   `persist.rs`'s `Box::pin`. The bound is demanded of the generic `D`, so the
///   library refuses it for everybody rather than for this state.
///
/// The second break is why the adapter cannot quietly acquire the bound; the
/// first is why nothing else in the stack — `service!`, `spawn_fed`, `run_loop` —
/// can either.
#[derive(Default)]
struct Counter {
    /// One reason to be `!Sync`.
    hits: std::cell::Cell<u64>,
    /// A second, independent one.
    misses: std::cell::RefCell<u64>,
}

keylink::service! {
    service Counting;

    rpc Hit() -> u64;
}

impl counting::Actor for Counter {
    async fn hit(&mut self, _cx: Caller<'_>) -> u64 {
        self.hits.set(self.hits.get() + 1);
        *self.misses.borrow_mut() += 1;
        self.hits.get()
    }
}

impl Durable for Counter {
    const VERSION: u32 = 1;

    fn snapshot(&self) -> Vec<u8> {
        self.hits.get().to_le_bytes().to_vec()
    }

    fn restore(bytes: &[u8]) -> Result<Self, RestoreError> {
        let hits: [u8; 8] = bytes.try_into().map_err(|_| {
            RestoreError::Malformed(format!("a counter is 8 bytes, not {}", bytes.len()))
        })?;
        Ok(Self {
            hits: std::cell::Cell::new(u64::from_le_bytes(hits)),
            misses: std::cell::RefCell::new(0),
        })
    }

    fn migrate(_from: u32, _bytes: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> {
        Ok(None)
    }
}

#[tokio::test]
async fn a_state_that_is_not_sync_is_still_persisted() {
    let (_scratch, files) = store("notsync").await;
    let id = StateId::new("counters/hits").expect("valid");

    {
        let (restored, saver) = Persist::<Counter, _>::open(&files, &id)
            .await
            .expect("claims and loads");
        let counter = counting::actor_with(restored.unwrap_or_default(), vec![Box::new(saver)]);
        assert_eq!(counter.hit().await.expect("running"), 1);
        assert_eq!(counter.hit().await.expect("running"), 2);
        counter.stop().await;
    }

    let (restored, _saver): (Option<Counter>, _) =
        Persist::open(&files, &id).await.expect("claims and loads");
    assert_eq!(restored.expect("stored").hits.get(), 2);
}

// ───────────────────────────────────────────────────────────── the measurement

/// **Not a test. The number RFC-06 §7 asked for, in a form anybody can re-run.**
///
/// It asserts nothing about time — there is no wall-clock assertion in this wave
/// — and it is `#[ignore]`d so the gate never pays for it. Run it with:
///
/// ```sh
/// cargo test -p keylink --test persist -- --ignored --nocapture
/// ```
///
/// The state is a fixed 8-byte `Counter`, so the payload is 20 framed bytes on
/// every turn and the answer is not a function of how far the run got. What is
/// being measured is one `Versioned::store` through the file adapter: a temporary
/// file, an `fsync`, a `rename` and an `fsync` of the directory. The control is
/// the same turns with no observer at all, so the difference is the store and not
/// the mailbox.
///
/// The number this produced, on the machine and day named, is in
/// `keylink/src/persist.rs`'s module documentation.
#[tokio::test]
#[ignore = "a measurement, not an assertion: run with --ignored --nocapture"]
async fn the_per_turn_store_cost() {
    use n0_future::time::Instant;

    const TURNS: u32 = 1_000;

    let (_scratch, files) = store("cost").await;
    let id = StateId::new("counters/cost").expect("valid");

    let bare = counting::actor(Counter::default());
    let started = Instant::now();
    for _ in 0..TURNS {
        bare.hit().await.expect("running");
    }
    let without = started.elapsed();
    bare.stop().await;

    let (_restored, saver) = Persist::open(&files, &id).await.expect("claims and loads");
    let stored = counting::actor_with(Counter::default(), vec![Box::new(saver)]);
    let started = Instant::now();
    for _ in 0..TURNS {
        stored.hit().await.expect("running");
    }
    let with = started.elapsed();
    stored.stop().await;

    println!("── per-turn cost over {TURNS} turns of an 8-byte state");
    println!("   no observer : {:>8.1} µs/turn", micros(without, TURNS));
    println!("   Persist     : {:>8.1} µs/turn", micros(with, TURNS));
    println!(
        "   the store   : {:>8.1} µs/turn",
        micros(with, TURNS) - micros(without, TURNS)
    );
}

/// Microseconds per turn, as a float, for the print above. Not used by any
/// assertion.
fn micros(elapsed: std::time::Duration, turns: u32) -> f64 {
    elapsed.as_secs_f64() * 1_000_000.0 / f64::from(turns)
}
