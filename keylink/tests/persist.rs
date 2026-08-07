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
    ActorId, Allowlist, Applied, CallId, Caller, Context, Deferred, Ended, Eviction,
    IdempotencyKey, KeyState, Observer, Persist, Replied, Reply, Server, Stop, TurnFut, TurnInfo,
};
use std::time::Duration;

use keylink_persist::file::Files;
use keylink_persist::versioned::FRAME_LEN;
use keylink_persist::{Claim, Durable, RestoreError, Snapshots, StateId};

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

/// A **second adapter for the port**, whose `store` can be made to fail exactly
/// once.
///
/// Not a mock of anything the house rule protects — not the ledger, not the
/// mailbox, not the connection. It is the port being implemented from outside
/// the crate that declares it, which is the only reason a port exists, and
/// `.mise/tasks/framing` permits it for that reason: this file **implements**
/// `Claim` rather than merely importing it, and the gate now tells those apart.
///
/// It exists because a real `Files` cannot be asked to fail **transiently**.
/// `limit()` fails permanently and removing the directory breaks the lock the
/// claim holds — RFC-07 §6.8 recorded exactly that as the reason the failed-store
/// path shipped with no test on it at all.
///
/// One state, and the [`StateId`] is ignored: the tests that need two states, or
/// exclusion between two holders, use `Files`.
struct Flaky {
    /// Armed between two turns. **Cleared by the failure it causes**, so the
    /// next attempt succeeds — which is the whole point. A store that stayed
    /// broken would make the break below go red for the wrong reason, and a red
    /// test that cannot say which rule it caught is not a guard.
    failing: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// What is "on the disk": framed bytes, exactly what `Files` would hold.
    stored: std::sync::Arc<std::sync::Mutex<Option<Vec<u8>>>>,
}

impl Flaky {
    fn new() -> Self {
        Self {
            failing: std::sync::Arc::default(),
            stored: std::sync::Arc::default(),
        }
    }

    /// The next `store`, and only the next one, returns `Err`.
    fn fail_the_next_store(&self) {
        self.failing
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// The stored state, read the way the `Files` tests read the file: raw,
    /// because the claim is what the running actor is holding.
    fn on_disk<D: Durable>(&self) -> Option<D> {
        let framed = self.stored.lock().expect("not poisoned").clone();
        framed.map(|bytes| D::restore(&bytes[FRAME_LEN..]).expect("restores"))
    }
}

impl Snapshots for Flaky {
    type Claim = FlakyClaim;

    async fn claim(&self, _id: &StateId) -> keylink_persist::Result<FlakyClaim> {
        Ok(FlakyClaim {
            failing: std::sync::Arc::clone(&self.failing),
            stored: std::sync::Arc::clone(&self.stored),
        })
    }
}

/// [`Flaky`]'s claim. Exclusivity is deliberately not modelled:
/// `a_second_open_on_one_state_is_busy` holds that against the real adapter, and
/// this one exists for one property.
struct FlakyClaim {
    failing: std::sync::Arc<std::sync::atomic::AtomicBool>,
    stored: std::sync::Arc<std::sync::Mutex<Option<Vec<u8>>>>,
}

impl Claim for FlakyClaim {
    async fn load(&self) -> keylink_persist::Result<Option<Vec<u8>>> {
        Ok(self.stored.lock().expect("not poisoned").clone())
    }

    async fn store(&mut self, bytes: &[u8]) -> keylink_persist::Result<()> {
        if self
            .failing
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(keylink_persist::Error::Io(std::io::Error::other(
                "the store said no, once",
            )));
        }
        *self.stored.lock().expect("not poisoned") = Some(bytes.to_vec());
        Ok(())
    }
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
        let opened = Persist::<Ledger, _>::open(&files, &id)
            .await
            .expect("claims and loads");
        assert!(!opened.was_stored(), "the first run has nothing stored");

        // `start_from` and not `Ledger::default()`, even here where the two are
        // the same value. This line is the one an author copies.
        let (state, saver) = opened.start_from(Ledger::default);
        let ledger = ledger::actor_with(state, vec![Box::new(saver)]);
        for entry in [1, 2, 3] {
            ledger.post(entry).await.expect("running");
        }
        ledger.stop().await;
    }

    // ── run two: as far as the store can tell, a new process
    {
        let opened = Persist::<Ledger, _>::open(&files, &id)
            .await
            .expect("claims and loads");
        assert!(opened.was_stored(), "run one stored something");
        let (state, saver) = opened.start_from(Ledger::default);
        assert_eq!(state.entries, vec![1, 2, 3]);

        let ledger = ledger::actor_with(state, vec![Box::new(saver)]);
        ledger.post(4).await.expect("running");
        ledger.stop().await;
    }

    // ── run three, which stores nothing, so what it reads is run two's write
    let opened = Persist::<Ledger, _>::open(&files, &id)
        .await
        .expect("claims and loads");
    assert!(opened.was_stored(), "run two stored something");
    let (state, _saver) = opened.start_from(Ledger::default);
    assert_eq!(state.entries, vec![1, 2, 3, 4]);
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
    let (state, saver) = Persist::open(&files, &state_id())
        .await
        .expect("claims and loads")
        .start_from(Ledger::default);

    let ledger = ledger::actor_with(state, vec![Box::new(saver)]);
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
    let (state, saver) = Persist::open(&files, &state_id())
        .await
        .expect("claims and loads")
        .start_from(Ledger::default);

    let ledger = ledger::actor_with(state, vec![Box::new(saver)]);
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
    let (state, saver) = Persist::open(&files, &state_id())
        .await
        .expect("claims and loads")
        .start_from(Ledger::default);

    let ledger = ledger::actor_with(state, vec![Box::new(Refusing), Box::new(saver)]);
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

/// **The criterion: what is stored is exactly what was answered — in the
/// direction that is easy to lose.** A turn whose store failed was reported to
/// its caller as *not committed*, so it must not reach the store afterwards
/// either.
///
/// This is the path RFC-07 §6.8 listed as untested, and the version this
/// replaced got it wrong. `Persist` kept a `dirty` flag set before the write,
/// and `Observer::finish` stored the state one last time whenever the flag was
/// still standing. The only thing that could leave it standing was a `store`
/// that had already returned `Err` — the turn whose caller had already been
/// handed a refusal. So the adapter wrote **precisely** the turn it had told
/// somebody was not written, and a caller who acted on that refusal by replaying
/// applied the entry twice.
///
/// # Over two real nodes, because the promise is a peer's
///
/// A local caller cannot see this: the generated handle collapses
/// `Answered::Refused` into `MailboxClosed` — *"the actor is no longer
/// running"* — and `ended()` is the only local route to the reason. A **peer**
/// is told `observer refused: persist`, produced by `local.rs` under a comment
/// reading *"The turn ran and was not committed"*, and that sentence is the
/// whole of what the caller has to act on. Asserting it here is what makes the
/// fix "the code was made to match the promise" rather than a preference.
///
/// Made red by putting the retry back — a `finish` that stores when a flag set
/// before the write is still standing. `Flaky` then holds `[1, 2]` and the
/// message names the double apply.
#[tokio::test]
async fn a_turn_whose_store_failed_is_not_written_on_the_way_out() {
    let store = Flaky::new();
    let opened = Persist::<Ledger, _>::open(&store, &state_id())
        .await
        .expect("claims and loads");
    assert!(!opened.was_stored(), "the first run has nothing stored");
    let (state, saver) = opened.start_from(Ledger::default);

    let server = local_node().await;
    let client_node = local_node().await;

    let ledger = ledger::actor_with(state, vec![Box::new(saver)]);
    let _running = ledger
        .serve(&server, Allowlist::anyone())
        .expect("one service, no duplicate ALPN");
    let client = ledger::Client::connect(&client_node, server.endpoint().addr())
        .await
        .expect("the client dials the actor");

    client.post(1).await.expect("the peer's call is answered");

    store.fail_the_next_store();
    let refused = client
        .post(2)
        .await
        .expect_err("a store that failed refuses its turn");
    assert!(
        refused.to_string().contains("observer refused: persist"),
        "the promise the peer acts on: got {refused}"
    );

    // `ended()` resolving means `run_loop` ran every observer's `finish` and
    // dropped the state, so what is stored now is final. Read afterwards on
    // purpose: the whole finding is about what happens *after* the refusal.
    match ledger.ended().await {
        Ended::Observer { name, .. } => assert_eq!(name, "persist"),
        other => panic!("got {other:?}"),
    }

    assert_eq!(
        store
            .on_disk::<Ledger>()
            .expect("the first post was stored")
            .entries,
        vec![1],
        "entry 2's caller was told the turn was not committed, so it must not be \
         stored: a peer or an operator who takes that refusal at its word and \
         replays applies the entry twice"
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
    let (state, saver) = Persist::open(&files, &id)
        .await
        .expect("claims and loads")
        .start_from(Ledger::default);

    let ledger = ledger::actor_with(state, vec![Box::new(saver)]);
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
///
/// Watched to fail — it was disclosed as unwatched and it is not any more. Make
/// `Files::claim` hand out a `FileClaim` on `TryLockError::WouldBlock` instead
/// of `Error::Busy`, which is the shape of any adapter that opens the file and
/// forgets to lock it: `a second open on a claimed state must be refused`.
#[tokio::test]
async fn a_second_open_on_one_state_is_busy() {
    let (_scratch, files) = store("busy").await;
    let id = state_id();

    // Holding the `Opened` holds the claim: the state and the observer are both
    // still inside it, so there is nothing to bind and nothing to drop early.
    let _held = Persist::<Ledger, _>::open(&files, &id)
        .await
        .expect("claims and loads");

    let refused = Persist::<Ledger, _>::open(&files, &id)
        .await
        .expect_err("a second open on a claimed state must be refused");
    assert!(
        matches!(refused, keylink_persist::Error::Busy { .. }),
        "got {refused:?}"
    );
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
        let (state, saver) = Persist::<Ledger, _>::open(&files, &id)
            .await
            .expect("claims and loads")
            .start_from(Ledger::default);
        let system = ledger::System::builder(Eviction::Never).capacity(4).build();
        system
            .mount(
                actor.clone(),
                ledger::spawn_wired(state, vec![Box::new(saver)], ledger::Feeds::new()),
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
        // Dropping the observers is the whole of what durability needs here:
        // every accepted turn was already stored before its reply.
        //
        // `shutdown` rather than `stop`: Wave 4b deleted `System::stop`, whose
        // rustdoc claimed `tokio::time::timeout` composes when a fired timeout
        // dropped the `JoinHandle` and detached a live actor still holding this
        // very `Claim`. Every caller names a budget now.
        //
        // `Some(Ended::Stopped)` and not `is_some()`, and the budget's old
        // justification is gone with it.
        //
        // The comment here used to read: the 5 s is generous because "this
        // actor's `Persist` observer stores on the way out", and `Some(..)` is
        // the claim that "the drain finished inside the budget". Both were
        // wrong. `shutdown` answers `Some` whenever the id was **mounted**, and
        // an expired drain answers `Some(Ended::Vanished)` — so the assertion
        // could not see the property the number was chosen for. And this
        // adapter no longer writes on the way out at all: `Persist` implements
        // no `Observer::finish`, so the teardown does no I/O and there is no
        // `fsync` for a budget to leave room for.
        //
        // Verified rather than assumed, and the result is the honest reason the
        // number is still 5 s: **a budget of one nanosecond passes this test**,
        // because the drain completes on the first poll. So the 5 s buys
        // headroom against a loaded CI box and nothing else, and this assertion
        // is NOT watched to fail — nothing available here produces a
        // `Vanished`. It is still strictly stronger than `is_some()`, which
        // accepted every ending there is.
        match system.shutdown(&actor, Duration::from_secs(5)).await {
            Some(Ended::Stopped) => {}
            other => panic!("the actor was mounted and must drain: got {other:?}"),
        }
        drop(running);
    }

    let opened = Persist::<Ledger, _>::open(&files, &id)
        .await
        .expect("claims and loads");
    assert!(opened.was_stored(), "the keyed runs stored something");
    let (state, _saver) = opened.start_from(Ledger::default);
    assert_eq!(
        state.entries,
        vec![10, 20],
        "a keyed actor's state must outlive its process exactly as a local one's does"
    );
}

// ────────────────────────── the path the headline used to be false on, and is not

keylink::service! {
    service Teller;

    /// Parked, and answered from a later `approve` — through
    /// [`Deferred::send`], which is the call this whole section is about.
    #[defer]
    rpc Withdraw(amount: u64) -> u64;

    /// Debits every parked withdrawal and hands each caller its receipt.
    rpc Approve() -> u64;

    /// How many withdrawals are parked. An ordinary `rpc`, so the test below
    /// waits for a park by **asking** rather than by sleeping — `tests/defer.rs`
    /// established the idiom and the reason: a `Waiting()` call takes a mailbox
    /// permit like anything else, so it can only answer once the `withdraw` turn
    /// has finished.
    rpc Waiting() -> u64;
}

/// A balance, and the withdrawals waiting for somebody to approve them.
///
/// `waiting` is **not** part of the snapshot and cannot be: a [`Deferred`] is a
/// live channel to a caller in this process. A restart answers nothing — the
/// loop drains parked replies as `TurnLost::Gone` on the way out — which is a
/// second reason a parked reply and a durable state need thinking about
/// together.
#[derive(Default)]
struct Vault {
    balance: u64,
    waiting: Vec<(u64, Deferred<u64>)>,
}

impl teller::Actor for Vault {
    async fn withdraw(&mut self, _cx: Caller<'_>, reply: Reply<u64>, amount: u64) -> Replied<u64> {
        let (deferred, replied) = reply.later();
        self.waiting.push((amount, deferred));
        replied
    }

    async fn approve(&mut self, _cx: Caller<'_>) -> u64 {
        let approved = self.waiting.len() as u64;
        // `mem::take` and not `drain`, because the loop body touches a second
        // field of `self`.
        for (amount, deferred) in std::mem::take(&mut self.waiting) {
            self.balance -= amount;
            // The receipt is **parked here** rather than crossing here. It waits
            // for this turn's observer chain, and the store inside it, exactly as
            // this method's own return value does.
            deferred.send(self.balance);
        }
        approved
    }

    async fn waiting(&mut self, _cx: Caller<'_>) -> u64 {
        self.waiting.len() as u64
    }
}

impl Durable for Vault {
    const VERSION: u32 = 1;

    fn snapshot(&self) -> Vec<u8> {
        self.balance.to_le_bytes().to_vec()
    }

    fn restore(bytes: &[u8]) -> Result<Self, RestoreError> {
        let balance: [u8; 8] = bytes.try_into().map_err(|_| {
            RestoreError::Malformed(format!("a vault is 8 bytes, not {}", bytes.len()))
        })?;
        Ok(Self {
            balance: u64::from_le_bytes(balance),
            waiting: Vec::new(),
        })
    }

    fn migrate(_from: u32, _bytes: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> {
        Ok(None)
    }
}

/// Wait for `want` withdrawals to be parked by asking, not by sleeping.
/// `tests/defer.rs::parked`, with this file's names.
async fn waiting_for(vault: &teller::Handle<Vault>, want: u64) {
    for _ in 0..500 {
        if vault.waiting().await.expect("the actor is running") == want {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("{want} withdrawals never parked");
}

/// **A `#[defer]` reply that its turn did not store does not reach its caller.**
///
/// # INVERTED, 2026-08-07. This test used to pin the defect.
///
/// It was written as `a_deferred_reply_is_answered_before_its_turn_is_stored`,
/// and it asserted the opposite of what it asserts now: that the withdrawal's
/// caller **did** receive a receipt for 60 while the disk still said 100. Its
/// own doc said that if `Deferred::send` were ever made to wait for the observer
/// chain the test would go red, and that the redness was the notice. It was made
/// to wait, it went red — `the parked caller was answered from inside the
/// handler: MailboxClosed` — and this is that notice, honoured rather than
/// deleted.
///
/// What changed is `parking.rs`: a value handed to [`Deferred::send`] during a
/// turn is held until that turn's observer chain has accepted it, and released
/// with the same verdict the turn's own caller is answered by. So the window
/// this test used to demonstrate — handler mutates the state → the caller has
/// its answer → ~7.6 ms of `fsync` → a crash in between — is not a window any
/// more. `#[defer]`'s point is untouched: the reply still outlives the turn that
/// **received** it.
///
/// # What it asserts now
///
/// A crash is not something a test can arrange, so the commit point is shown
/// with the other failure that lands in the same place: the store **fails**. The
/// approving turn debits, sends the receipt, and is then refused by `persist` —
/// and all three callers are told the same thing. The withdrawal's caller gets a
/// refusal rather than a receipt, and the balance on the disk is still 100, so
/// there is no receipt anywhere for a debit that is not stored.
///
/// A `Handle` folds every loss into `MailboxClosed`, so the precise
/// `TurnLost::Refused { name: "persist" }` underneath is asserted in
/// `local.rs::a_released_reply_is_refused_with_the_turn_that_released_it`, where
/// the variant is visible.
///
/// Watched to fail from both ends, because two facts have to hold at once and
/// one break can only reach one of them:
///
/// - `scripts/break-once.sh keylink/src/parking.rs` on `park`'s
///   `if let Some(mut lot) = slot.take() {` → `if let Some(mut lot) =
///   Option::<Vec<Parked>>::None {`, which is the old behaviour exactly: the send
///   escapes to the immediate branch. Watched: `a refused turn's receipt reached
///   its caller: Ok(60)`.
/// - make `Persist::after_turn` stop storing — the first assertion goes first,
///   because a store that never runs cannot fail and `approve` is not refused.
///   That is what pins the second half to a real write.
#[tokio::test]
async fn a_deferred_reply_whose_turn_was_not_stored_is_refused() {
    let store = Flaky::new();
    let id = StateId::new("vaults/sp-01").expect("valid");
    let opened = Persist::<Vault, _>::open(&store, &id)
        .await
        .expect("claims and loads");
    assert!(!opened.was_stored(), "the first run has nothing stored");
    let (state, saver) = opened.start_from(|| Vault {
        balance: 100,
        waiting: Vec::new(),
    });

    let vault = teller::actor_with(state, vec![Box::new(saver)]);

    // Parked: `withdraw` mutates no balance, so the turn it stores is still 100.
    let receipt = {
        let vault = vault.clone();
        tokio::spawn(async move { vault.withdraw(40).await })
    };
    waiting_for(&vault, 1).await;

    store.fail_the_next_store();
    assert!(
        vault.approve().await.is_err(),
        "the approving turn's store failed, so that turn is refused"
    );

    let answer = receipt.await.expect("the task joins");
    assert!(
        answer.is_err(),
        "a refused turn's receipt reached its caller: {answer:?}"
    );

    match vault.ended().await {
        Ended::Observer { name, .. } => assert_eq!(name, "persist"),
        other => panic!("got {other:?}"),
    }

    assert_eq!(
        store.on_disk::<Vault>().expect("stored").balance,
        100,
        "the debit is not stored, and now nobody holds a receipt for it"
    );
}

/// The other half, and the one that makes the pair a biconditional: a `#[defer]`
/// reply that **is** answered is a stored one.
///
/// The refusal test above would still pass with `Deferred::send` deleted
/// outright, which is the failure mode a withholding fix invites — everything is
/// safe if nothing is ever answered. This one is the guard on the release: the
/// receipt says 60, and the balance on the disk says 60 when the caller reads it,
/// so the caller's answer and the `fsync` are the same event in the right order.
///
/// The disk is read **after** the receipt has been awaited, which is what makes
/// it an ordering assertion rather than a coincidence: the store runs in
/// `after_turn`, the release runs after the chain, so a receipt in hand implies a
/// completed store. Under the old behaviour that implication ran the other way
/// and this read could see 100.
///
/// Made red by `scripts/break-once.sh keylink/src/local.rs` on `Deferred::send`'s
/// `Verdict::Committed => reply.answer(Answered::Ran(value)),` →
/// `Verdict::Committed => drop(value),`. Watched: `the receipt reaches its
/// caller when the turn commits: MailboxClosed`.
#[tokio::test]
async fn a_deferred_reply_that_is_answered_is_a_stored_reply() {
    let store = Flaky::new();
    let id = StateId::new("vaults/sp-02").expect("valid");
    let (state, saver) = Persist::<Vault, _>::open(&store, &id)
        .await
        .expect("claims and loads")
        .start_from(|| Vault {
            balance: 100,
            waiting: Vec::new(),
        });

    let vault = teller::actor_with(state, vec![Box::new(saver)]);

    let receipt = {
        let vault = vault.clone();
        tokio::spawn(async move { vault.withdraw(40).await })
    };
    waiting_for(&vault, 1).await;

    assert_eq!(vault.approve().await.expect("running"), 1);
    assert_eq!(
        receipt
            .await
            .expect("the task joins")
            .expect("the receipt reaches its caller when the turn commits"),
        60,
    );
    assert_eq!(
        store.on_disk::<Vault>().expect("stored").balance,
        60,
        "the caller holds a receipt, so the debit is on the disk"
    );

    vault.stop().await;
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
        let (state, saver) = Persist::<Counter, _>::open(&files, &id)
            .await
            .expect("claims and loads")
            .start_from(Counter::default);
        let counter = counting::actor_with(state, vec![Box::new(saver)]);
        assert_eq!(counter.hit().await.expect("running"), 1);
        assert_eq!(counter.hit().await.expect("running"), 2);
        counter.stop().await;
    }

    let opened = Persist::<Counter, _>::open(&files, &id)
        .await
        .expect("claims and loads");
    assert!(opened.was_stored(), "the run above stored something");
    let (state, _saver) = opened.start_from(Counter::default);
    assert_eq!(state.hits.get(), 2);
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

    let (state, saver) = Persist::open(&files, &id)
        .await
        .expect("claims and loads")
        .start_from(Counter::default);
    let stored = counting::actor_with(state, vec![Box::new(saver)]);
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

// ───────────────────────────────── the restart the ledger cannot cross
//
// The other half of the pair `src/idempotency.rs` names: a durable state and a
// volatile ledger disagree across a restart, and `Context::call` is what lets the
// state settle it. Everything below drives the real file store, like the rest of
// this file, and a "new process" is a second `Persist::open` on the same one.

keylink::service! {
    service Till;

    /// Adds `amount` once per call, and answers the balance that produced.
    ///
    /// The whole subject: a retry of this that arrives after a restart meets an
    /// empty ledger, is dispatched, and must not add the amount again.
    rpc Debit(amount: u64) -> u64;
    /// The balance now.
    rpc Balance() -> u64;
    /// How many times [`Debit`] actually did the work. This is the assertion —
    /// "it did not run twice" and not "the ledger refused it".
    rpc Runs() -> u64;
}

/// A balance, the calls that produced it, and a count of the work.
///
/// `applied` is the only new thing here, and it is in the caller's state rather
/// than in keylink's — which is the design. The bound it carries is the caller's
/// to argue, and this one is [`APPLIED_CAPACITY`](keylink::APPLIED_CAPACITY)
/// because nothing about a test argues for another number.
#[derive(Default)]
struct Till {
    balance: u64,
    runs: u64,
    applied: Applied<u64>,
}

impl till::Actor for Till {
    /// The three lines the README's second limit now points at.
    async fn debit(&mut self, cx: Caller<'_>, amount: u64) -> u64 {
        let call = cx.peer().and_then(Context::call);
        if let Some(&balance) = self.applied.get(call) {
            return balance;
        }
        self.runs += 1;
        self.balance += amount;
        self.applied.insert(call, self.balance);
        self.balance
    }

    async fn balance(&mut self, _cx: Caller<'_>) -> u64 {
        self.balance
    }

    async fn runs(&mut self, _cx: Caller<'_>) -> u64 {
        self.runs
    }
}

/// Balance, run count, then one `(id, answer)` pair per applied call, oldest
/// first — which is the order [`Applied::iter`](keylink::Applied::iter) reports
/// for exactly this reason.
impl Durable for Till {
    const VERSION: u32 = 1;

    fn snapshot(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.balance.to_le_bytes());
        bytes.extend_from_slice(&self.runs.to_le_bytes());
        for (call, answer) in self.applied.iter() {
            bytes.extend_from_slice(call.as_bytes());
            bytes.extend_from_slice(&answer.to_le_bytes());
        }
        bytes
    }

    fn restore(bytes: &[u8]) -> Result<Self, RestoreError> {
        const HEAD: usize = 16;
        const ENTRY: usize = 24;
        if bytes.len() < HEAD || !(bytes.len() - HEAD).is_multiple_of(ENTRY) {
            return Err(RestoreError::Malformed(format!(
                "a till is 16 bytes and then whole 24-byte entries, and this is {} bytes",
                bytes.len()
            )));
        }
        let mut till = Self {
            balance: u64::from_le_bytes(bytes[..8].try_into().expect("8 bytes")),
            runs: u64::from_le_bytes(bytes[8..HEAD].try_into().expect("8 bytes")),
            applied: Applied::new(),
        };
        for entry in bytes[HEAD..].chunks_exact(ENTRY) {
            let id: [u8; 16] = entry[..16].try_into().expect("16 bytes");
            let answer = u64::from_le_bytes(entry[16..].try_into().expect("8 bytes"));
            till.applied.insert(Some(CallId::from_bytes(id)), answer);
        }
        Ok(till)
    }

    fn migrate(_from: u32, _bytes: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> {
        Ok(None)
    }
}

/// One incarnation of the till: a claim on the store, a mounted actor, and a
/// served ALPN.
///
/// A struct rather than two runs of a loop — which is how
/// `a_keyed_actors_state_survives_its_own_process` does it — because the two runs
/// here are not the same script: one does the work and the other retries it.
struct Serving {
    system: till::System,
    running: Server,
}

impl Serving {
    async fn start(files: &Files, id: &StateId, actor: &ActorId, node: &keylink::Node) -> Self {
        let (state, saver) = Persist::<Till, _>::open(files, id)
            .await
            .expect("claims and loads")
            .start_from(Till::default);
        let system = till::System::builder(Eviction::Never).capacity(4).build();
        system
            .mount(
                actor.clone(),
                till::spawn_wired(state, vec![Box::new(saver)], till::Feeds::new()),
            )
            .expect("mounts");
        let running = Server::builder(node)
            .service(till::keyed_service(&system, Allowlist::anyone()).build())
            .spawn()
            .expect("one service, no duplicate ALPN");
        Self { system, running }
    }

    /// End the process, as far as a claim and a ledger can tell: drain the actor,
    /// which drops the observers and releases the claim, then drop the router.
    async fn stop(self, actor: &ActorId) {
        match self.system.shutdown(actor, Duration::from_secs(5)).await {
            Some(Ended::Stopped) => {}
            other => panic!("the actor was mounted and must drain: got {other:?}"),
        }
        drop(self.running);
    }
}

/// **The criterion: a retried call across a restart is applied once.**
///
/// This is `README.md`'s second limit, rewritten and then guarded. The ledger is
/// memory, so the restarted process answers the retried key
/// [`Fresh`](keylink::KeyState::Fresh) and dispatches it — that cost is asserted
/// here rather than hidden, because option E accepts it. What must not happen is
/// the *work* happening twice, and `runs` is where that is read.
///
/// # Why it rebuilds from the store
///
/// A test that reused the live service would prove the in-memory tombstone path
/// and nothing about durability: `Ledger` already answers a repeated key from
/// memory. So the actor is drained, the claim released and the router dropped —
/// the same teardown `a_keyed_actors_state_survives_its_own_process` calls a new
/// process — and the second incarnation loads from the file.
///
/// # Two negatives, because one of them was missing and the harness said so
///
/// A state that recognised *everything* has to fail this test, and so does a
/// wiring that hashed the wrong thing. Two assertions do that:
///
/// - a **second key from the same peer** must be applied rather than recognised.
/// - a **fresh keypair** using the first key must be applied too: a different peer
///   hashes to a different [`CallId`](keylink::CallId) and is not recognised —
///   silently, which is the precondition stated as a fact rather than a defect.
///
/// The first of those was not here originally, and the break below is how that was
/// found: replacing the key with a constant left every assertion green, because the
/// peer was still folded in and the test only ever used one key per peer. A guard
/// that cannot go red is not a guard, so the assertion was added rather than the
/// break record softened.
///
/// Made red at both ends, because two facts have to hold at once:
///
/// - `scripts/break-once.sh keylink/src/rpc.rs` on
///   `.map(|idem| CallId::of(peer, idem.key())),` →
///   `.map(|idem| { let _ = idem; CallId::of(peer, &IdempotencyKey::new("x").expect("valid")) }),`
///   — every one of a peer's calls then shares one id. Watched:
///   `left: 50 right: 57`, on the second-key assertion.
/// - `scripts/break-once.sh keylink/src/middleware.rs` on `pub fn call`'s body
///   `self.call` → `self.call.and(None)` — nothing is ever recognised. Watched:
///   `the retry is answered with the FIRST run's balance, out of the state`,
///   `left: 100 right: 50`.
#[tokio::test]
async fn a_retried_call_across_a_restart_is_applied_once() {
    let (_scratch, files) = store("across-restart").await;
    let id = StateId::new("tills/sp-01").expect("valid");
    let actor = ActorId::new("tills/sp-01").expect("valid");
    let server = local_node().await;
    let caller = local_node().await;
    let key = IdempotencyKey::new("debit-4471").expect("a valid key");

    // ── the first process: the work happens and is stored, and the record that
    //    would have suppressed a repeat is in memory only.
    {
        let serving = Serving::start(&files, &id, &actor, &server).await;
        let client = till::KeyedClient::connect(&caller, server.endpoint().addr())
            .await
            .expect("dials");
        assert_eq!(
            till::at(&client.with_idempotency(key.clone()), actor.clone())
                .debit(50)
                .await
                .expect("the keyed actor answers"),
            50,
        );
        assert_eq!(
            till::at(&client, actor.clone())
                .runs()
                .await
                .expect("answers"),
            1,
        );
        assert_eq!(
            client.key_state(key.clone()).await.expect("answers"),
            KeyState::Done,
            "the ledger holds the reply — in memory, which is what the restart takes",
        );
        serving.stop(&actor).await;
    }

    // ── the second process: same store, same served node, same calling keypair.
    let serving = Serving::start(&files, &id, &actor, &server).await;
    let client = till::KeyedClient::connect(&caller, server.endpoint().addr())
        .await
        .expect("dials");

    assert_eq!(
        client.key_state(key.clone()).await.expect("answers"),
        KeyState::Fresh,
        "a restarted ledger knows nothing about the key, and E accepts that \
         rather than closing it",
    );
    assert_eq!(
        till::at(&client.with_idempotency(key.clone()), actor.clone())
            .debit(50)
            .await
            .expect("the keyed actor answers"),
        50,
        "the retry is answered with the FIRST run's balance, out of the state",
    );
    assert_eq!(
        till::at(&client, actor.clone())
            .runs()
            .await
            .expect("answers"),
        1,
        "the work must not have run a second time",
    );
    assert_eq!(
        till::at(&client, actor.clone())
            .balance()
            .await
            .expect("answers"),
        50,
    );

    // ── the first negative: a SECOND key from the SAME peer is a different call.
    //    Without this the wiring could hash any fixed key it liked and every
    //    assertion above would still hold — see this test's own break record.
    let another = IdempotencyKey::new("debit-4472").expect("a valid key");
    assert_eq!(
        till::at(&client.with_idempotency(another), actor.clone())
            .debit(7)
            .await
            .expect("the keyed actor answers"),
        57,
        "a different key from the same peer must be applied, not recognised",
    );
    assert_eq!(
        till::at(&client, actor.clone())
            .runs()
            .await
            .expect("answers"),
        2,
    );

    // ── the second negative: a caller on a fresh keypair is a different peer, so
    //    the same key is a different call. The precondition, stated as a fact.
    let stranger = local_node().await;
    let other = till::KeyedClient::connect(&stranger, server.endpoint().addr())
        .await
        .expect("dials");
    assert_eq!(
        till::at(&other.with_idempotency(key.clone()), actor.clone())
            .debit(50)
            .await
            .expect("the keyed actor answers"),
        107,
        "a peer that reconnected on a new keypair is not recognised, and the \
         work runs again — silently, which is the limit and not a defect",
    );
    assert_eq!(
        till::at(&other, actor.clone())
            .runs()
            .await
            .expect("answers"),
        3,
    );

    serving.stop(&actor).await;
}

// ───────────────────────────── whose key a parked reply is answered under

keylink::service! {
    service Booth;

    /// Parked, and answered from a later `approve`.
    #[defer]
    rpc Withdraw(amount: u64) -> u64;
    /// Releases every parked withdrawal.
    rpc Approve() -> u64;
    /// How many are parked. The sync point, asked rather than slept for.
    rpc Waiting() -> u64;
    /// What each recorded turn saw on `cx.call()`, oldest first: the id in hex,
    /// or `-` for a turn whose call carried no key.
    ///
    /// This method does **not** record itself — a probe that appeared in its own
    /// answer would make the sequence a function of how often the test asked.
    rpc Seen() -> Vec<String>;
}

/// A balance, the withdrawals waiting for approval, and what each turn was called.
///
/// Two fields are deliberately not in the snapshot, for two different reasons:
///
/// - `waiting` **cannot** be. A [`Deferred`] is a live channel to a caller in this
///   process, and that is the whole of what a restart takes away.
/// - `seen` is a probe. Restoring it would make the second incarnation's answer a
///   function of the first's, which is the opposite of what the test reads it for.
///
/// `parked` is [`Applied<()>`](keylink::Applied) and not `Applied<u64>`, and that
/// is the `#[defer]` decision made concrete: the turn that parks produces no
/// answer, so there is nothing to record and nothing to replay. At-most-once is
/// all this shape can have. `Till` above is the other shape.
///
/// `approve` deduplicates nothing on purpose. This state exists for one question
/// and a second `Applied` in it would answer none of it better.
#[derive(Default)]
struct Booth {
    balance: u64,
    waiting: Vec<(u64, Deferred<u64>)>,
    parked: Applied<()>,
    seen: Vec<String>,
}

/// What a turn saw, in the form `Seen` reports.
fn mark(cx: Caller<'_>) -> String {
    cx.peer()
        .and_then(Context::call)
        .map_or_else(|| "-".to_string(), |call| call.to_string())
}

impl booth::Actor for Booth {
    async fn withdraw(&mut self, cx: Caller<'_>, reply: Reply<u64>, amount: u64) -> Replied<u64> {
        let call = cx.peer().and_then(Context::call);
        self.seen.push(mark(cx));
        if self.parked.seen(call) {
            // Recognised, and there is no recorded answer to give — so the domain
            // answers out of itself, which is all `Applied<()>` promises.
            return reply.send(self.balance);
        }
        self.parked.insert(call, ());
        let (deferred, replied) = reply.later();
        self.waiting.push((amount, deferred));
        replied
    }

    async fn approve(&mut self, cx: Caller<'_>) -> u64 {
        self.seen.push(mark(cx));
        let approved = self.waiting.len() as u64;
        for (amount, deferred) in std::mem::take(&mut self.waiting) {
            self.balance -= amount;
            deferred.send(self.balance);
        }
        approved
    }

    async fn waiting(&mut self, _cx: Caller<'_>) -> u64 {
        self.waiting.len() as u64
    }

    async fn seen(&mut self, _cx: Caller<'_>) -> Vec<String> {
        self.seen.clone()
    }
}

/// Balance, then one id per parked call. Neither `waiting` nor `seen` is here —
/// see the type.
impl Durable for Booth {
    const VERSION: u32 = 1;

    fn snapshot(&self) -> Vec<u8> {
        let mut bytes = self.balance.to_le_bytes().to_vec();
        for (call, ()) in self.parked.iter() {
            bytes.extend_from_slice(call.as_bytes());
        }
        bytes
    }

    fn restore(bytes: &[u8]) -> Result<Self, RestoreError> {
        if bytes.len() < 8 || !(bytes.len() - 8).is_multiple_of(16) {
            return Err(RestoreError::Malformed(format!(
                "a booth is 8 bytes and then whole 16-byte ids, and this is {} bytes",
                bytes.len()
            )));
        }
        let mut booth = Self {
            balance: u64::from_le_bytes(bytes[..8].try_into().expect("8 bytes")),
            ..Self::default()
        };
        for id in bytes[8..].chunks_exact(16) {
            let id: [u8; 16] = id.try_into().expect("16 bytes");
            booth.parked.insert(Some(CallId::from_bytes(id)), ());
        }
        Ok(booth)
    }

    fn migrate(_from: u32, _bytes: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> {
        Ok(None)
    }
}

/// Wait for `want` withdrawals to be parked by asking over the wire, not by
/// sleeping. `waiting_for` above, with a client instead of a `Handle`.
async fn parked_over_the_wire(client: &booth::KeyedClient, actor: &ActorId, want: u64) {
    for _ in 0..500 {
        if booth::at(client, actor.clone())
            .waiting()
            .await
            .expect("the actor is running")
            == want
        {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("{want} withdrawals never parked");
}

/// One incarnation of the booth. [`Serving`]'s twin; the two differ only in the
/// service, and a generic over `service!`'s generated module is not a thing Rust
/// can express.
struct Booking {
    system: booth::System,
    running: Server,
}

impl Booking {
    async fn start(files: &Files, id: &StateId, actor: &ActorId, node: &keylink::Node) -> Self {
        let (state, saver) = Persist::<Booth, _>::open(files, id)
            .await
            .expect("claims and loads")
            .start_from(|| Booth {
                balance: 100,
                ..Booth::default()
            });
        let system = booth::System::builder(Eviction::Never).capacity(4).build();
        system
            .mount(
                actor.clone(),
                booth::spawn_wired(state, vec![Box::new(saver)], booth::Feeds::new()),
            )
            .expect("mounts");
        let running = Server::builder(node)
            .service(booth::keyed_service(&system, Allowlist::anyone()).build())
            .spawn()
            .expect("one service, no duplicate ALPN");
        Self { system, running }
    }

    async fn stop(self, actor: &ActorId) {
        match self.system.shutdown(actor, Duration::from_secs(5)).await {
            Some(Ended::Stopped) => {}
            other => panic!("the actor was mounted and must drain: got {other:?}"),
        }
        drop(self.running);
    }
}

/// **The criterion: a parked reply is named by the turn that RELEASES it, and
/// that is a different call.**
///
/// `parking.rs` states the shape — *"a `Deferred` is parked in turn 1 and sent in
/// turn 5"* — and leaves open whose key turn 5 is running under. This decides it:
/// turn 5 is a call of its own, with its own caller and its own key, and
/// `Context::call` answers **turn 5's**. Nothing leaks backwards from the parked
/// reply into the turn that happens to release it.
///
/// The script, in order, and every assertion reads the one sequence:
///
/// 1. a **keyed** `withdraw` parks. Its turn reports its own id.
/// 2. an **unkeyed** `approve` releases it. Its turn reports `-` — even though the
///    reply it released belongs to a keyed call. That is the negative and it is
///    the sharp half.
/// 3. a **keyed** `withdraw`, then a **keyed** `approve`. The approving turn
///    reports ITS key and not the parked withdrawal's. That is the positive.
/// 4. a restart, and the retry of the second withdrawal's key. The ledger is
///    empty so the handler is entered; the `Applied<()>` the parking turn wrote
///    came back with the state, so nothing is parked twice.
/// 5. the same key from a fresh keypair parks, because it is a different peer.
///
/// # What the restart does to the ANSWER, which is not asserted here
///
/// A parked reply channel is not `Durable` and cannot be — `Vault` above says so
/// too — so a restart drops it and the original caller is answered by nobody. That
/// is why the record a parking turn writes is `Applied<()>`: at-most-once on the
/// work, never the replayed answer an ordinary `rpc` gets, because the parking turn
/// produced no answer to record.
///
/// It is stated rather than asserted, and here is the observation behind that
/// choice, because it is not a small one. **A keyed actor with a remote `#[defer]`
/// reply still parked in its state does not drain.** Skipping only the approval in
/// step 5 — `parked_over_the_wire(&other, &actor, 1).await;` followed immediately
/// by `booking.stop(&actor).await; return;` — produced, watched:
/// `the actor was mounted and must drain: got Some(Vanished)`, after the whole
/// 5-second budget. So the shutdown path with a parked remote reply is its own
/// subject with its own owner, and this test approves before it stops rather than
/// smuggling that in.
///
/// Every break literal here is quoted without its leading indentation. Pass it to
/// `scripts/break-once.sh` **with** the indentation — this doc comment is
/// otherwise a second occurrence of the same text and the harness aborts on a
/// match that is not unique, which is what it is for.
///
/// Made red twice, both on `keylink/src/middleware.rs`'s `pub fn call` body:
///
/// - `self.call` → `self.call.and(None)`, so no turn is named at all. Watched:
///   `the first withdrawal's turn carried its own key`, `left: "-" right: "-"`.
/// - `self.call` → `self.call.filter(|_| self.method() != "Approve")`, so the
///   releasing turn loses its own name. Watched:
///   `and the keyed approval carried its own`, `left: "-" right: "-"`.
///
/// # The leak itself has no break, and that is the argument
///
/// What this test refuses is the *other* direction — turn 5 reporting the parked
/// `withdraw`'s id — and there is no one-literal edit that produces it, because
/// there is no plumbing that could carry it. A [`Deferred`] holds a reply channel
/// and nothing else; the `Context` a turn sees arrives with the mail for **that**
/// message. So the decision is not merely tested, it is unrepresentable, and the
/// two breaks above are what pin the assertions that would notice if that stopped
/// being true.
#[tokio::test]
async fn a_deferred_reply_is_named_by_the_turn_that_releases_it() {
    let (_scratch, files) = store("deferred-call-id").await;
    let id = StateId::new("booths/sp-01").expect("valid");
    let actor = ActorId::new("booths/sp-01").expect("valid");
    let server = local_node().await;
    let caller = local_node().await;
    let first_key = IdempotencyKey::new("withdraw-1").expect("a valid key");
    let second_key = IdempotencyKey::new("withdraw-2").expect("a valid key");
    let approve_key = IdempotencyKey::new("approve-2").expect("a valid key");

    let booking = Booking::start(&files, &id, &actor, &server).await;
    let client = booth::KeyedClient::connect(&caller, server.endpoint().addr())
        .await
        .expect("dials");

    // ── 1 and 2: a keyed withdrawal, released by an UNKEYED approval.
    let pending = {
        let keyed = client.with_idempotency(first_key.clone());
        let actor = actor.clone();
        tokio::spawn(async move { booth::at(&keyed, actor).withdraw(40).await })
    };
    parked_over_the_wire(&client, &actor, 1).await;
    assert_eq!(
        booth::at(&client, actor.clone())
            .approve()
            .await
            .expect("answers"),
        1,
    );
    assert_eq!(
        pending.await.expect("no panic").expect("answers"),
        60,
        "the parked caller is still answered from the later turn",
    );

    // ── 3: a keyed withdrawal, released by a KEYED approval.
    let pending = {
        let keyed = client.with_idempotency(second_key.clone());
        let actor = actor.clone();
        tokio::spawn(async move { booth::at(&keyed, actor).withdraw(40).await })
    };
    parked_over_the_wire(&client, &actor, 1).await;
    assert_eq!(
        booth::at(&client.with_idempotency(approve_key.clone()), actor.clone())
            .approve()
            .await
            .expect("answers"),
        1,
    );
    assert_eq!(pending.await.expect("no panic").expect("answers"), 20);

    let seen = booth::at(&client, actor.clone())
        .seen()
        .await
        .expect("answers");
    assert_eq!(seen.len(), 4, "four recorded turns, in order: {seen:?}");
    assert_ne!(
        seen[0], "-",
        "the first withdrawal's turn carried its own key"
    );
    assert_eq!(
        seen[1], "-",
        "the releasing turn carried NO key, and did not inherit the parked \
         call's: {seen:?}",
    );
    assert_ne!(
        seen[2], "-",
        "the second withdrawal's turn carried its own key"
    );
    assert_ne!(seen[3], "-", "and the keyed approval carried its own");
    assert_ne!(
        seen[2], seen[3],
        "the releasing turn is named by its OWN call, not by the one it released",
    );
    assert_ne!(seen[0], seen[2], "two keys, two calls");

    booking.stop(&actor).await;

    // ── 4: the restart. The channel is gone; the record the parking turn wrote
    //      is not.
    let booking = Booking::start(&files, &id, &actor, &server).await;
    let client = booth::KeyedClient::connect(&caller, server.endpoint().addr())
        .await
        .expect("dials");
    assert_eq!(
        client.key_state(second_key.clone()).await.expect("answers"),
        KeyState::Fresh,
        "the ledger is memory here too",
    );
    assert_eq!(
        booth::at(&client.with_idempotency(second_key.clone()), actor.clone())
            .withdraw(40)
            .await
            .expect("answers"),
        20,
        "the retry is recognised out of the restored state and answered at once",
    );
    assert_eq!(
        booth::at(&client, actor.clone())
            .waiting()
            .await
            .expect("answers"),
        0,
        "and nothing was parked a second time",
    );

    // ── 5: a fresh keypair is a different peer, so the same key is a different
    //    call and the withdrawal parks for real — a second piece of work, not a
    //    recognised retry.
    let stranger = local_node().await;
    let other = booth::KeyedClient::connect(&stranger, server.endpoint().addr())
        .await
        .expect("dials");
    let strangers = {
        let keyed = other.with_idempotency(second_key.clone());
        let actor = actor.clone();
        tokio::spawn(async move { booth::at(&keyed, actor).withdraw(5).await })
    };
    parked_over_the_wire(&other, &actor, 1).await;
    assert_eq!(
        booth::at(&other, actor.clone())
            .approve()
            .await
            .expect("answers"),
        1,
    );
    assert_eq!(
        strangers.await.expect("no panic").expect("answers"),
        15,
        "the stranger's withdrawal was applied, because it was never recognised",
    );

    booking.stop(&actor).await;
}
