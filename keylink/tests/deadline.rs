//! A deadline answers the caller and never cancels the work.
//!
//! `Timeout` was a layer, so on expiry it dropped the wait for the reply and left
//! the turn running. A keyed call then settled `Interrupted` and every retry was
//! refused for good, though the work had run. The deadline now lives in
//! `serve_one`, outside every layer, where the guard can keep waiting for what
//! really happened. Three claims, one test each:
//!
//! | the claim | the test |
//! |---|---|
//! | the caller is answered at the deadline and the turn still finishes, whole | `a_deadline_answers_the_caller_and_the_turn_still_finishes` |
//! | a keyed retry after a deadline gets the first answer, and the work ran once | `a_retry_after_a_deadline_gets_the_first_answer_and_runs_once` |
//! | the caller's own deadline is a typed local error | `a_caller_deadline_is_typed_locally` |
//!
//! No assertion reads a clock. A fallback task releases the gate after ten
//! seconds, so a missing deadline fails the test instead of hanging it; the
//! deadline itself is long enough that an actor that starts its turn late still
//! starts it, because a deadline also sheds a message that has not started.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use keylink::prelude::*;
use keylink::{
    Allowlist, Caller, Error, Idempotency, IdempotencyKey, KeyState, Node, Observer, RemoteError,
    Server, TurnFut, TurnInfo, TurnOutcome,
};
use tokio::sync::Notify;

mod common;
use common::local_node;

keylink::service! {
    service Vault;

    /// Awaits the gate in the state, so the turn is genuinely running.
    rpc Hold() -> u64;

    /// How many `Hold` turns have started.
    rpc Runs() -> u64;
}

/// Long enough that a loaded machine still starts the turn inside it, and short
/// enough to wait for.
const DEADLINE: Duration = Duration::from_millis(250);

struct Vault {
    gate: Arc<Notify>,
    runs: u64,
}

impl vault::Actor for Vault {
    async fn hold(&mut self, _cx: Caller<'_>) -> u64 {
        self.runs += 1;
        let run = self.runs;
        self.gate.notified().await;
        run
    }

    async fn runs(&mut self, _cx: Caller<'_>) -> u64 {
        self.runs
    }
}

/// Counts the `Hold` turns that completed, whole.
struct Counting(Arc<AtomicUsize>);

impl Observer<Vault> for Counting {
    fn name(&self) -> &'static str {
        "counting"
    }

    fn after_turn<'a>(&'a mut self, _state: &'a Vault, turn: TurnInfo<'a>) -> TurnFut<'a> {
        if turn.method == "Hold" && matches!(turn.outcome, TurnOutcome::Completed) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        Box::pin(async { Ok(()) })
    }
}

/// A served `Vault` and a client dialled to it.
struct Fixture {
    /// The local handle, which no service deadline reaches.
    vault: vault::Handle<Vault>,
    client: vault::Client,
    gate: Arc<Notify>,
    completed: Arc<AtomicUsize>,
    _server: Server,
    _nodes: (Node, Node),
}

impl Fixture {
    async fn new(deadline: Option<Duration>) -> Self {
        let gate = Arc::new(Notify::new());
        let completed = Arc::new(AtomicUsize::new(0));
        let vault = vault::setup(Vault {
            gate: Arc::clone(&gate),
            runs: 0,
        })
        .observe(Counting(Arc::clone(&completed)))
        .actor();

        let server_node = local_node().await;
        let client_node = local_node().await;
        let mut service = vault.service(Allowlist::anyone());
        if let Some(deadline) = deadline {
            service = service.deadline(deadline);
        }
        let server = Server::builder(&server_node)
            .service(service.build())
            .spawn()
            .expect("serves");
        let client = vault::Client::connect(&client_node, server_node.endpoint().addr())
            .await
            .expect("dials");
        Self {
            vault,
            client,
            gate,
            completed,
            _server: server,
            _nodes: (server_node, client_node),
        }
    }

    /// Releases the gate after ten seconds whatever happens, so a deadline that
    /// never fires turns into a failed assertion and not a hang.
    fn release_in_ten_seconds(&self) -> tokio::task::JoinHandle<()> {
        let gate = Arc::clone(&self.gate);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            gate.notify_one();
        })
    }
}

fn millis(budget: Duration) -> u64 {
    u64::try_from(budget.as_millis()).expect("a test-sized budget")
}

/// The caller is answered at the deadline, and the turn runs on to the end.
///
/// `runs()` goes through the local handle, so no service deadline reaches it, and
/// it queues behind the held turn: its answer is also the proof that the turn
/// finished. The observer counts completed `Hold` turns and sees exactly one, so
/// the deadline produced one whole turn and not a half one.
///
/// Made red by `scripts/break-once.sh keylink/src/rpc.rs` on `serve_one`'s
/// `Some((budget, _)) => match n0_future::time::timeout(budget, &mut chain).await {`
/// (indented eight spaces) and the replacement `Some((budget, _)) => match
/// Ok::<_, ()>((&mut chain).await) {`, which removes the race: nobody answers at
/// the deadline. The ten-second fallback releases the gate, the call returns, and
/// the first assertion fails with `the caller is told the deadline passed: got
/// Ok(1)`.
#[tokio::test]
async fn a_deadline_answers_the_caller_and_the_turn_still_finishes() {
    let fixture = Fixture::new(Some(DEADLINE)).await;
    let fallback = fixture.release_in_ten_seconds();

    let held = fixture.client.hold().await;
    assert!(
        matches!(
            held,
            Err(Error::Remote(RemoteError::DeadlinePassed { millis: passed }))
                if passed == millis(DEADLINE)
        ),
        "the caller is told the deadline passed: got {held:?}"
    );

    fixture.gate.notify_one();
    assert_eq!(fixture.vault.runs().await.expect("running"), 1);
    assert_eq!(
        fixture.completed.load(Ordering::SeqCst),
        1,
        "one whole Hold turn, never a half one"
    );
    fallback.abort();
}

/// A retry with the same key after a deadline gets the first call's real answer,
/// and the work ran once.
///
/// Made red by `scripts/break-once.sh keylink/src/rpc.rs` on `serve_one`'s late
/// path, the line `let _ = settle_key(Some(guard), &progress, &reply, grant).await;`
/// (indented twenty spaces) and the replacement `drop((guard, reply, grant));`,
/// which lets the guard settle on its own instead of from what the
/// chain really returned. The retry is then refused and the test fails with `the
/// retry gets the first call's answer: Remote(Idempotency(Interrupted))`.
#[tokio::test]
async fn a_retry_after_a_deadline_gets_the_first_answer_and_runs_once() {
    let fixture = Fixture::new(Some(DEADLINE)).await;
    let fallback = fixture.release_in_ten_seconds();
    let key = IdempotencyKey::new("hold-1").expect("a valid key");
    let keyed = || {
        fixture.client.with_idempotency(Idempotency::attaching_for(
            key.clone(),
            Duration::from_secs(30),
        ))
    };

    let first = keyed().hold().await;
    assert!(
        matches!(
            first,
            Err(Error::Remote(RemoteError::DeadlinePassed { .. }))
        ),
        "the first call is told the deadline passed: got {first:?}"
    );

    let retry = {
        let retrying = keyed();
        tokio::spawn(async move { retrying.hold().await })
    };
    fixture.gate.notify_one();
    assert_eq!(
        retry
            .await
            .expect("the retry does not panic")
            .expect("the retry gets the first call's answer"),
        1
    );

    assert_eq!(
        keyed().hold().await.expect("a settled key replays"),
        1,
        "a third call is a replay of the same answer"
    );
    assert_eq!(fixture.vault.runs().await.expect("running"), 1);
    fallback.abort();
}

/// The caller's own deadline ends its wait with a typed local error.
///
/// The call attaches to a key whose first call is held. The serving side is then
/// waiting on the ledger, which a deadline does not reach, so nothing answers the
/// caller and its own timer is the only thing that can: the local error is
/// certain and not a race with the remote one.
///
/// Made red by `scripts/break-once.sh keylink/src/rpc.rs` on `RpcClient::call`'s
/// `Some(budget) => n0_future::time::timeout(budget, self.exchange(request, body))`
/// (indented twelve spaces) and the replacement `Some(budget) =>
/// std::future::ready(Ok::<_, ()>(self.exchange(request, body).await))`, which
/// removes the caller's timer. The ten-second fallback releases the gate and the
/// test fails with `the caller's own deadline is a local error: got Ok(1)`.
#[tokio::test]
async fn a_caller_deadline_is_typed_locally() {
    let fixture = Fixture::new(None).await;
    let fallback = fixture.release_in_ten_seconds();
    let key = IdempotencyKey::new("hold-2").expect("a valid key");
    let holding = {
        let client = fixture.client.with_idempotency(key.clone());
        tokio::spawn(async move { client.hold().await })
    };
    for _ in 0..500 {
        let state = fixture.client.key_state(key.clone()).await.expect("asks");
        if state == KeyState::Running {
            break;
        }
        tokio::task::yield_now().await;
    }

    let attached = fixture
        .client
        .with_idempotency(Idempotency::attaching_for(
            key.clone(),
            Duration::from_secs(30),
        ))
        .with_deadline(DEADLINE)
        .hold()
        .await;
    assert!(
        matches!(
            attached,
            Err(Error::DeadlinePassed { millis: passed }) if passed == millis(DEADLINE)
        ),
        "the caller's own deadline is a local error: got {attached:?}"
    );

    fixture.gate.notify_one();
    assert_eq!(holding.await.expect("no panic").expect("answers"), 1);
    fallback.abort();
}
