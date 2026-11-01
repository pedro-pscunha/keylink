//! A payment that needs a second signature, and a retry that must not pay twice.
//!
//! ```sh
//! cargo run -p keylink --example payment_approval
//! ```
//!
//! Three nodes, one real QUIC connection each, no network: `Node::bind_local`
//! binds with no relay and no discovery. A real program uses [`Node::bind`].
//!
//! # The problem
//!
//! A finance desk asks the treasury to pay a supplier. The treasury pays only
//! after a second person approves. The desk's connection can drop while it
//! waits, so the desk asks again. The treasury must pay once.
//!
//! Three parts answer that, and each is a separate mechanism.
//!
//! 1. [`Allowlist::only`] names the public keys that may call at all.
//! 2. `#[defer]` parks the reply, so one call can outlive the turn that
//!    received it. The approver answers it from a later turn.
//! 3. An [`IdempotencyKey`] makes the repeat a replay rather than a payment.
//!
//! # What this example is not
//!
//! It does not reconnect. `reliable_call.rs` owns that, through [`Peer`], and
//! repeating it here would hide the three parts above. The retry is written out
//! as a second call, which is what a reconnect would issue anyway.
//!
//! # Two memories, and both of them outlive the process
//!
//! A treasury that forgets what it paid is the wrong kind of treasury, so this
//! one is durable twice over, through two different ports of the same crate.
//!
//! - **The state.** [`Persist`] stores the treasury inside every turn, over
//!   [`Snapshots`](keylink_persist::Snapshots) — one whole value, replaced
//!   atomically. An answered call is a stored call.
//! - **The idempotency record.** [`Records::Durable`] writes one entry per
//!   claimed key, over [`Entries`](keylink_persist::Entries) — many small
//!   values, each written on its own. A settled key stays settled.
//!
//! **This example asserted the opposite until the second port existed**, and
//! the sentence it carried is worth keeping as the reason the port does: the
//! replay record lived in the served service, never reached a disk, and died
//! with the process — so the key that read `Done` read `Fresh` after a restart
//! and the same call paid a second time. Act 11 is that measurement, inverted.
//!
//! # What durability costs here, stated rather than discovered
//!
//! Two `fsync`s per keyed call: one before the handler is entered, and one
//! before its reply is written. Neither is free and neither is optional — the
//! second is what makes the record a fact the caller can rely on, and the first
//! is what makes an interrupted call recoverable as *unknown* rather than as
//! never having happened. [`Records::Memory`] is still the default, and a
//! service that does not need this should not pay it.

use std::error::Error as StdError;
use std::time::Duration;

use keylink::prelude::*;
use keylink::{
    Allowlist, Caller, Deferred, DurableRecords, Error, Idempotency, IdempotencyKey,
    IdempotencyRefusal, KeyState, Node, Persist, RecordError, Records, Refusal, RemoteError,
    Replied, Reply, Server, Whole,
};
use keylink_persist::file::Files;
use keylink_persist::{Durable, RestoreError, StateId};
use postcard_schema::Schema;
use serde::{Deserialize, Serialize};

/// A payment the treasury actually made.
///
/// `run` counts executions rather than requests, so a repeated `run` is itself
/// the evidence that the treasury did not pay again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub struct Settlement {
    /// Which execution produced this settlement.
    pub run: u32,
    /// Who was paid.
    pub payee: String,
    /// How much, in cents.
    pub cents: u64,
}

keylink::service! {
    service Treasury;

    /// Ask for a payment. Parked until somebody approves it.
    ///
    /// `#[defer]` changes nothing on the wire. The caller makes one ordinary
    /// call and blocks on one stream. What changes is the handler: it receives
    /// a `Reply` and may answer from a later turn.
    #[defer]
    rpc Instruct(payee: String, cents: u64) -> Settlement;

    /// The second signature. Releases every parked request and pays each one.
    rpc Approve() -> u32;

    /// Every payment the treasury has made, so a caller can count executions.
    rpc Paid() -> Vec<Settlement>;

    /// How many requests are parked, so the example can wait by asking.
    rpc Pending() -> u32;
}

/// The treasury. It pays inside `approve` and nowhere else.
#[derive(Default)]
struct Treasury {
    paid: Vec<Settlement>,
    waiting: Vec<(String, u64, Deferred<Settlement>)>,
    runs: u32,
}

impl treasury::Actor for Treasury {
    async fn instruct(
        &mut self,
        _cx: Caller<'_>,
        reply: Reply<Settlement>,
        payee: String,
        cents: u64,
    ) -> Replied<Settlement> {
        // `later` splits the reply in two: a `Deferred` this actor stores, and
        // a `Replied` it must return. Losing the `Deferred` is what
        // `Withheld::Dropped` reports, so the compiler asks for both halves.
        let (deferred, replied) = reply.later();
        self.waiting.push((payee, cents, deferred));
        replied
    }

    async fn approve(&mut self, _cx: Caller<'_>) -> u32 {
        let mut released = 0;
        for (payee, cents, deferred) in std::mem::take(&mut self.waiting) {
            self.runs += 1;
            let settlement = Settlement {
                run: self.runs,
                payee,
                cents,
            };
            self.paid.push(settlement.clone());
            deferred.send(settlement);
            released += 1;
        }
        released
    }

    async fn paid(&mut self, _cx: Caller<'_>) -> Vec<Settlement> {
        self.paid.clone()
    }

    async fn pending(&mut self, _cx: Caller<'_>) -> u32 {
        self.waiting.len() as u32
    }
}

impl Durable for Treasury {
    /// Bump this whenever `snapshot` starts writing a different shape. Nothing
    /// can check that you remembered.
    const VERSION: u32 = 1;

    /// `waiting` is deliberately absent, and its absence is the honest part.
    ///
    /// A `Deferred` is a live reply channel to a caller that is blocked right
    /// now. It cannot be written to a disk, and a restored one would answer
    /// nobody. So a request that is parked when the process dies is simply
    /// gone, and its caller must ask again.
    fn snapshot(&self) -> Vec<u8> {
        // `snapshot` returns `Vec<u8>` and cannot report a failure, so every
        // serializer-based implementation needs an answer for the impossible
        // case. Empty bytes are that answer here, and `restore` below rejects
        // them loudly rather than starting from nothing.
        postcard::to_stdvec(&(self.runs, &self.paid)).unwrap_or_default()
    }

    fn restore(bytes: &[u8]) -> Result<Self, RestoreError> {
        let (runs, paid): (u32, Vec<Settlement>) = postcard::from_bytes(bytes)
            .map_err(|error| RestoreError::Malformed(error.to_string()))?;
        Ok(Self {
            paid,
            waiting: Vec::new(),
            runs,
        })
    }

    /// Version 1 is the first, so there is no older version to come from.
    fn migrate(_from: u32, _bytes: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> {
        Ok(None)
    }
}

/// Wait for `want` requests to be parked, by asking rather than by sleeping.
///
/// A sleep guesses how long a QUIC round trip plus a turn takes, and a guess is
/// what goes red on a loaded machine. This asks the actor, so it returns as soon
/// as the fact is true. The bound turns a hang into a message.
async fn parked(desk: &treasury::Handle<Treasury>, want: u32) -> Result<(), Box<dyn StdError>> {
    for _ in 0..400 {
        if desk.pending().await? == want {
            return Ok(());
        }
        n0_future::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("{want} requests never got parked").into())
}

/// Name the refusal, because "it failed" is not what a caller acts on.
fn why(error: &Error) -> &'static str {
    let Error::Remote(RemoteError::Idempotency(refusal)) = error else {
        return "not the ledger: read the error itself";
    };
    match refusal {
        IdempotencyRefusal::InFlight => "the first attempt is still parked: attach to it, or wait",
        IdempotencyRefusal::Interrupted => {
            "it may or may not have paid: reconcile, then use a NEW key"
        }
        IdempotencyRefusal::Forgotten => {
            "it paid and the reply expired: read the ledger, do not re-ask"
        }
        _ => "ask `key_state` before deciding anything",
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    // ── 1. The store, and the state that was already in it.
    //
    //  A fresh directory each run. The gate runs this example every time, and a
    //  treasury that carried the previous run's payments in would make every
    //  count below depend on history rather than on this run.
    let root = std::env::temp_dir().join("keylink-payment-approval");
    let _ = std::fs::remove_dir_all(&root);
    let store = Files::open(&root).await?.limit(1024 * 1024);
    let id = StateId::new("treasury/main")?;

    //  The second store, under the same root and a different name. One type,
    //  two ports: `Files` serves `Snapshots::claim` above and `Entries::hold`
    //  here. This one holds one entry per idempotency key rather than one whole
    //  value, which is the whole reason it is a different port — recording one
    //  reply through `Snapshots` would rewrite every reply already held.
    let keys = Files::open(&root).await?;
    let ledger_id = StateId::new("treasury/keys")?;

    //  `open` claims the state exclusively and reads it back in one call, and
    //  `start_from` is the only way to reach the observer. There is no spelling
    //  of these two lines that starts the actor empty while a full state sits
    //  on the disk.
    let (state, saver) = Persist::<Treasury, _, Whole>::open(&store, &id)
        .await?
        .start_from(Treasury::default);

    // ── 2. Three nodes. The treasury serves. One desk is on the list and one
    //       is not.
    let treasury_node = Node::bind_local().await?;
    let finance_node = Node::bind_local().await?;
    let stranger_node = Node::bind_local().await?;

    //  `observe(saver)` puts the store inside the turn. The handler runs, the
    //  observer stores, and only then does the caller hear anything — so an
    //  answered payment is a stored payment, and a panicked turn is refused
    //  rather than written.
    let desk = treasury::setup(state).observe(saver).actor();
    //  `Server::builder` rather than `desk.serve`, because the shorthand takes a
    //  policy and nothing else, and this service has a second thing to say:
    //  where its idempotency records live.
    let server = Server::builder(&treasury_node)
        .service(
            desk.service(Allowlist::only([finance_node.id()]))
                .records(Records::Durable(
                    DurableRecords::open(&keys, &ledger_id).await?,
                ))
                .build(),
        )
        .spawn()?;

    // ── 2. Authentication, and the part a reader guesses wrong.
    //
    //  The dial SUCCEEDS. Admission runs one layer above the handshake, so an
    //  unlisted peer connects and is refused on its first call. A program that
    //  treats a live connection as proof of access is wrong here.
    let stranger =
        treasury::Client::connect(&stranger_node, treasury_node.endpoint().addr()).await?;
    match stranger.paid().await {
        Ok(_) => return Err("an unlisted peer must not read the ledger".into()),
        Err(refused) => {
            //  `transport: connection lost` is what a dead network reads like
            //  too, and a caller has to act differently on the two. `refusal`
            //  answers which: `Denied` means ask for access, and `Unavailable`
            //  means back off and try later.
            println!("the unlisted node connected, then was refused: {refused}");
            assert_eq!(
                refused.refusal(),
                Some(Refusal::Denied),
                "the refusal must be the policy's, not a lost link"
            );
        }
    }

    let finance = treasury::Client::connect(&finance_node, treasury_node.endpoint().addr()).await?;

    // ── 3. One key for one invoice. It belongs to the payment, not to the
    //       attempt, so every retry of this invoice carries this same key.
    let invoice = IdempotencyKey::new("invoice-8812")?;
    println!(
        "before the request, the ledger says {:?}",
        finance.key_state(invoice.clone()).await?
    );

    // ── 4. The request parks. The caller is blocked on one ordinary call, so
    //       it runs on its own task while this one plays the approver.
    let asking = {
        let finance = finance.clone();
        let invoice = invoice.clone();
        tokio::spawn(async move {
            finance
                .with_idempotency(invoice)
                .instruct("ACME Bolts".to_string(), 12_500)
                .await
        })
    };
    parked(&desk, 1).await?;

    // ── 5. A retry that arrives while the first attempt is still parked.
    //
    //  This is the case a recipe usually skips. The work has not finished, so
    //  there is no record to replay and no answer to give. The ledger refuses
    //  rather than running the payment a second time.
    match finance
        .with_idempotency(invoice.clone())
        .instruct("ACME Bolts".to_string(), 12_500)
        .await
    {
        Ok(_) => return Err("a parked key must not be answered twice".into()),
        Err(error) => println!("retried too early: {error}\n  what now? {}", why(&error)),
    }

    // ── 6. The second signature. It answers the parked reply from its own
    //       turn, and the original caller's single call returns.
    println!("approved {} request(s)", desk.approve().await?);
    let first = asking.await??;
    println!("paid: run {} for {} cents", first.run, first.cents);

    // ── 7. The retry that a lost connection would issue. Same key, same
    //       arguments. The handler is not entered again: the reply comes back
    //       from the record.
    let again = finance
        .with_idempotency(invoice.clone())
        .instruct("ACME Bolts".to_string(), 12_500)
        .await?;
    assert_eq!(first, again, "the retry must replay, not re-pay");
    assert_eq!(finance.key_state(invoice.clone()).await?, KeyState::Done);
    assert_eq!(
        finance.paid().await?.len(),
        1,
        "one invoice, one execution, whatever the caller did"
    );

    // ── 8. What the key bought, measured rather than recommended.
    //
    //  The same two calls without one. Nothing refuses them, because nothing
    //  ties them together, and the supplier is paid twice.
    for _ in 0..2 {
        let asking = {
            let finance = finance.clone();
            tokio::spawn(async move { finance.instruct("Bolt Depot".to_string(), 4_000).await })
        };
        parked(&desk, 1).await?;
        desk.approve().await?;
        asking.await??;
    }
    let ledger = finance.paid().await?;
    println!("the ledger holds {} payments", ledger.len());
    assert_eq!(
        ledger.len(),
        3,
        "the keyed invoice paid once and the unkeyed one paid twice"
    );

    // ── 9. The retry that helps instead of racing.
    //
    //  `attaching_for` says what to do when the key is already in flight: wait
    //  up to the budget for that call's answer. The serving side caps the
    //  budget, so asking for longer than it allows is accepted and shortened.
    let repair = IdempotencyKey::new("invoice-9001")?;
    let asking = {
        let finance = finance.clone();
        let repair = repair.clone();
        tokio::spawn(async move {
            finance
                .with_idempotency(repair)
                .instruct("Kiln Spares".to_string(), 8_100)
                .await
        })
    };
    parked(&desk, 1).await?;

    let attaching = {
        let finance = finance.clone();
        let waiting = Idempotency::attaching_for(repair, Duration::from_secs(5));
        tokio::spawn(async move {
            finance
                .with_idempotency(waiting)
                .instruct("Kiln Spares".to_string(), 8_100)
                .await
        })
    };

    desk.approve().await?;
    let placed = asking.await??;
    let attached = attaching.await??;
    assert_eq!(
        placed, attached,
        "the attacher takes the first call's answer"
    );
    assert_eq!(
        finance.paid().await?.len(),
        4,
        "attaching waits for the payment; it does not make a second one"
    );
    println!("attached to run {} instead of paying again", attached.run);

    // ── 10. The restart, and the boundary a reader has to know.
    //
    //  Stop the actor and drop the server. Everything below this line is a new
    //  process as far as the treasury is concerned: a new node, a new service,
    //  a new client, and a state read back off the disk.
    desk.stop().await?;
    drop(server);
    drop(finance);

    //  The claim is released when the previous owner drops it, and dropping
    //  runs on the actor's own task rather than on this one. So a restart waits
    //  for the claim rather than assuming it. `Busy` is NOT a transient error
    //  in general — two owners is exactly what the claim exists to make
    //  visible — and it is transient only here, where this process is the
    //  owner that just let go.
    let reopened = {
        let mut opened = None;
        for _ in 0..200 {
            match Persist::<Treasury, _, Whole>::open(&store, &id).await {
                Ok(ready) => {
                    opened = Some(ready);
                    break;
                }
                Err(keylink_persist::Error::Busy { .. }) => {
                    n0_future::time::sleep(Duration::from_millis(10)).await;
                }
                Err(other) => return Err(other.into()),
            }
        }
        opened.ok_or("the previous incarnation never released its claim")?
    };
    assert!(
        reopened.was_stored(),
        "an answered payment is a stored payment"
    );
    let (restored, saver) = reopened.start_from(Treasury::default);
    println!(
        "after the restart, the treasury remembers {} payments",
        restored.paid.len()
    );
    assert_eq!(restored.paid.len(), 4, "every payment came back");

    //  The record store is claimed too, and released on the same schedule, so
    //  reopening it waits the same way and for the same reason.
    let recovered = {
        let mut opened = None;
        for _ in 0..200 {
            match DurableRecords::open(&keys, &ledger_id).await {
                Ok(ready) => {
                    opened = Some(ready);
                    break;
                }
                Err(RecordError::Store(keylink_persist::Error::Busy { .. })) => {
                    n0_future::time::sleep(Duration::from_millis(10)).await;
                }
                Err(other) => return Err(other.into()),
            }
        }
        opened.ok_or("the previous incarnation never released its record store")?
    };

    let treasury_again = Node::bind_local().await?;
    let desk = treasury::setup(restored).observe(saver).actor();
    let _serving = Server::builder(&treasury_again)
        .service(
            desk.service(Allowlist::only([finance_node.id()]))
                .records(Records::Durable(recovered))
                .build(),
        )
        .spawn()?;
    //  The SAME finance node, which is load bearing rather than convenient. A
    //  record is keyed on the peer the treasury proved, so a desk that came back
    //  with a fresh identity would read `Fresh` whatever the disk held — and
    //  this act would be measuring the key instead of the ledger.
    let finance =
        treasury::Client::connect(&finance_node, treasury_again.endpoint().addr()).await?;

    // ── 11. What came back, measured rather than asserted in prose.
    //
    //  The record was written to the disk before its reply went to the wire, so
    //  the key that read `Done` before the restart reads `Done` after it.
    let after_the_restart = finance.key_state(invoice.clone()).await?;
    println!("after the restart, the ledger says {after_the_restart:?}");
    assert_eq!(
        after_the_restart,
        KeyState::Done,
        "a settled key must survive the process that settled it"
    );

    //  And the retry is answered from the record rather than by the treasury.
    //  No approval is needed, because nothing parks: the second signature
    //  belongs to a payment that already happened.
    let replayed = finance
        .with_idempotency(invoice.clone())
        .instruct("ACME Bolts".to_string(), 12_500)
        .await?;
    assert_eq!(
        replayed, first,
        "the repeat must be the first reply, byte for byte"
    );
    let after = finance.paid().await?.len();
    println!(
        "the same key replayed run {} instead of paying again, and the count is still {after}",
        replayed.run
    );
    assert_eq!(after, 4, "a replayed key must not add a payment");

    // ── 12. The half that is still not durable, and cannot be.
    //
    //  A key CLAIMED when the process died belongs to a caller holding a live
    //  stream, and no disk carries one of those forward. It comes back
    //  `Interrupted` — the state that already means *whether it took effect is
    //  unknown* — rather than vacant, which would run the work twice. That is
    //  the conservative answer, and it is where the domain question still
    //  applies: the state is durable, so the treasury itself knows what it paid.
    let history = finance.paid().await?;
    let already = history.iter().filter(|s| s.payee == "ACME Bolts").count();
    assert_eq!(already, 1, "one instruction, one payment, across a restart");
    println!("the durable state shows {already} payment to ACME Bolts, and the ledger agrees");

    desk.stop().await?;
    Ok(())
}
