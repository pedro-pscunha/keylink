//! Effectively-once for one call, composed from parts that already exist.
//!
//! ```sh
//! cargo run -p keylink --example reliable_call
//! ```
//!
//! Two nodes, one real QUIC connection, no network: `Node::bind_local` binds
//! with no relay and no discovery, so this runs on a laptop in a tunnel. A real
//! program uses [`Node::bind`].
//!
//! # The recipe
//!
//! 1. Put an [`IdempotencyKey`] on the call. The serving side records the reply
//!    under `(peer, key)` and answers a repeat from that record, byte for byte.
//! 2. Make the call through a [`Peer`], which reconnects once when a call fails
//!    and re-issues it against the new connection. The ledger lives in the
//!    served `Service`, not in the connection, so it is the same ledger.
//! 3. Ask to *attach* rather than be refused, with
//!    [`Idempotency::attaching_for`], so a retry that overtakes the call it is
//!    retrying waits for it instead of being turned away.
//!
//! Composed, that is effectively-once: the work runs at most once, and a caller
//! that lost its answer gets the answer rather than a second execution.
//!
//! # What it is not
//!
//! Not durable execution. Everything here is one process's memory, and
//! `README.md` names the seven limits — including the two easiest to assume
//! away: **there is no reliable `tell`**, and **a `feed` has no delivery
//! guarantee at all**. Both are local decisions to send something nobody acks;
//! neither is a call, and no key can make one.

use std::error::Error as StdError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use keylink::{
    Allowlist, Body, Context, Error, Idempotency, IdempotencyKey, IdempotencyRefusal, KeyState,
    Node, Peer, RemoteError, Wire, WithIdempotency,
};
use postcard_schema::Schema;
use serde::{Deserialize, Serialize};

/// The reply names which run of the handler produced it, so a repeated number is
/// itself the evidence that the handler was not entered again.
#[derive(Debug, Clone, Serialize, Deserialize, Schema)]
pub struct Receipt {
    /// Which run of the handler produced this reply.
    pub run: u64,
    /// The item the order named.
    pub sku: String,
}

keylink::service! {
    service Orders;

    rpc Submit(sku: String, qty: u32) -> Receipt;

    // A reply with a payload behind it. Bodies are streamed and never held, so
    // this one cannot be recorded — the limit this example demonstrates rather
    // than merely mentioning.
    recv Datasheet(sku: String) -> Receipt;
}

#[derive(Default)]
struct Desk {
    submits: AtomicU64,
    datasheets: AtomicU64,
}

impl orders::Handler for Desk {
    async fn submit(&self, _cx: &Context, sku: String, _qty: u32) -> Receipt {
        Receipt {
            run: self.submits.fetch_add(1, Ordering::SeqCst) + 1,
            sku,
        }
    }

    async fn datasheet(&self, _cx: &Context, sku: String) -> (Receipt, Body) {
        let receipt = Receipt {
            run: self.datasheets.fetch_add(1, Ordering::SeqCst) + 1,
            sku,
        };
        (receipt, Body::from_bytes(vec![0u8; 64]))
    }
}

/// What a caller *does* about an answer that is not the reply.
///
/// This is the half a recipe usually leaves out. Three of these mean "stop
/// retrying this key", and telling them apart is the whole point of having six
/// variants instead of one error string.
fn what_now(error: &Error) -> &'static str {
    let Error::Remote(RemoteError::Idempotency(refusal)) = error else {
        // Not the ledger talking. A transport error is what `Peer` already
        // retried once; anything else is this service's own refusal.
        return "not a ledger refusal: retry the call, with the same key";
    };
    match refusal {
        IdempotencyRefusal::InFlight => {
            "the work is running: ask again, or ask with `attaching_for` and wait for it"
        }
        IdempotencyRefusal::TooManyRunning { .. } => {
            "the far side is saturated: stop adding load, then ask again with the same key"
        }
        // Whether the work took effect is genuinely unknown, and no record can
        // make it known. Retrying the same key will refuse forever, by design.
        IdempotencyRefusal::Interrupted => {
            "unknown whether it took effect: reconcile out of band, then use a NEW key"
        }
        // At-most-once still holds, so the work is not owed again. What is gone
        // is the answer.
        IdempotencyRefusal::Forgotten => {
            "it ran and the reply expired: read the result from the domain, do not re-run"
        }
        // A permanent property of the method, not an eviction.
        IdempotencyRefusal::Unrecordable => {
            "this method's reply carries a payload: fetch it again without a key"
        }
        IdempotencyRefusal::Malformed(_) => "the key itself is wrong: fix the caller",
        // `IdempotencyRefusal` is `#[non_exhaustive]`, so this arm is the
        // compiler's, not a choice. A refusal this build has never heard of is
        // still a refusal: do not retry blindly, ask `key_state` and decide.
        _ => "an unfamiliar refusal: ask `key_state` before deciding anything",
    }
}

/// One attempt at the order, and every attempt is exactly this one.
///
/// A retry is not a different call: same key, same arguments, same budget. The
/// only thing that changes between attempts is the connection underneath, and
/// `Peer` owns that.
async fn submit(desk: &Peer<orders::Client>, key: &IdempotencyKey) -> keylink::Result<Receipt> {
    desk.call(|client| {
        let attaching = Idempotency::attaching_for(key.clone(), Duration::from_secs(5));
        async move {
            client
                .with_idempotency(attaching)
                .submit("ADU-014".to_string(), 2)
                .await
        }
    })
    .await
}

/// The same shape for the method whose reply carries a payload.
async fn datasheet(
    desk: &Peer<orders::Client>,
    key: &IdempotencyKey,
) -> keylink::Result<(Receipt, keylink::BodyReader)> {
    desk.call(|client| {
        let key = key.clone();
        async move {
            client
                .with_idempotency(key)
                .datasheet("ADU-014".to_string())
                .await
        }
    })
    .await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    // ── 1. Two nodes. The desk serves; the buyer calls.
    let desk_node = Node::bind_local().await?;
    let buyer_node = Node::bind_local().await?;
    let _serving = orders::serve(&desk_node, Desk::default(), Allowlist::anyone())?;

    // A `Peer` rather than a `Client`: it dials on first use and, when a call
    // fails, reconnects **once** and re-issues that call against the new
    // connection. That single re-issue is the reason the key matters.
    let desk: Peer<orders::Client> = Peer::new(&buyer_node, desk_node.endpoint().addr());

    // ── 2. One key for one piece of work. It belongs to the work, not to the
    //       attempt: every retry of this order carries this same key, and a
    //       different order gets a different one.
    let order = IdempotencyKey::new("order-4471")?;

    // ── 3. Ask before asking, when the answer matters. `Fresh` is the one state
    //       a retry cannot discover for you, because discovering it by retrying
    //       means running the work.
    let before = desk
        .call(|client| {
            let order = order.clone();
            async move { client.key_state(order).await }
        })
        .await?;
    println!("before the call, the ledger says {before:?}");

    // ── 4. The call. `attaching_for` says what to do if this key is already in
    //       flight: wait up to the budget for the answer, rather than be refused
    //       with `InFlight`. That is the difference between a retry that helps
    //       and a retry that races.
    let first = submit(&desk, &order).await?;
    println!("submitted: run {} of {}", first.run, first.sku);

    // ── 5. The retry. Same key, same call, and the handler is not entered
    //       again — the reply comes back from the record, byte for byte. This is
    //       what a caller that lost its answer to a dead connection receives.
    let again = submit(&desk, &order).await?;
    println!("retried:   run {} (the same run, replayed)", again.run);
    assert_eq!(first.run, again.run, "the retry must not re-run the work");

    let after = desk
        .call(|client| {
            let order = order.clone();
            async move { client.key_state(order).await }
        })
        .await?;
    println!("after the retry, the ledger says {after:?}");
    assert_eq!(after, KeyState::Done);

    // ── 6. A limit, met rather than described. A reply that carries a payload is
    //       never recorded, so its repeat is refused instead of being answered
    //       with a header whose body never arrives.
    let sheet_key = IdempotencyKey::new("datasheet-4471")?;
    let (sheet, body) = datasheet(&desk, &sheet_key).await?;
    println!(
        "datasheet: run {}, {} bytes of payload",
        sheet.run,
        body.declared_len().unwrap_or(0)
    );

    match datasheet(&desk, &sheet_key).await {
        Ok(_) => println!("unexpected: a reply with a payload was replayed"),
        Err(error) => println!(
            "refused as expected: {error}\n  what now? {}",
            what_now(&error)
        ),
    }

    Ok(())
}
