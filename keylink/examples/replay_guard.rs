//! The retry the ledger cannot see, and the set inside the state that can.
//!
//! ```sh
//! cargo run -p keylink --example replay_guard
//! ```
//!
//! # The gap this closes
//!
//! `payment_approval.rs` ends at the boundary: a durable state, a durable record
//! store, and one case left over. This example starts there and takes the next
//! step, on a service whose records are [`Records::Memory`](keylink::Records) —
//! the default, and what most services run.
//!
//! A keyed call is refused a second run by the ledger in
//! `keylink/src/idempotency.rs`, and that ledger lives in one process's memory.
//! Restart the process and it is empty. The state is not, because it was stored.
//! So the retry arrives, [`key_state`](keylink::RpcClient::key_state) answers
//! [`Fresh`](keylink::KeyState::Fresh), the handler is entered, and the work runs
//! against a state that already contains it.
//!
//! [`CallId`] is the name that call is known by inside the handler, and
//! [`Applied`] is somewhere to keep it. The set lives in **the state's own
//! snapshot**, beside the balance it protects, so the observer chain stores the
//! work and the record of the work together or stores neither.
//!
//! # The precondition, measured rather than asserted
//!
//! A `CallId` is a digest of the key **and the calling peer**. So the guarantee
//! rests on that peer keeping its identity across the restart. Act 5 is the
//! control: the same key and the same arguments from a caller on a fresh keypair
//! hash to a different `CallId`, are not recognised, and run the work again.
//!
//! That is why [`SecretKey`](keylink::SecretKey) appears here at all. A
//! long-lived key on the calling side is not a deployment preference, it is what
//! makes the dedupe reachable.
//!
//! # What this costs, stated rather than hidden
//!
//! A duplicate still enters the handler and still pays a turn. Refusing before
//! dispatch is what [`Records::Durable`](keylink::Records) buys, and this is not
//! that. What it buys is that the turn changes nothing and answers the first
//! reply.
//!
//! # The store is written here, in about forty lines
//!
//! Every other example in this repository stores through
//! [`keylink_persist::file::Files`]. This one implements
//! [`Snapshots`] and [`Claim`] over a `Vec<u8>` instead, because the port is
//! two methods and a reader should see how small it is. It is not a disk and it
//! does not pretend to be: the restart below is a new node, a new service and a
//! new client reading bytes the old incarnation wrote.

use std::error::Error as StdError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use keylink::prelude::*;
use keylink::{
    Allowlist, Applied, CallId, Caller, Context, IdempotencyKey, KeyState, Node, Persist,
    SecretKey, APPLIED_BOUND, MAX_KEY_LEN,
};
use keylink_persist::{Claim, Durable, RestoreError, Snapshots, StateId};
use postcard_schema::Schema;
use serde::{Deserialize, Serialize};

/// What one withdrawal answered.
///
/// `run` counts executions rather than requests, so a repeated `run` is itself
/// the evidence that the handler changed nothing the second time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub struct Receipt {
    /// Which execution produced this receipt.
    pub run: u32,
    /// What was left after it.
    pub left: u64,
}

keylink::service! {
    service Vault;

    /// Take money out. Idempotent across a restart, by the set below.
    rpc Withdraw(cents: u64) -> Receipt;

    /// What is left, so the example can measure rather than trust.
    rpc Held() -> u64;

    /// How many withdrawals actually executed.
    rpc Runs() -> u32;
}

/// The vault. The dedupe set is a field like any other, and it is snapshotted
/// like any other.
#[derive(Default, Serialize, Deserialize)]
struct Vault {
    cents: u64,
    runs: u32,
    /// Bounded at [`APPLIED_BOUND`], which is 4096 calls. The bound is the
    /// caller's decision because the caller is the one that knows what a
    /// duplicate costs it.
    applied: Applied<Receipt>,
}

impl vault::Actor for Vault {
    async fn withdraw(&mut self, cx: Caller<'_>, cents: u64) -> Receipt {
        // `Caller::Local` has no peer, no proven key and therefore no `CallId`.
        // A local call is never deduplicated here, and `Applied` says so by
        // taking an `Option`: `get(None)` is always `None` and `insert(None,
        // ..)` stores nothing.
        //  The type is written out because it is the subject: a `CallId` is a
        //  digest of the key AND the calling peer, so it is the identity of
        //  *this peer's* attempt rather than of the key alone.
        let call: Option<CallId> = cx.peer().and_then(Context::call);

        // The three lines the module header says keylink will not write for
        // you. Replaying the first answer is right here because the receipt is
        // what the caller lost; a service whose answer must not be replayed
        // refuses instead.
        if let Some(answer) = self.applied.get(call) {
            return answer.clone();
        }

        self.cents = self.cents.saturating_sub(cents);
        self.runs += 1;
        let receipt = Receipt {
            run: self.runs,
            left: self.cents,
        };
        self.applied.insert(call, receipt.clone());
        receipt
    }

    async fn held(&mut self, _cx: Caller<'_>) -> u64 {
        self.cents
    }

    async fn runs(&mut self, _cx: Caller<'_>) -> u32 {
        self.runs
    }
}

impl Durable for Vault {
    const VERSION: u32 = 1;

    /// `applied` rides along with everything else, and that is the design rather
    /// than a convenience. `Applied<T>` implements `Serialize` so that adding
    /// the field to a state that already snapshots itself with
    /// `postcard::to_stdvec(self)` needs no new line here.
    fn snapshot(&self) -> Vec<u8> {
        postcard::to_stdvec(self).unwrap_or_default()
    }

    fn restore(bytes: &[u8]) -> Result<Self, RestoreError> {
        postcard::from_bytes(bytes).map_err(|error| RestoreError::Malformed(error.to_string()))
    }

    fn migrate(_from: u32, _bytes: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> {
        Ok(None)
    }
}

// ─────────────────────────────────────────────── the port, written out in full

/// A store that is one `Vec<u8>` behind a lock.
///
/// Exclusivity is what the port is for, so this one enforces it rather than
/// assuming it: a second [`claim`](Snapshots::claim) while the first is alive is
/// [`Busy`](keylink_persist::Error::Busy), exactly as the file adapter's lock
/// file is on a real machine.
#[derive(Default, Clone)]
struct Bucket {
    bytes: Arc<Mutex<Option<Vec<u8>>>>,
    held: Arc<AtomicBool>,
}

impl Snapshots for Bucket {
    type Claim = BucketClaim;

    async fn claim(&self, id: &StateId) -> keylink_persist::Result<Self::Claim> {
        // `swap` and not a read followed by a write. Two tasks reading `false`
        // and both writing `true` is the whole failure this returns.
        if self.held.swap(true, Ordering::SeqCst) {
            return Err(keylink_persist::Error::Busy { id: id.clone() });
        }
        Ok(BucketClaim {
            bytes: Arc::clone(&self.bytes),
            held: Arc::clone(&self.held),
        })
    }
}

/// Exclusive access, released by [`Drop`] and by nothing else.
struct BucketClaim {
    bytes: Arc<Mutex<Option<Vec<u8>>>>,
    held: Arc<AtomicBool>,
}

impl Claim for BucketClaim {
    async fn load(&self) -> keylink_persist::Result<Option<Vec<u8>>> {
        Ok(self.bytes.lock().ok().and_then(|held| held.clone()))
    }

    /// Atomic, which a `Vec` behind a lock is for free. An adapter that cannot
    /// promise a crash leaves either the old bytes or the new ones is not an
    /// adapter for this port.
    async fn store(&mut self, bytes: &[u8]) -> keylink_persist::Result<()> {
        if let Ok(mut held) = self.bytes.lock() {
            *held = Some(bytes.to_vec());
        }
        Ok(())
    }
}

impl Drop for BucketClaim {
    fn drop(&mut self) {
        self.held.store(false, Ordering::SeqCst);
    }
}

// ───────────────────────────────────────────────────────────────── the program

/// Reopen a state whose previous owner is on its way out.
///
/// The claim is released when the old incarnation drops it, and dropping runs on
/// the actor's own task rather than on this one. `Busy` is not transient in
/// general — two owners is what a claim exists to make visible — and it is
/// transient only here, where this process is the owner that just let go.
async fn reopen(
    store: &Bucket,
    id: &StateId,
) -> Result<keylink::Opened<Vault, Bucket>, Box<dyn StdError>> {
    for _ in 0..200 {
        match Persist::<Vault, _>::open(store, id).await {
            Ok(ready) => return Ok(ready),
            Err(keylink_persist::Error::Busy { .. }) => {
                n0_future::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            Err(other) => return Err(other.into()),
        }
    }
    Err("the previous incarnation never released its claim".into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let store = Bucket::default();
    let id = StateId::new("vaults/main")?;

    // ── 1. The desk's identity, which the whole example rests on.
    //
    //  A fixed key rather than a fresh one, because this desk has to be the
    //  same peer after the restart as before it. A real program reads this out
    //  of a secret store, or out of `KEYLINK_SECRET` through
    //  `Node::bind_from_env`.
    let desk_secret = SecretKey::from_bytes(&[7; 32]);

    let opened = Persist::<Vault, _>::open(&store, &id).await?;
    let (mut state, saver) = opened.start_from(Vault::default);
    state.cents = 10_000;
    println!("the vault opens holding {} cents", state.cents);
    println!("the dedupe set is bounded at {APPLIED_BOUND} calls");

    let vault_node = Node::bind_local().await?;
    let desk_node = Node::bind_local_with_secret(desk_secret.clone()).await?;
    let vault = vault::setup(state).observe(saver).actor();
    let serving = vault.serve(&vault_node, Allowlist::anyone())?;
    let desk = vault::Client::connect(&desk_node, vault_node.endpoint().addr()).await?;

    // ── 2. One withdrawal, under one key.
    let invoice = IdempotencyKey::new("invoice-5510")?;
    let first = desk
        .with_idempotency(invoice.clone())
        .withdraw(2_500)
        .await?;
    println!("withdrew: run {}, {} cents left", first.run, first.left);
    assert_eq!(desk.key_state(invoice.clone()).await?, KeyState::Done);

    // ── 3. The restart. Everything below this line is a new process as far as
    //       the vault is concerned, and the desk keeps its keypair.
    vault.stop().await?;
    drop(serving);
    drop(desk);

    let reopened = reopen(&store, &id).await?;
    assert!(reopened.was_stored(), "an answered call is a stored call");
    let (restored, saver) = reopened.start_from(Vault::default);
    println!(
        "after the restart the vault holds {} cents and remembers {} applied call(s)",
        restored.cents,
        restored.applied.len()
    );

    let vault_again = Node::bind_local().await?;
    let vault = vault::setup(restored).observe(saver).actor();
    let _serving = vault.serve(&vault_again, Allowlist::anyone())?;
    let desk_node = Node::bind_local_with_secret(desk_secret).await?;
    let desk = vault::Client::connect(&desk_node, vault_again.endpoint().addr()).await?;

    // ── 4. The ledger forgot, and the state did not.
    //
    //  `Fresh` is the honest answer: the ledger is this process's memory and
    //  this process is new. Asking is still safe, it runs no handler. It is
    //  merely uninformative, which is the sentence `call.rs` writes about
    //  exactly this configuration.
    let after = desk.key_state(invoice.clone()).await?;
    println!("the ledger now says {after:?}, because the ledger is memory");
    assert_eq!(after, KeyState::Fresh);

    let again = desk
        .with_idempotency(invoice.clone())
        .withdraw(2_500)
        .await?;
    assert_eq!(again, first, "the retry must replay the first receipt");
    assert_eq!(desk.held().await?, first.left, "and move no money");
    assert_eq!(desk.runs().await?, 1, "one instruction, one execution");
    println!(
        "the retry replayed run {} and the balance is still {}",
        again.run,
        desk.held().await?
    );

    // ── 5. The control, which is what makes act 4 a measurement.
    //
    //  The same key, the same arguments, the same service. Only the calling
    //  keypair differs, so the digest differs, so the set does not recognise it
    //  — and the money moves. A caller that loses its identity loses the
    //  guarantee, silently and indistinguishably from a first call.
    let stranger_node = Node::bind_local().await?;
    let stranger = vault::Client::connect(&stranger_node, vault_again.endpoint().addr()).await?;
    let unrecognised = stranger.with_idempotency(invoice).withdraw(2_500).await?;
    assert_eq!(unrecognised.run, 2, "a fresh keypair is a fresh call");
    assert_eq!(desk.runs().await?, 2);
    println!(
        "the same key from a new keypair ran again: run {}, {} cents left",
        unrecognised.run, unrecognised.left
    );

    // ── 6. The other half of the key's identity: its size.
    //
    //  A `CallId` is a digest, so any key length would hash to the same 32
    //  bytes. The cap is not about the digest — it is about what a peer can make
    //  the serving side hold and compare *before* the digest exists. Refused at
    //  construction, so an over-long key is a caller's bug in the caller's
    //  process rather than a frame the far side has to buffer and reject.
    let over = "k".repeat(MAX_KEY_LEN + 1);
    assert!(
        IdempotencyKey::new(&over).is_err(),
        "a key past {MAX_KEY_LEN} bytes has no constructor"
    );
    assert!(
        IdempotencyKey::new("k".repeat(MAX_KEY_LEN)).is_ok(),
        "and the cap itself is admitted, so this is an off-by-one that would show"
    );
    println!("an idempotency key is capped at {MAX_KEY_LEN} bytes, refused where it is built");

    vault.stop().await?;
    Ok(())
}
