//! Who may call, which methods they may call, and taking it back while they are
//! connected.
//!
//! ```sh
//! cargo run -p keylink --example gatehouse
//! ```
//!
//! Nearly every example in this repository reaches for [`Allowlist::anyone`],
//! and a handful name their peers with [`Allowlist::only`] — this file among
//! them, at act 3. That is the whole of the authorization story a reader could
//! see, and it is the smallest part of it.
//!
//! No count here, on purpose. The sentence carried one, the count was wrong,
//! the correction was wrong in the same direction — it said three and omitted
//! this file — and nothing in the gate can see either. A claim whose truth
//! depends on counting sibling files is a claim that rots on the next commit.
//!
//! # Three decisions, three mechanisms
//!
//! | question | mechanism | when it runs |
//! |---|---|---|
//! | may this peer connect at all | [`Admission`] | once, at the handshake |
//! | may this peer call *this method* | [`Rules`] compiled to a layer | on every call |
//! | may it still, right now | [`Revocable`] | whenever the policy changes |
//!
//! They are not interchangeable, and the first is the one readers assume covers
//! the other two.
//!
//! # The dial succeeds either way
//!
//! Admission runs one layer above the handshake, so a refused peer connects and
//! is refused on its first call. A program that treats a live connection as
//! proof of access is wrong here, and `payment_approval.rs` says so too.
//!
//! # Nothing here needs a network
//!
//! Every dialling node binds with [`Node::bind_local`], so this runs offline.
//! Act 4 binds one node from the environment to show what
//! [`Node::bind_from_env`] does with its variable, and never dials it.
//!
//! # Every identity is pinned, and the environment is written before the runtime
//!
//! `main` is synchronous. It derives the peers' ids from fixed
//! [`SecretKey`]s, writes both environment variables, and only then starts the
//! runtime that `run` needs.
//!
//! That order is not tidiness. `setenv` is not thread-safe against a concurrent
//! `getenv`, and by the time a `#[tokio::main]` body executes there are worker
//! threads and iroh's own threads running. This crate is edition 2021, so the
//! call compiles without `unsafe` — edition 2024 makes it `unsafe` for exactly
//! this reason. Writing the environment before any thread exists is what makes
//! the race unreachable rather than merely rare.
//!
//! Deriving an id needs no node: [`SecretKey::public`] is arithmetic. That is
//! also how a real deployment writes an allowlist — the ids are known before
//! anything binds, which is the whole point of naming peers by public key.

use std::error::Error as StdError;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use keylink::prelude::*;
use keylink::{
    Admission, Admittance, AdmittedPeer, Allowlist, Compiled, ConfigError, Context, Departure,
    DynamicAdmission, Error, LiveConnection, Node, ProvenPeer, Refusal, RemoteError, Revocable,
    Rules, SecretKey, Server, Watch,
};

keylink::service! {
    service Gate;

    /// Anybody on the list may knock.
    rpc Knock() -> u32;

    /// Reading the books is a second permission, and the rules below give it to
    /// one peer and withhold it from another that is otherwise admitted.
    rpc Books() -> u32;
}

keylink::service! {
    service Door;

    /// One method, so that act 3 measures revocation and nothing else.
    rpc Enter() -> u32;
}

struct Gate;

impl gate::Handler for Gate {
    async fn knock(&self, _cx: &Context) -> u32 {
        1
    }

    async fn books(&self, _cx: &Context) -> u32 {
        2
    }
}

struct Door;

impl door::Handler for Door {
    async fn enter(&self, _cx: &Context) -> u32 {
        3
    }
}

/// A policy that decides nothing itself and watches one that does.
///
/// Written out because [`Admission`] is the seam a deployment reaches for when
/// an allowlist is not the shape of its answer — a directory lookup, a
/// certificate attribute, a tenant table. The two arguments are the whole
/// interface: a [`ProvenPeer`] is an identity iroh proved at the handshake, and
/// an [`AdmittedPeer`] is one this policy has just said yes to. Neither can be
/// constructed by hand, which is what stops a `bool` from being computed and
/// dropped.
struct Doorman {
    inner: Arc<DynamicAdmission>,
    seen: AtomicU32,
    refused: AtomicU32,
}

impl Doorman {
    fn new(initial: Allowlist) -> Self {
        Self {
            inner: Arc::new(DynamicAdmission::new(initial)),
            seen: AtomicU32::new(0),
            refused: AtomicU32::new(0),
        }
    }
}

impl Admission for Doorman {
    fn admits(&self, peer: &ProvenPeer) -> Admittance {
        self.seen.fetch_add(1, Ordering::SeqCst);
        // All three arms, always. `Unavailable` is not a refusal: it tells the
        // caller to try later, and collapsing it into `Refused` tells them to
        // stop for good.
        match self.inner.admits(peer) {
            Admittance::Admitted => Admittance::Admitted,
            Admittance::Refused => {
                self.refused.fetch_add(1, Ordering::SeqCst);
                Admittance::Refused
            }
            Admittance::Unavailable(why) => Admittance::Unavailable(why),
        }
    }

    /// Forwarding is not optional. The inner policy keeps the live connections
    /// it will later close, so a wrapper that swallows this hands back a
    /// [`Revocable`] that can revoke nothing.
    fn admitted(&self, peer: &AdmittedPeer, live: LiveConnection) {
        self.inner.admitted(peer, live);
    }
}

impl Revocable for Doorman {
    type Change = Allowlist;

    fn apply(&self, next: Self::Change) -> Vec<keylink::EndpointId> {
        self.inner.apply(next)
    }
}

/// What a caller does about a refusal, which is not the same for the two of them.
fn what_now(error: &keylink::Error) -> &'static str {
    match error.refusal() {
        Some(Refusal::Denied) => "the policy said no: ask for access, do not retry",
        Some(Refusal::Unavailable) => "the policy could not be reached: back off and try later",
        None => "not an admission decision: read the error itself",
    }
}

/// The peers this gatehouse knows by key, pinned so their ids exist before the
/// runtime does.
const CLERK: [u8; 32] = [11; 32];
const AUDITOR: [u8; 32] = [22; 32];
/// The identity act 4 reads back out of the environment.
const PINNED_HEX: &str = "0000000000000000000000000000000000000000000000000000000000000031";

fn main() -> Result<(), Box<dyn StdError>> {
    //  Before the runtime, so no other thread can be reading the environment
    //  while this writes it. See the header.
    let clerk = SecretKey::from_bytes(&CLERK).public();
    let auditor = SecretKey::from_bytes(&AUDITOR).public();
    std::env::set_var("KEYLINK_DOOR_ALLOW", format!("{clerk},{auditor}"));
    std::env::set_var("KEYLINK_GATEHOUSE_SECRET", PINNED_HEX);
    run()
}

#[tokio::main]
async fn run() -> Result<(), Box<dyn StdError>> {
    let gate_node = Node::bind_local().await?;
    let clerk_node = Node::bind_local_with_secret(SecretKey::from_bytes(&CLERK)).await?;
    let auditor_node = Node::bind_local_with_secret(SecretKey::from_bytes(&AUDITOR)).await?;
    let tourist_node = Node::bind_local().await?;

    // ── 1. The rules, as a deployment writes them: TOML, not Rust.
    //
    //  `compile` checks every name against this binary's `Req::METHODS`, so a
    //  method that was renamed and a policy that was not is a startup error
    //  rather than a permission that silently stopped applying.
    let raw = format!(
        "[[peer]]\nid = \"{auditor}\"\nallow = [\"Knock\", \"Books\"]\n\n\
         [[peer]]\nid = \"{clerk}\"\nallow = [\"Knock\"]\n",
        auditor = auditor_node.id(),
        clerk = clerk_node.id(),
    );
    //  The type is written out because it is the thing that carries the check:
    //  a `Compiled<Req>` is rules that have already been measured against this
    //  binary's method list, and it is the only input `install` accepts.
    let compiled: Compiled<gate::Request> = Rules::parse(&raw)?.compile::<gate::Request>()?;

    //  `install` gives the validated allowlist to the generated builder AND
    //  installs the method layer itself. There is no spelling of this that
    //  admits the peers and forgets their method rules.
    let _gate = Server::builder(&gate_node)
        .service(compiled.install(|allow| gate::service(Gate, allow)).build())
        .spawn()?;

    let auditor = gate::Client::connect(&auditor_node, gate_node.endpoint().addr()).await?;
    let clerk = gate::Client::connect(&clerk_node, gate_node.endpoint().addr()).await?;
    let tourist = gate::Client::connect(&tourist_node, gate_node.endpoint().addr()).await?;

    // ── 2. Two refusals that are not the same refusal.
    assert_eq!(auditor.knock().await?, 1);
    assert_eq!(auditor.books().await?, 2);
    println!("the auditor knocked and read the books");

    assert_eq!(clerk.knock().await?, 1, "the clerk is admitted");
    let refused = clerk
        .books()
        .await
        .err()
        .ok_or("a method the rules withhold must be refused")?;
    //  The KIND, not merely an error. A dropped connection satisfies "is_err"
    //  too, and it is the opposite diagnosis: this refusal came from the method
    //  layer, one level above the admission that let the clerk in.
    assert!(
        matches!(
            refused,
            keylink::Error::Remote(RemoteError::Unauthorized(_))
        ),
        "a withheld method is the layer's refusal, got {refused:?}"
    );
    println!("the clerk was admitted and refused one method: {refused}");

    //  The tourist is on no `[[peer]]` table at all, so the compiled allowlist
    //  never admitted the connection. Same outcome, one layer earlier.
    let turned_away = tourist
        .knock()
        .await
        .err()
        .ok_or("a peer on no table must not be answered")?;
    //  And this one came from the door rather than from the method layer, which
    //  is what `Refusal::Denied` says and `is_err` cannot.
    assert_eq!(
        turned_away.refusal(),
        Some(Refusal::Denied),
        "an unlisted peer is refused by the policy, not by a lost link"
    );
    println!(
        "the tourist was refused at the door: {turned_away}\n  what now? {}",
        what_now(&turned_away)
    );

    // ── 3. LiveConnection, which is the capability a plain `Admission` cannot have.
    //
    //  A policy is consulted at accept and never again, so without this a peer
    //  removed from the list keeps its open connection until it chooses to hang
    //  up. `Allowlist::from_env` builds the starting policy, which is how a
    //  deployment ships one without a rebuild — and `main` wrote the variable
    //  from the same two pinned keys these nodes bound with, so the policy names
    //  exactly them.
    let policy = Arc::new(Doorman::new(Allowlist::from_env("KEYLINK_DOOR_ALLOW")?));

    let door_node = Node::bind_local().await?;
    let _door = Server::builder(&door_node)
        .service(
            door::service(Door, Allowlist::anyone())
                .admission(policy.clone())
                .build(),
        )
        .spawn()?;

    let leaving = door::Client::connect(&clerk_node, door_node.endpoint().addr()).await?;
    let staying = door::Client::connect(&auditor_node, door_node.endpoint().addr()).await?;
    assert_eq!(leaving.enter().await?, 3);
    assert_eq!(staying.enter().await?, 3);
    println!(
        "two peers are inside, and the policy holds {} of them",
        policy.inner.live()
    );

    //  Taken BEFORE the revocation. A watch registers against a live connection,
    //  so taking one after the close would have nothing left to register on.
    let watch: Watch = leaving.watch();

    let closed = policy.apply(Allowlist::only([auditor_node.id()]));
    assert_eq!(
        closed,
        vec![clerk_node.id()],
        "exactly one peer was revoked"
    );

    //  The connection ends without the peer calling anything. That is the whole
    //  difference: a revoked peer learns immediately rather than on its next
    //  call. `Departure` is exhaustive on purpose — `Closed` is a goodbye,
    //  `Local` is us, and `Lost` is the one worth paging on.
    let departure = tokio::time::timeout(Duration::from_secs(10), watch).await?;
    let why = match departure {
        Departure::Closed => "the peer closed it",
        Departure::Local => "we closed it",
        Departure::Lost(_) => "the link failed or was cut from the far side",
    };
    println!("the revoked peer's live connection ended: {why}");

    //  And it cannot come back. The swap happens before the close, so a peer
    //  reconnecting during the revocation meets the new policy rather than the
    //  old one.
    let back = door::Client::connect(&clerk_node, door_node.endpoint().addr()).await?;
    let readmitted = back
        .enter()
        .await
        .err()
        .ok_or("a revoked peer must not be readmitted")?;
    assert_eq!(
        readmitted.refusal(),
        Some(Refusal::Denied),
        "the new policy refused it, rather than the old connection being stale"
    );
    assert_eq!(
        staying.enter().await?,
        3,
        "revoking one peer must not disturb another"
    );
    //  The wrapper's own counters. Asserting them is what would catch a
    //  `Doorman` that stopped consulting the inner policy, which is the failure
    //  its own documentation above is about.
    assert_eq!(
        policy.refused.load(Ordering::SeqCst),
        1,
        "exactly the readmission attempt was refused at the door"
    );
    println!(
        "the kept peer was never disturbed; the door saw {} handshakes and refused {}",
        policy.seen.load(Ordering::SeqCst),
        policy.refused.load(Ordering::SeqCst)
    );

    // ── 4. An identity out of the environment.
    //
    //  The variable determines the `EndpointId`, so a service that restarts
    //  keeps the id every peer's allowlist names. Unset, it falls back to a
    //  random identity; malformed, it is an error rather than a silent
    //  fallback, because a quietly random key bounces you off every allowlist
    //  you were on.
    let pinned = Node::bind_from_env("KEYLINK_GATEHOUSE_SECRET").await?;
    assert_eq!(
        pinned.id(),
        SecretKey::from_str(PINNED_HEX)?.public(),
        "the variable is the identity"
    );
    println!("bound from the environment as {}", pinned.id());

    // ── 5. What the environment gets wrong, and why it is its own error type.
    //
    //  Every [`ConfigError`] is raised at startup, and none of them can be
    //  reached by a running service — a service that met one never started.
    //  That is the whole reason they are not arms of the call-time error: a
    //  reader triaging a live incident can rule this type out entirely.
    let unset = Allowlist::from_env("KEYLINK_GATEHOUSE_NO_SUCH_VAR")
        .err()
        .ok_or("an unset variable must refuse")?;
    match unset {
        Error::Config(ConfigError::MissingEnvVar { ref var }) => {
            println!("unset:     {unset}");
            assert_eq!(var, "KEYLINK_GATEHOUSE_NO_SUCH_VAR");
        }
        other => return Err(format!("an unset variable is a config error, got {other}").into()),
    }

    //  A malformed key is refused rather than replaced. Silently substituting a
    //  random one would change this node's `EndpointId` and bounce it off every
    //  peer's allowlist, which is a miserable thing to debug at three in the
    //  morning — the failure appears at the peers, not here.
    std::env::set_var("KEYLINK_GATEHOUSE_BROKEN", "not-a-secret-key");
    let malformed = Node::bind_from_env("KEYLINK_GATEHOUSE_BROKEN")
        .await
        .err()
        .ok_or("a malformed key must refuse")?;
    match malformed {
        Error::Config(ConfigError::MalformedSecretKey { ref var, .. }) => {
            println!("malformed: {malformed}");
            assert_eq!(var, "KEYLINK_GATEHOUSE_BROKEN");
        }
        other => return Err(format!("a bad key is a config error, got {other}").into()),
    }

    Ok(())
}
