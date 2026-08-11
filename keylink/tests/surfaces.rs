//! One function, four surfaces — and the two arms that keep the boundary
//! visible while it is crossed.
//!
//! This is the file wave 2 exists for. Everything before it is machinery: a
//! shared error, a marker, a generated trait. None of that proves the thing the
//! goal asked for, which is that **a caller can write one function and run it
//! against a local actor and a remote one**. The three tests below are that
//! proof, and each carries a different part of it: the four surfaces, the bound
//! that refuses one of them, and the arm no local surface can reach.
//!
//! # Why a new file rather than a home in `tests/actors.rs`
//!
//! `tests/actors.rs` is about the actor surface. *Surface-genericity* is a
//! cross-cutting claim with no owner over there, and it needs four surfaces
//! standing at once — a local [`ledger::Handle`], an addressed
//! [`ledger::ActorRef`], a plain [`ledger::Client`] and a keyed
//! [`ledger::IdempotentClient`]. The fixture is borrowed from
//! `actors.rs`'s `one_panic_reads_the_same_whichever_way_the_service_is_served`,
//! which already stands up a keyed system and a singleton on one node; nothing
//! else is borrowed.
//!
//! # Three of the four reach the same actor, and that is the assertion
//!
//! A test that only checked *four types compile* would stay green if every call
//! answered from a different actor, or from none. So the singleton is served and
//! held locally at the same time, and the totals it answers are read in order:
//! `1`, `3`, `7`. The keyed actor is separate and answers `8`. Genericity is
//! what the signature proves; the running total is what proves the calls landed.

#![allow(clippy::expect_used)]

use keylink::prelude::*;
use std::sync::{Arc, OnceLock};

use keylink::{
    ActorId, Allowlist, Caller, Eviction, Failure, Governed, IdempotencyKey, Server, Withheld,
};
use tokio::sync::mpsc;

mod common;
use common::local_node;

keylink::service! {
    service Ledger;

    /// Apply one entry, answered with the running total.
    ///
    /// One `rpc` and nothing else is what every surface has, which is the whole
    /// of why `Calls` carries this method and no `tell`, `send`, `recv` or
    /// `blob`.
    rpc Note(entry: u64) -> u64;

    /// Call this actor's own `note` from inside its own turn, through `Calls`.
    ///
    /// The only door to a `Failure::Reentrant`: it is produced by the identity
    /// check inside the mailbox, so nothing outside a turn can reach one.
    rpc Reenter() -> u64;
}

/// A running total, plus the two wires only `a_local_surface_never_reports_other`
/// needs.
///
/// Both are `None` or unset for the served fixture, which never calls `reenter`.
/// A handle to yourself is the shape `tests/cycles.rs` uses for the same reason:
/// the state is moved into the loop's task, so the handle has to arrive through
/// an `Arc<OnceLock<_>>` that was cloned before the spawn.
#[derive(Default)]
struct Ledger {
    total: u64,
    me: Arc<OnceLock<ledger::Handle<Ledger>>>,
    refused: Option<mpsc::UnboundedSender<Failure>>,
}

impl ledger::Actor for Ledger {
    async fn note(&mut self, _cx: Caller<'_>, entry: u64) -> u64 {
        self.total += entry;
        self.total
    }

    async fn reenter(&mut self, _cx: Caller<'_>) -> u64 {
        let me = self.me.get().expect("wired before the first call").clone();
        // The SAME function the test below drives against four surfaces, called
        // here against a fifth position: this actor's own handle, from inside
        // this actor's own turn. It answers rather than deadlocking, and the
        // answer is the one arm no served surface can produce.
        let refused = total_after(&me, 1)
            .await
            .expect_err("a call from inside the target actor's own turn is refused");
        self.refused
            .as_ref()
            .expect("wired before the first call")
            .send(refused)
            .expect("the test holds the receiver until it has read this");
        self.total
    }
}

// ───────────────────────────────────────────────────── the two generic functions

/// **The wave's headline claim, and it is one signature.**
///
/// It names no surface, no transport and no locality. Before `Failure` existed
/// this could not be written at all: a local call answered `Unanswered` and a
/// served one answered `Error`, and no `Result` type is both.
///
/// Both callers below are real — the four-surface test drives it, and
/// `Ledger::reenter` calls it from inside a turn.
async fn total_after<C: ledger::Calls>(surface: &C, entry: u64) -> Result<u64, Failure> {
    surface.note(entry).await
}

/// The second signature, and it is the only thing `Governed` is for.
///
/// `impl Calls + Governed` is the bound that says *this surface reached the
/// actor through a `Dispatch`, so the service's `Middleware` stack and its
/// `Allowlist` ran*. The refusal half — that a [`ledger::Handle`] cannot be
/// passed here — is held by the `compile_fail` doctest on
/// [`Governed`](keylink::Governed), because a refusal cannot be written as a
/// call. This is the acceptance half.
///
/// It is a **second** function rather than a bound added to the first, and that
/// is forced rather than chosen: `one_function_calls_four_surfaces` is driven
/// against a `Handle`, which `Governed` refuses by construction.
async fn total_after_governed<C: ledger::Calls + Governed>(
    surface: &C,
    entry: u64,
) -> Result<u64, Failure> {
    surface.note(entry).await
}

// ───────────────────────────────────────────────────────────────────── fixture

fn id(name: &str) -> ActorId {
    ActorId::new(name).expect("a valid id")
}

fn key(text: &str) -> IdempotencyKey {
    IdempotencyKey::new(text).expect("a valid key")
}

// ───────────────────────────────────────────────────────────────────── the claim

/// One function. Four surfaces. No `#[cfg]`, no second signature, no downcast.
///
/// The singleton is held locally **and** served on the same node, so `Handle`,
/// `Client` and `IdempotentClient` all address one actor and its running total
/// orders them: `1`, `3`, `7`. `ActorRef` addresses the mounted actor in the
/// keyed system and answers `8` from a total of its own.
///
/// Both ALPNs sit on one node, which `tests/protocol.rs` already holds apart —
/// `alpn()` and `registry_alpn()` must not hash the same *"or one node cannot serve
/// both"*, and `.spawn()` here is the second reading of that.
///
/// Made red by `scripts/break-once.sh keylink/src/macros.rs`, as `T3`. Search
/// `self.$name($( $arg ),*).await.map_err(::core::convert::From::from)` — the
/// one `rpc` arm of `__keylink_calls_impl!` — and hand every method a default
/// where the caller's argument should be:
/// `self.$name($( { let _ = $arg; ::core::default::Default::default() } ),*).await.map_err(::core::convert::From::from)`.
/// Watched: `── selected=1`, `── exit=100`, and one assertion reporting
/// `left: [0, 0, 0, 0]` against `right: [1, 3, 7, 8]`.
///
/// **Four columns move on one substitution, and that is the whole reason this
/// record is worth keeping.** `__keylink_calls_impl!` is one emitter and the
/// four impls are four uses of it, so a red here says every surface reached the
/// actor through the trait. If any column stayed right, that surface's call was
/// resolving to the inherent method instead — which is exactly the drift the
/// one-emitter shape exists to make impossible, and the one thing no compiler
/// would report.
///
/// The registered literal is the **indented** form, counted after `cargo fmt`,
/// where it resolves to 1. `cargo fmt` owns the line breaks an anchor quotes, so
/// counting before it has left an anchor dead through a whole gate twice in this
/// repository. The record and the code are in different files, so the
/// same-file self-quote hazard `T1` and `T2` avoid by indentation does not
/// arise here.
///
/// The filter is this test's own name plus `-p keylink --test surfaces`, and the
/// harness appends it **positionally** as a plain libtest substring. A filterset
/// handed there matches nothing, which the banner prints as a caught defect.
#[tokio::test]
async fn one_function_calls_four_surfaces() {
    let server = local_node().await;
    let caller = local_node().await;

    let system = ledger::Registry::builder(Eviction::Never).build();
    system
        .mount(id("addressed"), ledger::spawn(Ledger::default()))
        .expect("the id is free");
    let singleton = ledger::actor(Ledger::default());

    let _running = Server::builder(&server)
        .service(system.service(Allowlist::anyone()).build())
        .service(singleton.service(Allowlist::anyone()).build())
        .spawn()
        .expect("the keyed and singleton ALPNs are distinct");

    let to_keyed = ledger::RegistryClient::connect(&caller, server.endpoint().addr())
        .await
        .expect("dials the actor system");
    let to_singleton = ledger::Client::connect(&caller, server.endpoint().addr())
        .await
        .expect("dials the same node's singleton");
    let with_key = to_singleton.with_idempotency(key("one-function-four-surfaces"));

    let local = total_after(&singleton, 1)
        .await
        .expect("the mailbox answers a local call");
    let served = total_after(&to_singleton, 2)
        .await
        .expect("the same actor answers a served call");
    let ledgered = total_after(&with_key, 4)
        .await
        .expect("the same actor answers a keyed call");
    let addressed = total_after(&to_keyed.at(id("addressed")), 8)
        .await
        .expect("the mounted actor answers an addressed call");

    // ONE assertion over all four, and that is what makes the red readable.
    // `Calls` gives the four impls one emitter, so a substitution in that
    // emitter moves every column of this array at once — which is the whole
    // claim, and an assertion per surface would report it one surface at a time
    // and stop at the first.
    assert_eq!(
        [local, served, ledgered, addressed],
        [1, 3, 7, 8],
        "one function, four surfaces: the first three name one actor and its \
         running total orders them, and the fourth is the mounted actor with a \
         total of its own"
    );
}

/// `Governed`'s caller, and without it the marker is machinery.
///
/// A trait nothing takes as a bound is a name, not an instrument. This is the
/// bound written the way a downstream author writes it, and driven — so the two
/// surfaces that reach the actor through a `Dispatch` are shown to satisfy it
/// at run time and not only at `cargo check`.
///
/// `ActorRef` is `Governed` too and is deliberately not here: the pair below is
/// what the plan scheduled, and a third surface would widen the claim without
/// changing it.
#[tokio::test]
async fn one_function_calls_two_governed_surfaces() {
    let server = local_node().await;
    let caller = local_node().await;

    let singleton = ledger::actor(Ledger::default());
    let _running = singleton
        .serve(&server, Allowlist::anyone())
        .expect("one service, no duplicate ALPN");

    let client = ledger::Client::connect(&caller, server.endpoint().addr())
        .await
        .expect("dials the served singleton");
    let with_key = client.with_idempotency(key("two-governed-surfaces"));

    let governed = total_after_governed(&client, 3)
        .await
        .expect("a plain client is governed and answers");
    let keyed = total_after_governed(&with_key, 5)
        .await
        .expect("a keyed client is governed and answers");

    assert_eq!(
        [governed, keyed],
        [3, 8],
        "both governed surfaces name one actor, so the second call must see the \
         first one's entry"
    );
}

/// `Failure::Other` has zero local producers, driven rather than argued.
///
/// `Failure`'s rustdoc has said *"no local surface produces this"* since the arm
/// was written, and until now nothing ran it. The population is exactly two,
/// because `Unanswered` has exactly two arms and `From<Unanswered> for Failure`
/// maps each to a named one: a turn loss and a reentrant call. Both are reached
/// here through `Calls`, on a `Handle`.
///
/// **Each loss is asserted on its exact variant rather than on `!matches!(..,
/// Other(_))`**, which is strictly stronger and not a stylistic choice: a change
/// that routed the local impl through anything able to produce an `Error` would
/// have to move one of these two values, and an inexact assertion would still be
/// satisfied by the other one.
#[tokio::test]
async fn a_local_surface_never_reports_other() {
    let (refusals, mut refused) = mpsc::unbounded_channel();
    let me = Arc::new(OnceLock::new());
    let handle = ledger::actor(Ledger {
        total: 0,
        me: Arc::clone(&me),
        refused: Some(refusals),
    });
    me.set(handle.clone()).expect("set once");

    // ── the reentrant half, produced inside a turn and carried out of it
    let total = handle
        .reenter()
        .await
        .expect("the outer call is answered: only the inner one is refused");
    assert_eq!(
        total, 0,
        "the refused inner call must not have applied its entry"
    );

    let reentrant = refused
        .recv()
        .await
        .expect("the handler sent the failure it met");
    // `matches!` and not `withheld()`, which answers `None` for this arm and for
    // `Other` alike and so discriminates neither. `Failure::withheld`'s own
    // doctest holds that fact; repeating it here would be a second guard on one
    // claim rather than a guard on this one.
    assert!(
        matches!(reentrant, Failure::Reentrant),
        "a call from inside the target actor's own turn is `Reentrant`: got {reentrant:?}"
    );

    // ── the turn-loss half
    handle
        .stop()
        .await
        .expect("a stop from outside the actor's own turn is never refused");
    let gone = total_after(&handle, 1)
        .await
        .expect_err("a stopped actor answers nothing");
    assert!(
        matches!(gone.withheld(), Some(Withheld::Gone)),
        "a call to a stopped actor is `Turn(Gone)`: got {gone:?}"
    );
}
