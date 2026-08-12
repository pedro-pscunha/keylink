//! Changing your mind about a peer, without restarting.
//!
//! An [`Allowlist`] is read once at startup, so revoking a key used to mean a
//! restart — and a restart drops every connection, including the ones that were
//! fine. `DynamicAdmission` is the same value behind a lock, plus the one thing
//! a lock around a value cannot give you: the connections a new policy no
//! longer admits, closed.
//!
//! Two real nodes over a real handshake, because admission is the one decision
//! that happens on the accept path and nowhere else.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use iroh::endpoint::ConnectionError;
use keylink::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use keylink::{
    Admission, Admittance, Allowlist, Body, BodyReader, ConfigError, Context, DynamicAdmission,
    Error, Middleware, Next, Outcome, ProvenPeer, RemoteError, Rules,
};
use keylink::{Revocable, Server};

mod common;
use common::local_node;

keylink::service! {
    service Gate;

    rpc Knock() -> u64;
}

struct Gate;

impl gate::Handler for Gate {
    async fn knock(&self, _cx: &Context) -> u64 {
        1
    }
}

keylink::service! {
    service Methods;

    rpc Read() -> u64;
    rpc Write() -> u64;
}

struct Methods {
    writes: Arc<AtomicUsize>,
}

struct MethodsActor {
    writes: Arc<AtomicUsize>,
}

impl methods::Actor for MethodsActor {
    async fn read(&mut self, _cx: keylink::Caller<'_>) -> u64 {
        7
    }

    async fn write(&mut self, _cx: keylink::Caller<'_>) -> u64 {
        self.writes.fetch_add(1, Ordering::Relaxed);
        9
    }
}

impl methods::Handler for Methods {
    async fn read(&self, _cx: &Context) -> u64 {
        7
    }

    async fn write(&self, _cx: &Context) -> u64 {
        self.writes.fetch_add(1, Ordering::Relaxed);
        9
    }
}

fn rules_for(peer: keylink::EndpointId, allow: &[&str]) -> String {
    let allow = allow
        .iter()
        .map(|method| format!("{method:?}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[[peer]]\nid = \"{peer}\"\nallow = [{allow}]\n")
}

fn assert_application_close(error: &Error, code: u32, reason: &[u8]) {
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(current) = cause {
        if let Some(ConnectionError::ApplicationClosed(frame)) =
            current.downcast_ref::<ConnectionError>()
        {
            assert_eq!(frame.error_code, code.into());
            assert_eq!(frame.reason.as_ref(), reason);
            return;
        }
        cause = current.source();
    }
    panic!("expected an application close, got {error:?}");
}

struct CachedWrite {
    reached: Arc<AtomicUsize>,
    answer: u64,
}

impl Middleware<methods::Request, methods::Response> for CachedWrite {
    fn handle(
        &self,
        cx: Context,
        req: methods::Request,
        body: BodyReader,
        next: Next<methods::Request, methods::Response>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = (Outcome<methods::Response>, Body)> + Send>,
    > {
        if matches!(req, methods::Request::Write { .. }) {
            self.reached.fetch_add(1, Ordering::Relaxed);
            let answer = self.answer;
            return Box::pin(async move {
                (Outcome::Ok(methods::Response::Write(answer)), Body::empty())
            });
        }
        next(cx, req, body)
    }
}

struct TrackingAnyone {
    admitted: Arc<AtomicUsize>,
}

impl Admission for TrackingAnyone {
    fn admits(&self, _peer: &ProvenPeer) -> Admittance {
        Admittance::Admitted
    }

    fn admitted(&self, _peer: &keylink::AdmittedPeer, _live: keylink::Revocation) {
        self.admitted.fetch_add(1, Ordering::Relaxed);
    }
}

/// The second table for one peer is rejected before it can add permissions.
///
/// Search `            if allowed.contains_key(&id) {`.
///
/// Made red by `scripts/break-once.sh` replacing the condition with `if false`.
/// The second table compiled instead of returning `DuplicatePeer`.
#[test]
fn duplicate_peer_rules_are_rejected_in_source_order() {
    let peer = keylink::SecretKey::from_bytes(&[40; 32]).public();
    let raw = format!(
        "[[peer]]\nid = \"{peer}\"\nallow = [\"Read\"]\n\n\
         [[peer]]\nid = \"{peer}\"\nallow = [\"Write\"]\n"
    );
    let error = Rules::parse(&raw)
        .expect("valid TOML")
        .compile::<methods::Request>()
        .expect_err("a repeated peer must not union permissions");

    assert!(matches!(
        error,
        Error::Config(ConfigError::DuplicatePeer { id }) if id == peer
    ));
}

#[test]
fn duplicate_method_names_in_one_peer_rule_harmlessly_deduplicate() {
    let peer = keylink::SecretKey::from_bytes(&[40; 32]).public();
    Rules::parse(&rules_for(peer, &["Read", "Read"]))
        .expect("valid TOML")
        .compile::<methods::Request>()
        .expect("duplicate method names denote the same permission");
}

/// Blank input is not a deny-all policy, and parse diagnostics keep their cause.
///
/// Search `    peer: Vec<PeerRules>,` and
/// `        #[source] error: toml::de::Error,` (whitespace-collapsed).
///
/// Made red twice by `scripts/break-once.sh`: adding `#[serde(default)]` let the
/// blank file parse, and removing `#[source]` ended the error chain at
/// `ConfigError`.
#[test]
fn blank_rules_are_malformed_and_retain_the_toml_source() {
    use std::error::Error as _;

    let error = Rules::parse("").expect_err("a blank file must not compile");
    assert!(matches!(
        error,
        Error::Config(ConfigError::MalformedRules { .. })
    ));
    let config = error.source().expect("the configuration error is retained");
    let source = config.source().expect("the TOML parser error is retained");
    assert!(source.downcast_ref::<toml::de::Error>().is_some());
    assert!(source.to_string().contains("missing field `peer`"));
}

/// A syntactically valid empty collection is still invalid startup policy.
///
/// Search `        if self.peer.is_empty() {` and
/// `            return Err(ConfigError::EmptyRules.into());`.
///
/// Made red twice by `scripts/break-once.sh`: changing the condition to
/// `if false` accepted the empty policy, and replacing `EmptyRules` with
/// `EmptyAllowlist` returned the wrong configuration error.
#[test]
fn an_explicit_empty_peer_collection_is_rejected() {
    let error = Rules::parse("peer = []")
        .expect("valid TOML")
        .compile::<methods::Request>()
        .expect_err("an empty peer list must not compile");
    assert!(matches!(error, Error::Config(ConfigError::EmptyRules)));
}

/// Connection admission cannot be granted to a peer with no usable method.
///
/// Search `            if peer.allow.is_empty() {`.
///
/// Made red by `scripts/break-once.sh` changing the condition to `if false`.
#[test]
fn a_peer_with_no_methods_is_rejected() {
    let peer = keylink::SecretKey::from_bytes(&[40; 32]).public();
    let error = Rules::parse(&rules_for(peer, &[]))
        .expect("valid TOML")
        .compile::<methods::Request>()
        .expect_err("an admitted peer must have at least one method");
    assert!(matches!(
        error,
        Error::Config(ConfigError::EmptyPeerMethods { id }) if id == peer
    ));
}

/// A rule name must resolve to one of the generated static method names before
/// either policy can exist.
///
/// Search `                let Some(method) = Req::METHODS.iter().copied().find(|method| *method == named)`.
///
/// Made red with `scripts/break-once.sh`, replacing the name lookup with
/// `Req::METHODS.first().copied()`. The test reached `expect_err` with a compiled
/// policy mapping the misspelling to `Read`.
#[test]
fn an_unknown_rule_method_fails_during_compilation() {
    let peer = keylink::SecretKey::from_bytes(&[41; 32]).public();
    let error = Rules::parse(&rules_for(peer, &["Reed"]))
        .expect("valid TOML")
        .compile::<methods::Request>()
        .expect_err("a misspelled method must not compile");

    assert!(matches!(
        error,
        Error::Config(ConfigError::UnknownMethod {
            service: "Methods",
            named,
        }) if named == "Reed"
    ));
}

/// Required authorization stays outside ordinary layers on both sides of install.
///
/// Search `        for layer in self.required_layers.into_iter().rev() {`.
///
/// Made red by `scripts/break-once.sh` adding `.take(0)` to that iterator. The
/// closure-installed cache returned `777` without authorization or dispatch.
#[tokio::test]
async fn compiled_authorization_wraps_short_circuits_added_before_and_after_install() {
    let node = local_node().await;
    let peer = local_node().await;
    let writes = Arc::new(AtomicUsize::new(0));
    let inside = Arc::new(AtomicUsize::new(0));
    let after = Arc::new(AtomicUsize::new(0));
    let compiled = Rules::parse(&rules_for(peer.id(), &["Read"]))
        .expect("valid TOML")
        .compile::<methods::Request>()
        .expect("known methods compile");
    let service = compiled
        .install(|allow| {
            methods::service(
                Methods {
                    writes: writes.clone(),
                },
                allow,
            )
            .layer(CachedWrite {
                reached: inside.clone(),
                answer: 777,
            })
        })
        .layer(CachedWrite {
            reached: after.clone(),
            answer: 888,
        })
        .build();
    let _running = Server::builder(&node)
        .service(service)
        .spawn()
        .expect("serves");

    let client = methods::Client::connect(&peer, node.endpoint().addr())
        .await
        .expect("dials");
    let refused = client
        .write()
        .await
        .expect_err("ordinary middleware cannot bypass compiled authorization");
    assert!(matches!(
        refused,
        Error::Remote(RemoteError::Unauthorized(ref method)) if method == "Write"
    ));
    assert_eq!(inside.load(Ordering::Relaxed), 0, "inside layer ran");
    assert_eq!(after.load(Ordering::Relaxed), 0, "later layer ran");
    assert_eq!(writes.load(Ordering::Relaxed), 0, "handler ran");
}

/// A required-policy refusal keeps its wire-level diagnostic after intersection.
///
/// Search `            connection.close(REFUSED.into(), b"unauthorized");`.
/// Made red with `scripts/break-once.sh` by changing that close to the unavailable
/// code and reason. The peer observed the wrong application-close frame.
#[tokio::test]
async fn post_install_anyone_admission_does_not_widen_compiled_peers() {
    let node = local_node().await;
    let listed = local_node().await;
    let stranger = local_node().await;
    let compiled = Rules::parse(&rules_for(listed.id(), &["Read"]))
        .expect("valid TOML")
        .compile::<methods::Request>()
        .expect("known methods compile");
    let service = compiled
        .install(|allow| {
            methods::service(
                Methods {
                    writes: Arc::new(AtomicUsize::new(0)),
                },
                allow,
            )
        })
        .admission(Arc::new(Allowlist::anyone()))
        .build();
    let _running = Server::builder(&node)
        .service(service)
        .spawn()
        .expect("serves");

    let outside = methods::Client::connect(&stranger, node.endpoint().addr())
        .await
        .expect("the handshake completes before admission");
    let denied = outside
        .read()
        .await
        .expect_err("ordinary admission cannot replace the compiled peer policy");
    assert_application_close(&denied, 1, b"unauthorized");
}

#[tokio::test]
async fn nested_compiled_installs_intersect_every_required_peer_policy() {
    let node = local_node().await;
    let common = local_node().await;
    let outer_only = local_node().await;
    let outer_rules = format!(
        "{}{}",
        rules_for(common.id(), &["Read"]),
        rules_for(outer_only.id(), &["Read"]),
    );
    let outer = Rules::parse(&outer_rules)
        .expect("valid TOML")
        .compile::<methods::Request>()
        .expect("known methods compile");
    let inner = Rules::parse(&rules_for(common.id(), &["Read"]))
        .expect("valid TOML")
        .compile::<methods::Request>()
        .expect("known methods compile");
    let service = outer
        .install(|outer_allow| {
            inner.install(|_inner_allow| {
                methods::service(
                    Methods {
                        writes: Arc::new(AtomicUsize::new(0)),
                    },
                    outer_allow,
                )
            })
        })
        .build();
    let _running = Server::builder(&node)
        .service(service)
        .spawn()
        .expect("serves");

    let allowed = methods::Client::connect(&common, node.endpoint().addr())
        .await
        .expect("dials");
    assert_eq!(allowed.read().await.expect("every policy admits"), 7);

    let outside = methods::Client::connect(&outer_only, node.endpoint().addr())
        .await
        .expect("the handshake completes before admission");
    let denied = outside
        .read()
        .await
        .expect_err("one required refusal denies the peer");
    assert!(matches!(denied, Error::Transport(_)), "got {denied:?}");
}

/// Required peer policy is intersected before the caller policy or its callback.
///
/// Search `    for policy in required { require_admitted(policy.admits(&proven), connection)?; }`
/// (whitespace-collapsed).
///
/// Made red by `scripts/break-once.sh` adding `.iter().take(0)` to that loop. The
/// stranger reached the caller policy and its `admitted` callback.
#[tokio::test]
async fn later_admission_cannot_replace_compiled_peer_admission() {
    let node = local_node().await;
    let peer = local_node().await;
    let stranger = local_node().await;
    let admitted = Arc::new(AtomicUsize::new(0));
    let compiled = Rules::parse(&rules_for(peer.id(), &["Read"]))
        .expect("valid TOML")
        .compile::<methods::Request>()
        .expect("known methods compile");
    let service = compiled
        .install(|allow| {
            methods::service(
                Methods {
                    writes: Arc::new(AtomicUsize::new(0)),
                },
                allow,
            )
        })
        .admission(Arc::new(TrackingAnyone {
            admitted: admitted.clone(),
        }))
        .build();
    let _running = Server::builder(&node)
        .service(service)
        .spawn()
        .expect("serves");

    let allowed = methods::Client::connect(&peer, node.endpoint().addr())
        .await
        .expect("dials");
    assert_eq!(allowed.read().await.expect("both policies admit"), 7);
    assert_eq!(admitted.load(Ordering::Relaxed), 1);

    let outside = methods::Client::connect(&stranger, node.endpoint().addr())
        .await
        .expect("the handshake completes before admission");
    let denied = outside
        .read()
        .await
        .expect_err("the compiled peer policy remains required");
    assert!(matches!(denied, Error::Transport(_)), "got {denied:?}");
    assert_eq!(
        admitted.load(Ordering::Relaxed),
        1,
        "caller admission callback ran for a peer a required policy refused"
    );
}

/// One compiled value installs two separate decisions: connection admission and
/// typed per-method middleware authorization.
///
/// Search `            .is_some_and(|methods| methods.contains(method))`,
/// `        let allow = Allowlist::only(allowed.keys().copied());`, and
/// `            .require_authorization(self.authorization)`.
///
/// Made red three ways with `scripts/break-once.sh`:
///
/// - changing the method predicate to `true` let `Write` dispatch and return `9`;
/// - changing the compiled allowlist to `Allowlist::anyone()` returned
///   `Remote(Unauthorized("Read"))` where the test requires a transport close;
/// - replacing `.require_authorization(self.authorization)` with the empty
///   string `""` let `Write` dispatch and return `9`.
#[tokio::test]
async fn compiled_rules_admit_the_peer_once_and_authorize_each_method() {
    let node = local_node().await;
    let peer = local_node().await;
    let stranger = local_node().await;
    let writes = Arc::new(AtomicUsize::new(0));
    let compiled = Rules::parse(&rules_for(peer.id(), &["Read"]))
        .expect("valid TOML")
        .compile::<methods::Request>()
        .expect("known methods compile");
    let service = compiled
        .install(|allow| {
            methods::service(
                Methods {
                    writes: writes.clone(),
                },
                allow,
            )
        })
        .build();
    let _running = Server::builder(&node)
        .service(service)
        .spawn()
        .expect("serves");

    let allowed = methods::Client::connect(&peer, node.endpoint().addr())
        .await
        .expect("dials");
    assert_eq!(allowed.read().await.expect("authorized"), 7);
    let refused = allowed
        .write()
        .await
        .expect_err("the admitted peer is not authorized for Write");
    assert!(matches!(
        refused,
        Error::Remote(RemoteError::Unauthorized(ref method)) if method == "Write"
    ));
    assert_eq!(
        writes.load(Ordering::Relaxed),
        0,
        "the handler must not run"
    );

    let outside = methods::Client::connect(&stranger, node.endpoint().addr())
        .await
        .expect("the handshake completes before admission");
    let denied = outside
        .read()
        .await
        .expect_err("an unlisted peer is not admitted");
    assert!(
        matches!(denied, Error::Transport(_)),
        "peer admission must close the connection before method authorization: {denied:?}"
    );
}

/// Method authorization runs before a same-peer idempotency replay.
///
/// A peer first records `Read` under one key. Reusing that key for denied
/// `Write` must not reveal or replay the earlier response.
///
/// Search `    for policy in required_authorization.iter() {`.
///
/// Made red by `scripts/break-once.sh` replacing it with
/// `    for policy in required_authorization.iter().take(0) {`. The peer got
/// `UnexpectedResponse { expected: "Write", actual: "Read" }`.
#[tokio::test]
async fn denied_method_cannot_replay_an_allowed_method_under_the_same_key() {
    let node = local_node().await;
    let peer = local_node().await;
    let writes = Arc::new(AtomicUsize::new(0));
    let compiled = Rules::parse(&rules_for(peer.id(), &["Read"]))
        .expect("valid TOML")
        .compile::<methods::Request>()
        .expect("known methods compile");
    let service = compiled
        .install(|allow| {
            methods::service(
                Methods {
                    writes: writes.clone(),
                },
                allow,
            )
        })
        .build();
    let _running = Server::builder(&node)
        .service(service)
        .spawn()
        .expect("serves");

    let client = methods::Client::connect(&peer, node.endpoint().addr())
        .await
        .expect("dials");
    let key = keylink::IdempotencyKey::new("authorization-before-replay").expect("a valid key");
    assert_eq!(
        client
            .with_idempotency(key.clone())
            .read()
            .await
            .expect("Read is authorized"),
        7,
    );

    let refused = client
        .with_idempotency(key)
        .write()
        .await
        .expect_err("Write is denied before the recorded Read can replay");
    assert!(
        matches!(
            refused,
            Error::Remote(RemoteError::Unauthorized(ref method)) if method == "Write"
        ),
        "got {refused:?}"
    );
    assert_eq!(
        writes.load(Ordering::Relaxed),
        0,
        "the handler must not run"
    );
}

/// Every nested required method policy runs before a same-peer replay.
///
/// The first policy permits both methods, while the second permits only `Read`.
/// Reusing a recorded `Read` key for `Write` must reach the second refusal first.
///
/// Search `    for policy in required_authorization.iter() {`.
///
/// Made red with `scripts/break-once.sh` by replacing it with
/// `    for policy in required_authorization.iter().take(1) {`. The peer got
/// `UnexpectedResponse { expected: "Write", actual: "Read" }`.
#[tokio::test]
async fn every_nested_method_policy_precedes_same_peer_replay() {
    let node = local_node().await;
    let peer = local_node().await;
    let writes = Arc::new(AtomicUsize::new(0));
    let first = Rules::parse(&rules_for(peer.id(), &["Read", "Write"]))
        .expect("valid TOML")
        .compile::<methods::Request>()
        .expect("known methods compile");
    let second = Rules::parse(&rules_for(peer.id(), &["Read"]))
        .expect("valid TOML")
        .compile::<methods::Request>()
        .expect("known methods compile");
    let service = second
        .install(|second_allow| {
            first.install(|_first_allow| {
                methods::service(
                    Methods {
                        writes: writes.clone(),
                    },
                    second_allow,
                )
            })
        })
        .build();
    let _running = Server::builder(&node)
        .service(service)
        .spawn()
        .expect("serves");

    let client = methods::Client::connect(&peer, node.endpoint().addr())
        .await
        .expect("dials");
    let key =
        keylink::IdempotencyKey::new("nested-authorization-before-replay").expect("a valid key");
    assert_eq!(
        client
            .with_idempotency(key.clone())
            .read()
            .await
            .expect("both policies authorize Read"),
        7,
    );

    let refused = client
        .with_idempotency(key)
        .write()
        .await
        .expect_err("the second policy denies Write before Read can replay");
    assert!(
        matches!(
            refused,
            Error::Remote(RemoteError::Unauthorized(ref method)) if method == "Write"
        ),
        "got {refused:?}"
    );
    assert_eq!(
        writes.load(Ordering::Relaxed),
        0,
        "the handler must not run"
    );
}

/// Authorization also precedes the idempotency policy lookup.
///
/// `Keyable::None` would return `NotKeyable` if the ledger policy ran first.
/// The required policy must instead return the exact method denial.
///
/// Search `    for policy in required_authorization.iter() {`.
///
/// Made red by `scripts/break-once.sh` replacing it with
/// `    for policy in required_authorization.iter().take(0) {`. The peer got
/// `Remote(Idempotency(NotKeyable { method: "Write" }))`.
#[tokio::test]
async fn denied_method_does_not_reach_the_idempotency_policy() {
    let node = local_node().await;
    let peer = local_node().await;
    let compiled = Rules::parse(&rules_for(peer.id(), &["Read"]))
        .expect("valid TOML")
        .compile::<methods::Request>()
        .expect("known methods compile");
    let service = compiled
        .install(|allow| {
            methods::service(
                Methods {
                    writes: Arc::new(AtomicUsize::new(0)),
                },
                allow,
            )
            .keyable(keylink::Keyable::None)
        })
        .build();
    let _running = Server::builder(&node)
        .service(service)
        .spawn()
        .expect("serves");

    let client = methods::Client::connect(&peer, node.endpoint().addr())
        .await
        .expect("dials");
    let key = keylink::IdempotencyKey::new("authorization-before-key-policy").expect("a valid key");
    let refused = client
        .with_idempotency(key)
        .write()
        .await
        .expect_err("Write is denied before key policy is consulted");
    assert!(
        matches!(
            refused,
            Error::Remote(RemoteError::Unauthorized(ref method)) if method == "Write"
        ),
        "got {refused:?}"
    );
}

#[tokio::test]
async fn compiled_authorization_applies_to_addressed_requests() {
    let node = local_node().await;
    let peer = local_node().await;
    let writes = Arc::new(AtomicUsize::new(0));
    let system = methods::Registry::builder(keylink::Eviction::Never).build();
    let actor = keylink::ActorId::new("methods").expect("valid actor id");
    system
        .mount(
            actor.clone(),
            methods::spawn(MethodsActor {
                writes: writes.clone(),
            }),
        )
        .expect("the actor id is free");

    let compiled = Rules::parse(&rules_for(peer.id(), &["Read"]))
        .expect("valid TOML")
        .compile::<methods::Request>()
        .expect("known methods compile");
    let service = compiled.install(|allow| system.service(allow)).build();
    let _running = Server::builder(&node)
        .service(service)
        .spawn()
        .expect("serves");

    let client = methods::RegistryClient::connect(&peer, node.endpoint().addr())
        .await
        .expect("dials");
    let addressed = client.at(actor);
    assert_eq!(addressed.read().await.expect("authorized"), 7);
    let refused = addressed
        .write()
        .await
        .expect_err("Write is not authorized for the addressed request");
    assert!(matches!(
        refused,
        Error::Remote(RemoteError::Unauthorized(ref method)) if method == "Write"
    ));
    assert_eq!(writes.load(Ordering::Relaxed), 0, "the actor must not run");
}

/// A policy whose backend is down. Every answer is `Unavailable`, which refuses
/// — and stays distinguishable from `Refused`, which is the whole point.
struct Unreachable;

impl Admission for Unreachable {
    fn admits(&self, _peer: &ProvenPeer) -> Admittance {
        Admittance::Unavailable("the policy service did not answer".into())
    }
}

/// A peer added while the service is running is admitted on its next dial, with
/// no restart and without disturbing anybody already connected.
#[tokio::test]
async fn a_newly_added_peer_is_admitted_without_a_restart() {
    let node = local_node().await;
    let welcome = local_node().await;
    let latecomer = local_node().await;

    let policy = Arc::new(DynamicAdmission::new(Allowlist::only([welcome.id()])));
    let _running = Server::builder(&node)
        .service(
            gate::service(Gate, Allowlist::anyone())
                .admission(policy.clone())
                .build(),
        )
        .spawn()
        .expect("serves");

    let inside = gate::Client::connect(&welcome, node.endpoint().addr())
        .await
        .expect("dials");
    assert_eq!(inside.knock().await.expect("answers"), 1);

    let outside = gate::Client::connect(&latecomer, node.endpoint().addr())
        .await
        .expect("the handshake completes; admission is one layer up");
    assert!(
        outside.knock().await.is_err(),
        "a peer not on the list must not be answered"
    );

    let closed = policy.replace(Allowlist::only([welcome.id(), latecomer.id()]));
    assert!(
        closed.is_empty(),
        "widening a policy closes nobody: {closed:?}"
    );

    let admitted = gate::Client::connect(&latecomer, node.endpoint().addr())
        .await
        .expect("dials");
    assert_eq!(
        admitted.knock().await.expect("answers"),
        1,
        "the new peer must be admitted without a restart"
    );

    // And the peer that was already inside was never disturbed.
    assert_eq!(inside.knock().await.expect("still answers"), 1);
}

/// Revocation reaches a connection that is already open, which is the
/// capability a plain [`Admission`] cannot have: it is consulted at accept and
/// never again, so without this a revoked peer keeps its connection until it
/// chooses to hang up.
#[tokio::test]
async fn revoking_a_peer_closes_its_connection() {
    let node = local_node().await;
    let peer = local_node().await;
    let keeper = local_node().await;

    let policy = Arc::new(DynamicAdmission::new(Allowlist::only([
        peer.id(),
        keeper.id(),
    ])));
    let _running = Server::builder(&node)
        .service(
            gate::service(Gate, Allowlist::anyone())
                .admission(policy.clone())
                .build(),
        )
        .spawn()
        .expect("serves");

    let revoked = gate::Client::connect(&peer, node.endpoint().addr())
        .await
        .expect("dials");
    let kept = gate::Client::connect(&keeper, node.endpoint().addr())
        .await
        .expect("dials");
    assert_eq!(revoked.knock().await.expect("answers"), 1);
    assert_eq!(kept.knock().await.expect("answers"), 1);

    let closed = policy.replace(Allowlist::only([keeper.id()]));
    assert_eq!(closed, vec![peer.id()], "exactly one peer was revoked");

    let error = revoked
        .knock()
        .await
        .expect_err("a revoked peer's live connection must be closed");
    assert!(matches!(error, Error::Transport(_)), "got {error:?}");

    assert_eq!(
        kept.knock().await.expect("answers"),
        1,
        "revoking one peer must not disturb another"
    );
}

/// The ordering is the correctness property: the policy is swapped **before**
/// anything is closed. Closing first leaves a window in which a revoked peer
/// reconnects and is admitted by the policy on its way out — and a window that
/// small is one nobody would ever reproduce from a bug report.
///
/// Proven from the outside, which is the only place it is observable: the
/// instant `replace` returns, a fresh dial by the revoked peer is already
/// refused. If the swap happened after the close, this dial would race it.
#[tokio::test]
async fn a_revoked_peer_cannot_reconnect_during_the_revocation() {
    let node = local_node().await;
    let peer = local_node().await;

    let policy = Arc::new(DynamicAdmission::new(Allowlist::only([peer.id()])));
    let _running = Server::builder(&node)
        .service(
            gate::service(Gate, Allowlist::anyone())
                .admission(policy.clone())
                .build(),
        )
        .spawn()
        .expect("serves");

    let first = gate::Client::connect(&peer, node.endpoint().addr())
        .await
        .expect("dials");
    assert_eq!(first.knock().await.expect("answers"), 1);

    let closed = policy.replace(Allowlist::only([node.id()]));
    assert_eq!(closed, vec![peer.id()]);

    let again = gate::Client::connect(&peer, node.endpoint().addr())
        .await
        .expect("the handshake still completes");
    assert!(
        again.knock().await.is_err(),
        "the new policy must already be in force when replace returns"
    );
}

/// A policy that cannot be consulted refuses, and refuses **distinguishably**.
/// A backend that answered `false` would be byte-identical to "not on the
/// list", and an operator who cannot tell a dead authorizer from an unlisted
/// peer reaches for `Allowlist::anyone()` during an incident — a fail-open
/// arrived at through a diagnostic hole.
///
/// Search `            connection.close(UNAVAILABLE.into(), b"admission unavailable");`.
/// Made red with `scripts/break-once.sh` by changing that reason to
/// `"unauthorized"`. The peer observed the wrong application-close frame.
#[tokio::test]
async fn a_policy_that_cannot_be_consulted_refuses() {
    let node = local_node().await;
    let peer = local_node().await;

    let _running = Server::builder(&node)
        .service(
            gate::service(Gate, Allowlist::anyone())
                .admission(Arc::new(Unreachable))
                .build(),
        )
        .spawn()
        .expect("serves");

    let client = gate::Client::connect(&peer, node.endpoint().addr())
        .await
        .expect("the handshake completes");
    let error = client
        .knock()
        .await
        .expect_err("an unreachable policy admits nobody");
    assert_application_close(&error, 2, b"admission unavailable");
}

/// The registry forgets a peer that leaves on its own, so a later revocation
/// does not report closing a connection that was already gone.
#[tokio::test]
async fn a_departed_peer_is_not_reported_as_revoked() {
    let node = local_node().await;
    let peer = local_node().await;

    let policy = Arc::new(DynamicAdmission::new(Allowlist::only([peer.id()])));
    let _running = Server::builder(&node)
        .service(
            gate::service(Gate, Allowlist::anyone())
                .admission(policy.clone())
                .build(),
        )
        .spawn()
        .expect("serves");

    let client = gate::Client::connect(&peer, node.endpoint().addr())
        .await
        .expect("dials");
    assert_eq!(client.knock().await.expect("answers"), 1);
    assert_eq!(policy.live(), 1);

    client.close().await.expect("closes");
    for _ in 0..100 {
        if policy.live() == 0 {
            break;
        }
        n0_future::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(policy.live(), 0, "a departed peer must be forgotten");

    assert!(
        policy.replace(Allowlist::only([node.id()])).is_empty(),
        "revoking a peer that already left closes nothing"
    );
}

/// `Allowlist` is the canonical policy, not a legacy path: a service built the
/// ordinary way admits through the same port.
#[tokio::test]
async fn an_allowlist_is_itself_an_admission() {
    let node = local_node().await;
    let stranger = local_node().await;

    let _running = gate::serve(&node, Gate, Allowlist::only([node.id()])).expect("serves");

    let client = gate::Client::connect(&stranger, node.endpoint().addr())
        .await
        .expect("the handshake completes");
    assert!(
        client.knock().await.is_err(),
        "the fixed list must still fail closed"
    );
}
