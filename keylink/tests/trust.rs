//! Delegated authority across real loopback connections, no discovery or relay.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use keylink::prelude::*;
use keylink::trust::{
    self, Access, AccessError, Call, Caveats, Certificate, Chain, ChainError, Change, Delegation,
    Pattern, Presented, Revocation, Roots, Scope, Timestamp, Trust, Validity, WithinCaveats,
};
use keylink::{Context, Error, Node, Refusal, RemoteError, Revocable, SecretKey, Server};
use std::borrow::Cow;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

keylink::service! {
    service Methods;
    rpc Read() -> u64;
}
struct Methods;
impl methods::Handler for Methods {
    async fn read(&self, cx: &Context) -> u64 {
        assert!(cx.credential().is_some());
        7
    }
}
struct Counted(Arc<AtomicUsize>);
impl methods::Actor for Counted {
    async fn read(&mut self, _cx: keylink::Caller<'_>) -> u64 {
        self.0.fetch_add(1, Ordering::SeqCst);
        7
    }
}
fn grant(subject: keylink::EndpointId, resource: &str, delegate: u8) -> Delegation {
    Delegation {
        subject,
        caveats: Caveats {
            resources: Scope::only([Pattern::under(resource).unwrap()]).unwrap(),
            operations: Scope::any(),
            validity: Validity::new(
                Timestamp::from_unix_secs(0),
                Timestamp::from_unix_secs(u64::MAX),
            )
            .unwrap(),
        },
        delegate,
        serial: 0,
    }
}
fn fixture() -> (SecretKey, SecretKey, Chain, Trust) {
    let root = SecretKey::from_bytes(&[1; 32]);
    let user = SecretKey::from_bytes(&[2; 32]);
    let device = SecretKey::from_bytes(&[3; 32]);
    let first = Certificate::issue(&root, grant(user.public(), "repo", 1)).unwrap();
    let chain = Chain::new(vec![first])
        .delegate(&user, grant(device.public(), "repo/a", 0))
        .unwrap();
    let trust = Trust::new(Roots::only([root.public()]).unwrap());
    (root, device, chain, trust)
}
fn resource(_: &methods::Request) -> Option<Cow<'_, str>> {
    Some(Cow::Borrowed("repo/a"))
}
fn denied(error: Error) {
    assert!(
        matches!(error, Error::Remote(RemoteError::Unauthorized(ref method)) if method == "Read"),
        "{error:?}"
    );
}

// Made red with scripts/break-once.sh by moving Trust::apply's store-mutation
// block after its live sweep (a multi-line search edit, described not quoted).
// The replacement is `sweep + mutation`, rather than `mutation + sweep`.
// Watched: apply returned no closed peers instead of the device key.
#[tokio::test]
async fn a_revoked_certificate_is_refused_and_its_live_connection_closed() {
    let (root, device, chain, trust) = fixture();
    let node = Node::bind_local().await.unwrap();
    let peer = Node::bind_local_with_secret(device).await.unwrap();
    trust
        .insert(Chain::from_bytes(&chain.to_bytes()).unwrap())
        .unwrap();
    let service = trust
        .guard::<methods::Request, _>(WithinCaveats)
        .resource(resource)
        .install(|allow| methods::service(Methods, allow))
        .build();
    let _running = Server::builder(&node).service(service).spawn().unwrap();
    let client = methods::Client::connect(&peer, node.endpoint().addr())
        .await
        .unwrap();
    assert_eq!(client.read().await.unwrap(), 7);
    let watch = client.watch();
    let revocation =
        Revocation::from_bytes(&Revocation::issue(&root, chain.links()[0].id()).to_bytes())
            .unwrap();
    assert_eq!(
        trust.apply(Change::Revoke(revocation)).unwrap(),
        vec![peer.id()]
    );
    let _ = watch.await;
    assert_eq!(
        client.read().await.unwrap_err().refusal(),
        Some(Refusal::Denied)
    );
    let again = methods::Client::connect(&peer, node.endpoint().addr())
        .await
        .unwrap();
    assert_eq!(
        again.read().await.unwrap_err().refusal(),
        Some(Refusal::Denied)
    );
}

#[tokio::test]
async fn a_peer_presents_its_own_chain_and_is_admitted() {
    let (_, device, chain, trust) = fixture();
    let node = Node::bind_local().await.unwrap();
    let peer = Node::bind_local_with_secret(device).await.unwrap();
    let service = trust
        .guard::<methods::Request, _>(WithinCaveats)
        .resource(resource)
        .install(|allow| methods::service(Methods, allow))
        .build();
    let _running = Server::builder(&node)
        .service(service)
        .service(trust.presentation().build())
        .spawn()
        .unwrap();
    assert!(matches!(
        trust::present(&peer, node.endpoint().addr(), &chain)
            .await
            .unwrap(),
        Presented::Held { .. }
    ));
    let client = methods::Client::connect(&peer, node.endpoint().addr())
        .await
        .unwrap();
    assert_eq!(client.read().await.unwrap(), 7);
    let user_chain = Chain::new(vec![chain.links()[0].clone()]);
    assert_eq!(
        trust::present(&peer, node.endpoint().addr(), &user_chain)
            .await
            .unwrap(),
        Presented::Refused(ChainError::NotTheHolder)
    );
}

#[tokio::test]
async fn a_call_outside_the_caveat_is_refused_while_one_inside_succeeds() {
    let (_, device, chain, trust) = fixture();
    let node = Node::bind_local().await.unwrap();
    let peer = Node::bind_local_with_secret(device).await.unwrap();
    trust.insert(chain).unwrap();
    let registry = methods::Registry::builder(keylink::Eviction::Never).build();
    let b = Arc::new(AtomicUsize::new(0));
    registry
        .mount(
            keylink::ActorId::new("repo/a").unwrap(),
            methods::spawn(Counted(Arc::new(AtomicUsize::new(0)))),
        )
        .unwrap();
    registry
        .mount(
            keylink::ActorId::new("repo/b").unwrap(),
            methods::spawn(Counted(b.clone())),
        )
        .unwrap();
    let service = trust
        .guard::<methods::Request, _>(WithinCaveats)
        .install(|allow| registry.service(allow))
        .build();
    let _running = Server::builder(&node).service(service).spawn().unwrap();
    let client = methods::RegistryClient::connect(&peer, node.endpoint().addr())
        .await
        .unwrap();
    assert_eq!(
        client
            .at(keylink::ActorId::new("repo/a").unwrap())
            .read()
            .await
            .unwrap(),
        7
    );
    denied(
        client
            .at(keylink::ActorId::new("repo/b").unwrap())
            .read()
            .await
            .unwrap_err(),
    );
    denied(
        client
            .at(keylink::ActorId::new("repo/zzz").unwrap())
            .read()
            .await
            .unwrap_err(),
    );
    assert_eq!(b.load(Ordering::SeqCst), 0);
}

keylink::service! {
    service Files;
    rpc Read(name: String) -> u64;
    tell Write(name: String) -> ();
}
struct Files(Arc<AtomicUsize>);
impl files::Handler for Files {
    async fn read(&self, _cx: &Context, _name: String) -> u64 {
        7
    }
    async fn write(&self, _cx: &Context, _name: String) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn file_resource(request: &files::Request) -> Option<Cow<'_, str>> {
    match request {
        files::Request::Read { name } | files::Request::Write { name } => Some(Cow::Borrowed(name)),
        files::Request::__Status {} => None,
    }
}
#[tokio::test]
async fn singleton_resources_are_checked_before_the_idempotency_policy() {
    let (_, device, chain, trust) = fixture();
    let node = Node::bind_local().await.unwrap();
    let peer = Node::bind_local_with_secret(device).await.unwrap();
    trust.insert(chain).unwrap();
    let service = trust
        .guard::<files::Request, _>(WithinCaveats)
        .resource(file_resource)
        .install(|allow| files::service(Files(Arc::new(AtomicUsize::new(0))), allow))
        .build();
    let _running = Server::builder(&node).service(service).spawn().unwrap();
    let client = files::Client::connect(&peer, node.endpoint().addr())
        .await
        .unwrap();
    assert_eq!(client.read("repo/a/one".into()).await.unwrap(), 7);
    denied(client.read("repo/b/one".into()).await.unwrap_err());
    let keyed = client.with_idempotency(keylink::IdempotencyKey::new("outside").unwrap());
    denied(keyed.read("repo/b/one".into()).await.unwrap_err());
}
struct Unavailable;
impl Access for Unavailable {
    async fn authorize(&self, _call: Call<'_>) -> Result<(), AccessError> {
        Err(AccessError::Unavailable)
    }
}
#[tokio::test]
async fn an_unavailable_policy_is_not_a_forbidden_one() {
    let (_, device, chain, trust) = fixture();
    let node = Node::bind_local().await.unwrap();
    let peer = Node::bind_local_with_secret(device).await.unwrap();
    trust.insert(chain).unwrap();
    let service = trust
        .guard::<methods::Request, _>(Unavailable)
        .resource(resource)
        .install(|allow| methods::service(Methods, allow))
        .build();
    let _running = Server::builder(&node).service(service).spawn().unwrap();
    let client = methods::Client::connect(&peer, node.endpoint().addr())
        .await
        .unwrap();
    assert!(matches!(
        client.read().await.unwrap_err(),
        Error::Remote(RemoteError::AccessUnavailable)
    ));
}

// The payload's drop is a completion barrier even when preflight refuses it.
// A bi-stream Count would race an independent tell stream and could pass stale.
static TELL_DROPPED: std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<()>>> =
    std::sync::Mutex::new(None);
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, postcard_schema::Schema)]
pub struct Tracked {
    name: String,
    #[serde(skip)]
    local: bool,
}
impl Drop for Tracked {
    fn drop(&mut self) {
        if !self.local {
            if let Some(sender) = TELL_DROPPED.lock().unwrap().as_ref() {
                let _ = sender.send(());
            }
        }
    }
}
keylink::service! {
    service TellProbe;
    tell Write(payload: Tracked) -> ();
}
struct TellProbe(Arc<AtomicUsize>);
impl tell_probe::Actor for TellProbe {
    async fn write(&mut self, _cx: keylink::Caller<'_>, _payload: Tracked) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn tracked_resource(request: &tell_probe::Request) -> Option<Cow<'_, str>> {
    match request {
        tell_probe::Request::Write { payload } => Some(Cow::Borrowed(&payload.name)),
        tell_probe::Request::__Status {} => None,
    }
}
// Search `    let Ok(credential) = preflight(&required, peer.id(), &message).await else {`.
// Made red with scripts/break-once.sh replacing the preflight expression with
// `Ok::<Option<Arc<crate::trust::Credential>>, RemoteError>(None)`.
// Payload drop proved completion; the forbidden tell incremented (1 instead of 0).
#[tokio::test]
async fn a_tell_outside_the_caveat_never_reaches_the_actor() {
    let (_, device, chain, trust) = fixture();
    let node = Node::bind_local().await.unwrap();
    let peer = Node::bind_local_with_secret(device).await.unwrap();
    trust.insert(chain).unwrap();
    let writes = Arc::new(AtomicUsize::new(0));
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    *TELL_DROPPED.lock().unwrap() = Some(sent);
    let registry = tell_probe::Registry::builder(keylink::Eviction::Never).build();
    let actor = keylink::ActorId::new("repo/b").unwrap();
    registry
        .mount(actor.clone(), tell_probe::spawn(TellProbe(writes.clone())))
        .unwrap();
    let service = trust
        .guard::<tell_probe::Request, _>(WithinCaveats)
        .resource(tracked_resource)
        .install(|allow| registry.service(allow))
        .build();
    let _running = Server::builder(&node).service(service).spawn().unwrap();
    // Generated keyed clients deliberately have no tell methods: registry
    // refusals need a reply. A raw peer can still send one, so guard that path.
    let client = keylink::RpcClient::<keylink::Addressed<tell_probe::Request>, tell_probe::Response>::connect(
        &peer, node.endpoint().addr(), tell_probe::registry_alpn(),
    ).await.unwrap();
    let _proof = client
        .tell(keylink::Addressed::new(
            actor,
            tell_probe::Request::Write {
                payload: Tracked {
                    name: "file".into(),
                    local: true,
                },
            },
        ))
        .await
        .unwrap();
    received.recv().await.unwrap();
    assert_eq!(writes.load(Ordering::SeqCst), 0);
    *TELL_DROPPED.lock().unwrap() = None;
}

keylink::service! { service Ungated; rpc Read() -> u64; }
struct Ungated;
impl ungated::Handler for Ungated {
    async fn read(&self, _cx: &Context) -> u64 {
        7
    }
}
#[tokio::test]
async fn revocation_closes_every_connection_across_two_alpns() {
    let (root, device, chain, trust) = fixture();
    let node = Node::bind_local().await.unwrap();
    let peer = Node::bind_local_with_secret(device).await.unwrap();
    trust.insert(chain.clone()).unwrap();
    // Admission alone must close both: no per-call Guard can hide a stale entry.
    let _running = Server::builder(&node)
        .service(
            files::service(
                Files(Arc::new(AtomicUsize::new(0))),
                keylink::Allowlist::anyone(),
            )
            .admission(Arc::new(trust.clone()))
            .build(),
        )
        .service(
            ungated::service(Ungated, keylink::Allowlist::anyone())
                .admission(Arc::new(trust.clone()))
                .build(),
        )
        .spawn()
        .unwrap();
    let a = files::Client::connect(&peer, node.endpoint().addr())
        .await
        .unwrap();
    let b = ungated::Client::connect(&peer, node.endpoint().addr())
        .await
        .unwrap();
    assert_eq!(a.read("repo/a".into()).await.unwrap(), 7);
    assert_eq!(b.read().await.unwrap(), 7);
    assert_eq!(trust.live(), 2);
    let wa = a.watch();
    let wb = b.watch();
    assert_eq!(
        trust
            .apply(Change::Revoke(Revocation::issue(
                &root,
                chain.links()[0].id()
            )))
            .unwrap(),
        vec![peer.id()]
    );
    let _ = tokio::join!(wa, wb);
    assert_eq!(trust.live(), 0);
    assert_eq!(
        a.read("repo/a".into()).await.unwrap_err().refusal(),
        Some(Refusal::Denied)
    );
    assert_eq!(b.read().await.unwrap_err().refusal(), Some(Refusal::Denied));
}

struct PolicyBarrier {
    entered: tokio::sync::mpsc::UnboundedSender<()>,
    release: Arc<tokio::sync::Notify>,
}
impl Access for PolicyBarrier {
    async fn authorize(&self, _call: Call<'_>) -> Result<(), AccessError> {
        let ready = self.release.notified();
        let _ = self.entered.send(());
        ready.await;
        Ok(())
    }
}
#[tokio::test]
async fn revoking_the_selected_chain_during_access_refuses_dispatch() {
    let (root, device, _, trust) = fixture();
    let selected = Chain::new(vec![Certificate::issue(
        &root,
        grant(device.public(), "repo/a", 0),
    )
    .unwrap()]);
    let alternative = Chain::new(vec![Certificate::issue(
        &root,
        grant(device.public(), "repo/b", 0),
    )
    .unwrap()]);
    trust.insert(selected.clone()).unwrap();
    trust.insert(alternative).unwrap();
    let node = Node::bind_local().await.unwrap();
    let peer = Node::bind_local_with_secret(device).await.unwrap();
    let (entered, mut observed) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let service = trust
        .guard::<files::Request, _>(PolicyBarrier {
            entered,
            release: release.clone(),
        })
        .resource(file_resource)
        .install(|allow| files::service(Files(Arc::new(AtomicUsize::new(0))), allow))
        .build();
    let _running = Server::builder(&node).service(service).spawn().unwrap();
    let client = files::Client::connect(&peer, node.endpoint().addr())
        .await
        .unwrap();
    let pending = tokio::spawn(async move { client.read("repo/a/file".into()).await });
    observed.recv().await.unwrap();
    assert!(trust
        .apply(Change::Revoke(Revocation::issue(
            &root,
            selected.links()[0].id()
        )))
        .unwrap()
        .is_empty());
    assert_eq!(trust.live(), 1);
    release.notify_one();
    denied(pending.await.unwrap().unwrap_err());
}

#[tokio::test]
async fn a_forbidden_principal_does_not_mask_an_authorized_candidate() {
    struct PrincipalAccess(keylink::EndpointId);
    impl Access for PrincipalAccess {
        async fn authorize(&self, call: Call<'_>) -> Result<(), AccessError> {
            if call.credential.principal() == self.0 {
                Ok(())
            } else {
                Err(AccessError::Forbidden)
            }
        }
    }
    struct PrincipalMethods(keylink::EndpointId);
    impl methods::Handler for PrincipalMethods {
        async fn read(&self, cx: &Context) -> u64 {
            assert_eq!(cx.credential().unwrap().principal(), self.0);
            7
        }
    }
    let a = SecretKey::from_bytes(&[10; 32]);
    let b = SecretKey::from_bytes(&[11; 32]);
    let device = SecretKey::from_bytes(&[12; 32]);
    let trust = Trust::new(Roots::only([a.public(), b.public()]).unwrap());
    for root in [&a, &b] {
        trust
            .insert(Chain::new(vec![Certificate::issue(
                root,
                grant(device.public(), "repo/a", 0),
            )
            .unwrap()]))
            .unwrap();
    }
    let node = Node::bind_local().await.unwrap();
    let peer = Node::bind_local_with_secret(device).await.unwrap();
    let service = trust
        .guard::<methods::Request, _>(PrincipalAccess(b.public()))
        .resource(resource)
        .install(|allow| methods::service(PrincipalMethods(b.public()), allow))
        .build();
    let _running = Server::builder(&node).service(service).spawn().unwrap();
    let client = methods::Client::connect(&peer, node.endpoint().addr())
        .await
        .unwrap();
    assert_eq!(client.read().await.unwrap(), 7);
}

#[tokio::test]
async fn presentation_never_reports_held_for_an_evicted_offer() {
    let (root, device, _, trust) = fixture();
    for serial in 0..trust::MAX_CHAINS_PER_HOLDER as u64 {
        let mut delegation = grant(device.public(), "repo/a", 0);
        delegation.serial = serial;
        trust
            .insert(Chain::new(vec![
                Certificate::issue(&root, delegation).unwrap()
            ]))
            .unwrap();
    }
    let mut shorter = grant(device.public(), "repo/b", 0);
    shorter.caveats.validity = Validity::new(
        Timestamp::from_unix_secs(0),
        Timestamp::from_unix_secs(Timestamp::now().unix_secs() + 300),
    )
    .unwrap();
    let offered = Chain::new(vec![Certificate::issue(&root, shorter).unwrap()]);
    let node = Node::bind_local().await.unwrap();
    let peer = Node::bind_local_with_secret(device).await.unwrap();
    let _running = Server::builder(&node)
        .service(trust.presentation().build())
        .spawn()
        .unwrap();
    assert_eq!(
        trust::present(&peer, node.endpoint().addr(), &offered)
            .await
            .unwrap(),
        Presented::Refused(ChainError::NotRetained)
    );
}

#[tokio::test]
async fn a_request_fingerprint_prevents_cross_resource_replay_after_revocation() {
    let (root, device, _, trust) = fixture();
    let a = Chain::new(vec![Certificate::issue(
        &root,
        grant(device.public(), "repo/a", 0),
    )
    .unwrap()]);
    let b = Chain::new(vec![Certificate::issue(
        &root,
        grant(device.public(), "repo/b", 0),
    )
    .unwrap()]);
    trust.insert(a.clone()).unwrap();
    trust.insert(b).unwrap();
    let node = Node::bind_local().await.unwrap();
    let peer = Node::bind_local_with_secret(device).await.unwrap();
    let service = trust
        .guard::<files::Request, _>(WithinCaveats)
        .resource(file_resource)
        .install(|allow| files::service(Files(Arc::new(AtomicUsize::new(0))), allow))
        .build();
    let _running = Server::builder(&node).service(service).spawn().unwrap();
    let client = files::Client::connect(&peer, node.endpoint().addr())
        .await
        .unwrap();
    let keyed = client.with_idempotency(keylink::IdempotencyKey::new("resource-bound").unwrap());
    assert_eq!(keyed.read("repo/a/file".into()).await.unwrap(), 7);
    assert_eq!(keyed.read("repo/a/file".into()).await.unwrap(), 7);
    assert!(trust
        .apply(Change::Revoke(Revocation::issue(&root, a.links()[0].id())))
        .unwrap()
        .is_empty());
    denied(keyed.read("repo/a/file".into()).await.unwrap_err());
    assert!(matches!(
        keyed.read("repo/b/file".into()).await.unwrap_err(),
        Error::Remote(RemoteError::Idempotency(
            keylink::IdempotencyRefusal::RequestMismatch
        ))
    ));
    assert_eq!(client.read("repo/b/file".into()).await.unwrap(), 7);
}

struct ResourceCache {
    enabled: Arc<std::sync::atomic::AtomicBool>,
    entered: Arc<AtomicUsize>,
}
impl keylink::Middleware<files::Request, files::Response> for ResourceCache {
    fn handle(
        &self,
        cx: Context,
        req: files::Request,
        body: keylink::BodyReader,
        next: keylink::Next<files::Request, files::Response>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = (keylink::Outcome<files::Response>, keylink::Body)>
                + Send,
        >,
    > {
        self.entered.fetch_add(1, Ordering::SeqCst);
        if self.enabled.load(Ordering::SeqCst) && matches!(req, files::Request::Read { .. }) {
            Box::pin(async {
                (
                    keylink::Outcome::Ok(files::Response::Read(777)),
                    keylink::Body::empty(),
                )
            })
        } else {
            next(cx, req, body)
        }
    }
}
#[tokio::test]
async fn resource_changes_refuse_before_real_replays_and_short_circuit_layers() {
    let (root, device, _, trust) = fixture();
    let a = Chain::new(vec![Certificate::issue(
        &root,
        grant(device.public(), "repo/a", 0),
    )
    .unwrap()]);
    let b = Chain::new(vec![Certificate::issue(
        &root,
        grant(device.public(), "repo/b", 0),
    )
    .unwrap()]);
    trust.insert(a.clone()).unwrap();
    trust.insert(b).unwrap();
    let enabled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let entered = Arc::new(AtomicUsize::new(0));
    let node = Node::bind_local().await.unwrap();
    let peer = Node::bind_local_with_secret(device).await.unwrap();
    let service = trust
        .guard::<files::Request, _>(WithinCaveats)
        .resource(file_resource)
        .install(|allow| files::service(Files(Arc::new(AtomicUsize::new(0))), allow))
        .layer(ResourceCache {
            enabled: enabled.clone(),
            entered: entered.clone(),
        })
        .build();
    let _running = Server::builder(&node).service(service).spawn().unwrap();
    let client = files::Client::connect(&peer, node.endpoint().addr())
        .await
        .unwrap();
    let key = keylink::IdempotencyKey::new("real-record").unwrap();
    let keyed = client.with_idempotency(key.clone());
    assert_eq!(keyed.read("repo/a/file".into()).await.unwrap(), 7);
    assert_eq!(
        client.key_state(key.clone()).await.unwrap(),
        keylink::KeyState::Done
    );
    enabled.store(true, Ordering::SeqCst);
    assert!(trust
        .apply(Change::Revoke(Revocation::issue(&root, a.links()[0].id())))
        .unwrap()
        .is_empty());
    denied(keyed.read("repo/a/file".into()).await.unwrap_err());
    denied(client.read("repo/a/file".into()).await.unwrap_err());
    assert!(matches!(
        keyed.read("repo/b/file".into()).await.unwrap_err(),
        Error::Remote(RemoteError::Idempotency(
            keylink::IdempotencyRefusal::RequestMismatch
        ))
    ));
    assert_eq!(
        entered.load(Ordering::SeqCst),
        1,
        "neither denial nor mismatched replay reaches the cache"
    );
    assert_eq!(client.read("repo/b/file".into()).await.unwrap(), 777);
    assert_eq!(entered.load(Ordering::SeqCst), 2);
    assert_eq!(
        client.key_state(key).await.unwrap(),
        keylink::KeyState::Done
    );
}
