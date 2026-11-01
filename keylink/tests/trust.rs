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
// Made red with scripts/break-once.sh by capturing the sweep result before Trust::apply mutates the store, then returning that result.
// The multi-line or same-file edit is described rather than quoted.
// Watched: apply reported no closed peer instead of the device key; the former inline-sweep break is spent after extraction of sweep.
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
// Search `        for (registration, live) in self.0.live.snapshot() {`.
// Made red with scripts/break-once.sh replacing it with `        for (registration, live) in self.0.live.snapshot().into_iter().take(1) {`.
// Watched: one connection remained live instead of zero.
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
    assert_eq!(trust.live(), 0);
    let _ = tokio::join!(wa, wb);
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
// Search `            store.standing(credential.chain(), now).is_ok()`.
// Made red with scripts/break-once.sh replacing it with `            true`.
// Watched: the revoked credential dispatched after the policy barrier.
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
// Search `                    Err(AccessError::Forbidden) => continue,`.
// Made red with scripts/break-once.sh replacing it with `                    Err(AccessError::Forbidden) => return Err(RemoteError::Unauthorized(method.to_owned())),`.
// Watched: the first forbidden principal masked the second authorized one.
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
    let ca = Certificate::issue(&a, grant(device.public(), "repo/a", 0)).unwrap();
    let cb = Certificate::issue(&b, grant(device.public(), "repo/a", 0)).unwrap();
    // Retention sorts ascending by ids, so authorize only the second candidate.
    let allowed = if ca.id() > cb.id() {
        a.public()
    } else {
        b.public()
    };
    trust.insert(Chain::new(vec![ca])).unwrap();
    trust.insert(Chain::new(vec![cb])).unwrap();
    let node = Node::bind_local().await.unwrap();
    let peer = Node::bind_local_with_secret(device).await.unwrap();
    let service = trust
        .guard::<methods::Request, _>(PrincipalAccess(allowed))
        .resource(resource)
        .install(|allow| methods::service(PrincipalMethods(allowed), allow))
        .build();
    let _running = Server::builder(&node).service(service).spawn().unwrap();
    let client = methods::Client::connect(&peer, node.endpoint().addr())
        .await
        .unwrap();
    assert_eq!(client.read().await.unwrap(), 7);
}

#[tokio::test]
// Search `                    if offered {`.
// Made red with scripts/break-once.sh replacing it with `                if false {`.
// Watched: presentation claimed Held for the immediately evicted offer.
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
// Search `                if entry.fingerprint != fingerprint {`.
// Made red with scripts/break-once.sh replacing it with `                if false {`.
// Watched: the changed-resource call returned the original cached reply instead of RequestMismatch.
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
// Search `    for policy in required {`.
// Made red with scripts/break-once.sh replacing it with `    for policy in required.iter().take(0) {`.
// Watched: the revoked resource received its old replay before the cache test could proceed.
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

#[tokio::test]
// Search `    for (policy, credential) in required.iter().zip(&checked) {`.
// Made red with scripts/break-once.sh replacing it with `    for (policy, credential) in required.iter().zip(&checked).take(0) {`.
// Watched: the earlier revoked credential dispatched after a later policy waited.
async fn nested_policy_waits_recheck_earlier_selected_credentials() {
    let (root, device, _, first) = fixture();
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
    first.insert(a.clone()).unwrap();
    first.insert(b).unwrap();
    let second = Trust::new(Roots::only([root.public()]).unwrap());
    second.insert(a.clone()).unwrap();
    let (entered, mut observed) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let node = Node::bind_local().await.unwrap();
    let peer = Node::bind_local_with_secret(device).await.unwrap();
    let service = second
        .guard::<files::Request, _>(PolicyBarrier {
            entered,
            release: release.clone(),
        })
        .resource(file_resource)
        .install(|_| {
            first
                .guard::<files::Request, _>(WithinCaveats)
                .resource(file_resource)
                .install(|allow| files::service(Files(Arc::new(AtomicUsize::new(0))), allow))
        })
        .build();
    let _running = Server::builder(&node).service(service).spawn().unwrap();
    let client = files::Client::connect(&peer, node.endpoint().addr())
        .await
        .unwrap();
    let pending = tokio::spawn(async move { client.read("repo/a/file".into()).await });
    observed.recv().await.unwrap();
    assert!(first
        .apply(Change::Revoke(Revocation::issue(&root, a.links()[0].id())))
        .unwrap()
        .is_empty());
    release.notify_one();
    denied(pending.await.unwrap().unwrap_err());
}

#[tokio::test]
// Made red with scripts/break-once.sh by suppressing the promoted-tombstone sweep in Trust::verify with `if false`.
// The multi-line or same-file edit is described rather than quoted.
// Watched: the admitted-only connection remained live after its revocation became authoritative.
async fn learning_pending_revocation_authority_also_closes_existing_connections() {
    let root = SecretKey::from_bytes(&[1; 32]);
    let issuer = SecretKey::from_bytes(&[2; 32]);
    let device = SecretKey::from_bytes(&[3; 32]);
    let upstream = SecretKey::from_bytes(&[4; 32]);
    let leaf = Certificate::issue(&issuer, grant(device.public(), "repo/a", 0)).unwrap();
    let old = Chain::new(vec![
        Certificate::issue(&root, grant(issuer.public(), "repo", 1)).unwrap(),
        leaf.clone(),
    ]);
    let new = Chain::new(vec![
        Certificate::issue(&root, grant(upstream.public(), "repo", 2)).unwrap(),
        Certificate::issue(&upstream, grant(issuer.public(), "repo", 1)).unwrap(),
        leaf.clone(),
    ]);
    let trust = Trust::new(Roots::only([root.public()]).unwrap());
    trust.insert(old.clone()).unwrap();
    trust
        .apply(Change::Revoke(Revocation::issue(&upstream, leaf.id())))
        .unwrap();
    assert!(trust.verify(&old, Timestamp::now()).is_ok());
    let node = Node::bind_local().await.unwrap();
    let peer = Node::bind_local_with_secret(device).await.unwrap();
    let _running = Server::builder(&node)
        .service(
            ungated::service(Ungated, keylink::Allowlist::anyone())
                .admission(Arc::new(trust.clone()))
                .build(),
        )
        .spawn()
        .unwrap();
    let client = ungated::Client::connect(&peer, node.endpoint().addr())
        .await
        .unwrap();
    assert_eq!(client.read().await.unwrap(), 7);
    let watch = client.watch();
    assert!(matches!(
        trust.verify(&new, Timestamp::now()),
        Err(ChainError::Revoked { at: 2 })
    ));
    assert_eq!(trust.live(), 0);
    let _ = watch.await;
    assert_eq!(
        client.read().await.unwrap_err().refusal(),
        Some(Refusal::Denied)
    );
}

#[tokio::test]
// Search `            hash.update(credential.principal().as_bytes());`.
// Made red with scripts/break-once.sh replacing it with `            hash.update(&[0; 32]);`.
// Watched: a retry under B received A's cached value instead of RequestMismatch.
async fn a_recorded_reply_cannot_cross_principals_for_the_same_request() {
    struct PrincipalData {
        a: keylink::EndpointId,
        b: keylink::EndpointId,
    }
    impl methods::Handler for PrincipalData {
        async fn read(&self, cx: &Context) -> u64 {
            let principal = cx.credential().unwrap().principal();
            if principal == self.a {
                11
            } else {
                assert_eq!(principal, self.b);
                22
            }
        }
    }
    let root_a = SecretKey::from_bytes(&[24; 32]);
    let root_b = SecretKey::from_bytes(&[25; 32]);
    let device = SecretKey::from_bytes(&[26; 32]);
    let ca = Certificate::issue(&root_a, grant(device.public(), "repo/a", 0)).unwrap();
    let cb = Certificate::issue(&root_b, grant(device.public(), "repo/a", 0)).unwrap();
    let (first, selected, other) = if ca.id() < cb.id() {
        (root_a.clone(), ca.clone(), root_b.public())
    } else {
        (root_b.clone(), cb.clone(), root_a.public())
    };
    let trust = Trust::new(Roots::only([root_a.public(), root_b.public()]).unwrap());
    trust.insert(Chain::new(vec![ca])).unwrap();
    trust.insert(Chain::new(vec![cb])).unwrap();
    let node = Node::bind_local().await.unwrap();
    let peer = Node::bind_local_with_secret(device).await.unwrap();
    let service = trust
        .guard::<methods::Request, _>(WithinCaveats)
        .resource(resource)
        .install(|allow| {
            methods::service(
                PrincipalData {
                    a: first.public(),
                    b: other,
                },
                allow,
            )
        })
        .build();
    let _running = Server::builder(&node).service(service).spawn().unwrap();
    let client = methods::Client::connect(&peer, node.endpoint().addr())
        .await
        .unwrap();
    let keyed = client.with_idempotency(keylink::IdempotencyKey::new("principal-data").unwrap());
    assert_eq!(keyed.read().await.unwrap(), 11);
    assert_eq!(keyed.read().await.unwrap(), 11);
    assert!(trust
        .apply(Change::Revoke(Revocation::issue(&first, selected.id())))
        .unwrap()
        .is_empty());
    assert_eq!(client.read().await.unwrap(), 22);
    assert!(matches!(
        keyed.read().await.unwrap_err(),
        Error::Remote(RemoteError::Idempotency(
            keylink::IdempotencyRefusal::RequestMismatch
        ))
    ));
}

#[tokio::test]
// Search `            if revoked.is_exhausted(&key) {`.
// Made red with scripts/break-once.sh replacing it with `            if false {`.
// Watched: the exhausted subtree retained its live connection.
// Search `        let closed = self.sweep();`.
// Made red with scripts/break-once.sh replacing it with `        let closed = if result.is_ok() { self.sweep() } else { Vec::new() };`.
// Watched: an exhaustion error left the revoked subtree connection open.
async fn revoker_exhaustion_closes_its_live_subtree_and_keeps_a_sibling_open() {
    let root = SecretKey::from_bytes(&[1; 32]);
    let delegate = SecretKey::from_bytes(&[2; 32]);
    let holder = SecretKey::from_bytes(&[3; 32]);
    let sibling = SecretKey::from_bytes(&[4; 32]);
    for during_apply in [false, true] {
        let trust = Trust::new(Roots::only([root.public()]).unwrap());
        let parent = Certificate::issue(&root, grant(delegate.public(), "repo", 1)).unwrap();
        let mut live_grant = grant(holder.public(), "repo/a", 0);
        live_grant.serial = u64::MAX;
        let live_leaf = Certificate::issue(&delegate, live_grant).unwrap();
        trust
            .insert(Chain::new(vec![parent.clone(), live_leaf.clone()]))
            .unwrap();
        trust
            .insert(Chain::new(vec![Certificate::issue(
                &root,
                grant(sibling.public(), "repo/b", 0),
            )
            .unwrap()]))
            .unwrap();
        let node = Node::bind_local().await.unwrap();
        let peer = Node::bind_local_with_secret(holder.clone()).await.unwrap();
        let keeper = Node::bind_local_with_secret(sibling.clone()).await.unwrap();
        let _running = Server::builder(&node)
            .service(
                ungated::service(Ungated, keylink::Allowlist::anyone())
                    .admission(Arc::new(trust.clone()))
                    .build(),
            )
            .spawn()
            .unwrap();
        let client = ungated::Client::connect(&peer, node.endpoint().addr())
            .await
            .unwrap();
        let kept = ungated::Client::connect(&keeper, node.endpoint().addr())
            .await
            .unwrap();
        assert_eq!(client.read().await.unwrap(), 7);
        assert_eq!(kept.read().await.unwrap(), 7);
        let watch = client.watch();
        for serial in 0..trust::MAX_ACCEPTED_PER_REVOKER as u64 {
            let mut delegation = grant(holder.public(), "repo/a", 0);
            delegation.serial = serial;
            let leaf = Certificate::issue(&delegate, delegation).unwrap();
            trust
                .apply(Change::Revoke(Revocation::issue(&delegate, leaf.id())))
                .unwrap();
            assert_eq!(
                trust
                    .verify(&Chain::new(vec![parent.clone(), leaf]), Timestamp::now())
                    .unwrap_err(),
                ChainError::Revoked { at: 1 }
            );
        }
        assert_eq!(trust.live(), 2);
        if during_apply {
            assert!(matches!(
                trust.apply(Change::Revoke(Revocation::issue(&delegate, live_leaf.id()))),
                Err(trust::ChangeError::RevokerExhausted { .. })
            ));
        } else {
            let mut delegation = grant(holder.public(), "repo/a", 0);
            delegation.serial = trust::MAX_ACCEPTED_PER_REVOKER as u64;
            let leaf = Certificate::issue(&delegate, delegation).unwrap();
            trust
                .apply(Change::Revoke(Revocation::issue(&delegate, leaf.id())))
                .unwrap();
            assert!(matches!(
                trust.verify(&Chain::new(vec![parent, leaf]), Timestamp::now()),
                Err(ChainError::RevokerExhausted { .. })
            ));
        }
        assert_eq!(trust.live(), 1);
        let _ = watch.await;
        assert_eq!(
            client.read().await.unwrap_err().refusal(),
            Some(Refusal::Denied)
        );
        assert_eq!(kept.read().await.unwrap(), 7);
    }
}

#[tokio::test]
// Search `        hasher.update(&binding);`.
// Made red with scripts/break-once.sh replacing it with `        let _ = binding;`.
// Watched: principal B received A's applied answer (11 instead of 22) after the
// transport ledger reported the original key Fresh.
async fn applied_replies_keep_authority_after_the_ledger_forgets_the_key() {
    struct PrincipalState {
        a: keylink::EndpointId,
        b: keylink::EndpointId,
        applied: std::sync::Mutex<keylink::Applied<u64>>,
    }
    impl methods::Handler for PrincipalState {
        async fn read(&self, cx: &Context) -> u64 {
            let principal = cx.credential().unwrap().principal();
            let mut applied = self.applied.lock().unwrap();
            if let Some(&answer) = applied.get(cx.call()) {
                return answer;
            }
            let answer = if principal == self.a {
                11
            } else {
                assert_eq!(principal, self.b);
                22
            };
            applied.insert(cx.call(), answer);
            answer
        }
    }
    let root_a = SecretKey::from_bytes(&[24; 32]);
    let root_b = SecretKey::from_bytes(&[25; 32]);
    let device = SecretKey::from_bytes(&[26; 32]);
    let ca = Certificate::issue(&root_a, grant(device.public(), "repo/a", 0)).unwrap();
    let cb = Certificate::issue(&root_b, grant(device.public(), "repo/a", 0)).unwrap();
    let (first, selected, other) = if ca.id() < cb.id() {
        (root_a.clone(), ca.clone(), root_b.public())
    } else {
        (root_b.clone(), cb.clone(), root_a.public())
    };
    let trust = Trust::new(Roots::only([root_a.public(), root_b.public()]).unwrap());
    trust.insert(Chain::new(vec![ca])).unwrap();
    trust.insert(Chain::new(vec![cb])).unwrap();
    let node = Node::bind_local().await.unwrap();
    let peer = Node::bind_local_with_secret(device).await.unwrap();
    let service = trust
        .guard::<methods::Request, _>(WithinCaveats)
        .resource(resource)
        .install(|allow| {
            methods::service(
                PrincipalState {
                    a: first.public(),
                    b: other,
                    applied: std::sync::Mutex::new(keylink::Applied::new()),
                },
                allow,
            )
        })
        .limits(
            keylink::Limits::default()
                .pool_records(std::num::NonZeroUsize::new(1).unwrap())
                .tombstones(std::num::NonZeroUsize::new(1).unwrap()),
        )
        .build();
    let _running = Server::builder(&node).service(service).spawn().unwrap();
    let client = methods::Client::connect(&peer, node.endpoint().addr())
        .await
        .unwrap();
    let key = keylink::IdempotencyKey::new("applied-principal").unwrap();
    let keyed = client.with_idempotency(key.clone());
    assert_eq!(keyed.read().await.unwrap(), 11);
    assert!(trust
        .apply(Change::Revoke(Revocation::issue(&first, selected.id())))
        .unwrap()
        .is_empty());
    for next in ["churn-one", "churn-two"] {
        assert_eq!(
            client
                .with_idempotency(keylink::IdempotencyKey::new(next).unwrap())
                .read()
                .await
                .unwrap(),
            22
        );
    }
    assert_eq!(
        client.key_state(key).await.unwrap(),
        keylink::KeyState::Fresh
    );
    assert_eq!(client.read().await.unwrap(), 22);
    assert_eq!(
        keyed.read().await.unwrap(),
        22,
        "the new principal runs rather than receiving A's applied answer"
    );
}
