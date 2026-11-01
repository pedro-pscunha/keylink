//! Real peers negotiate one contract and carry only current types to the handler.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use keylink::{prelude::*, Allowlist, Error, Node, PeerWindow, Rpc, Unsupported};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use tokio::sync::Notify;

mod v2 {
    #[derive(
        Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, postcard_schema::Schema,
    )]
    pub struct Entry {
        pub qty: u64,
    }
    keylink::service! {
        service Ledger;
        version 2;
        rpc Get(id: u64) -> Option<Entry>;
        rpc Add(entry: Entry) -> u64;
        tell Note(line: String) -> ();
    }
}
mod v3 {
    use super::{v2, Rpc, Unsupported};
    #[derive(
        Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, postcard_schema::Schema,
    )]
    pub struct Entry {
        pub qty: u64,
        pub memo: Option<String>,
    }
    keylink::service! {
        service Ledger;
        version 3;
        rpc Get(id: u64) -> Option<Entry>;
        rpc Add(entry: Entry) -> u64;
        tell Note(line: String) -> ();
        rpc Purge() -> u64;
        previous 2 {
            hash v2::ledger::VERSION.hash(); keyed_hash v2::ledger::KEYED_VERSION.hash();
            changed rpc Get(id: u64) -> Option<v2::Entry>;
            changed rpc Add(entry: v2::Entry) -> u64;
            tell Note(line: String) -> ();
        }
    }
    impl ledger::version_2::Convert for ledger::version_2::Step {
        fn upgrade_request(
            old: ledger::version_2::Request,
        ) -> Result<ledger::Request, Unsupported> {
            use ledger::{version_2::Request as Old, Request as New};
            match old {
                Old::Get { id } => Ok(New::Get { id }),
                Old::Add { entry } if entry.qty == 15 => {
                    Err(Unsupported::new("Add", "request refused by conversion"))
                }
                Old::Add { entry } => Ok(New::Add {
                    entry: Entry {
                        qty: entry.qty,
                        memo: None,
                    },
                }),
                other => Err(Unsupported::new(
                    other.method(),
                    "carried by generated arms",
                )),
            }
        }
        fn downgrade_request(
            new: ledger::Request,
        ) -> Result<ledger::version_2::Request, Unsupported> {
            use ledger::{version_2::Request as Old, Request as New};
            match new {
                New::Get { id } => Ok(Old::Get { id }),
                New::Add { entry } => Ok(Old::Add {
                    entry: v2::Entry { qty: entry.qty },
                }),
                other => Err(Unsupported::new(
                    other.method(),
                    "version 2 has no such method",
                )),
            }
        }
        fn upgrade_response(
            old: ledger::version_2::Response,
        ) -> Result<ledger::Response, Unsupported> {
            use ledger::{version_2::Response as Old, Response as New};
            match old {
                Old::Get(entry) => Ok(New::Get(entry.map(|e| Entry {
                    qty: e.qty,
                    memo: None,
                }))),
                Old::Add(13) => Err(Unsupported::new("Add", "reply upgrade refused")),
                Old::Add(n) => Ok(New::Add(n)),
                _ => Err(Unsupported::new("Note", "carried by generated arms")),
            }
        }
        fn downgrade_response(
            new: ledger::Response,
        ) -> Result<ledger::version_2::Response, Unsupported> {
            use ledger::{version_2::Response as Old, Response as New};
            match new {
                New::Get(entry) => Ok(Old::Get(entry.map(|e| v2::Entry { qty: e.qty }))),
                New::Add(14) => Err(Unsupported::new("Add", "reply downgrade refused")),
                New::Add(n) => Ok(Old::Add(n)),
                _ => Err(Unsupported::new("Purge", "version 2 has no such method")),
            }
        }
    }
}
mod v5 {
    keylink::service! { service Ledger; version 5; rpc Get(id: u64) -> u8; }
}
mod v3_drifted {
    keylink::service! { service Ledger; version 3; rpc Get(id: u64) -> u8; }
}

#[derive(Clone, Default)]
struct Current {
    calls: Arc<AtomicU64>,
    added: Arc<Mutex<Option<v3::Entry>>>,
    noted: Arc<Notify>,
}
impl v3::ledger::Handler for Current {
    async fn get(&self, _cx: &keylink::Context, _id: u64) -> Option<v3::Entry> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Some(v3::Entry {
            qty: 7,
            memo: Some("late".into()),
        })
    }
    async fn add(&self, _cx: &keylink::Context, entry: v3::Entry) -> u64 {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let answer = if entry.qty == 14 { 14 } else { 1 };
        *self.added.lock().unwrap() = Some(entry);
        answer
    }
    async fn note(&self, _cx: &keylink::Context, _line: String) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.noted.notify_one();
    }
    async fn purge(&self, _cx: &keylink::Context) -> u64 {
        self.calls.fetch_add(1, Ordering::SeqCst);
        0
    }
}
impl v3::ledger::Actor for Current {
    async fn get(&mut self, _cx: keylink::Caller<'_>, _id: u64) -> Option<v3::Entry> {
        Some(v3::Entry {
            qty: 7,
            memo: Some("late".into()),
        })
    }
    async fn add(&mut self, _cx: keylink::Caller<'_>, entry: v3::Entry) -> u64 {
        *self.added.lock().unwrap() = Some(entry);
        1
    }
    async fn note(&mut self, _cx: keylink::Caller<'_>, _line: String) {
        self.noted.notify_one();
    }
    async fn purge(&mut self, _cx: keylink::Caller<'_>) -> u64 {
        0
    }
}
#[derive(Clone, Default)]
struct Older(Arc<AtomicU64>);
impl v2::ledger::Handler for Older {
    async fn get(&self, _cx: &keylink::Context, _id: u64) -> Option<v2::Entry> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Some(v2::Entry { qty: 7 })
    }
    async fn add(&self, _cx: &keylink::Context, entry: v2::Entry) -> u64 {
        self.0.fetch_add(1, Ordering::SeqCst);
        if entry.qty == 13 {
            13
        } else {
            1
        }
    }
    async fn note(&self, _cx: &keylink::Context, _line: String) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
async fn node() -> Node {
    Node::bind_local().await.unwrap()
}

#[tokio::test]
async fn an_older_client_reaches_the_newer_handler() {
    let server = node().await;
    let peer = node().await;
    let handler = Current::default();
    let _running = v3::ledger::serve(&server, handler.clone(), Allowlist::anyone()).unwrap();
    let old = v2::ledger::Client::connect(&peer, server.endpoint().addr())
        .await
        .unwrap();
    let new = v3::ledger::Client::connect(&peer, server.endpoint().addr())
        .await
        .unwrap();
    assert_eq!(old.version().label(), 2);
    assert_eq!(new.version().label(), 3);
    assert!(old.get(1).await.unwrap().is_some());
    assert!(new.get(1).await.unwrap().is_some());
    let _ = old.note("hello".into()).await.unwrap();
    handler.noted.notified().await;
    assert_eq!(handler.calls.load(Ordering::SeqCst), 3);
}
/// Watched red with `scripts/break-once.sh`: replace
/// `iroh::endpoint::ConnectOptions::new().with_additional_alpns(additional),`
/// with `iroh::endpoint::ConnectOptions::new().with_additional_alpns({ let mut missing: Vec<Vec<u8>> = additional; missing.clear(); missing }),`.
/// The isolated run reported 0 passed, 1 failed, and `SchemaMismatch` at the
/// connect unwrap: removing the older offer prevents fallback. Restored.
#[tokio::test]
async fn a_newer_client_speaks_the_older_version_to_an_older_server() {
    let server = node().await;
    let peer = node().await;
    let handler = Older::default();
    let _running = v2::ledger::serve(&server, handler.clone(), Allowlist::anyone()).unwrap();
    let c = v3::ledger::Client::connect(&peer, server.endpoint().addr())
        .await
        .unwrap();
    assert_eq!(c.version(), v2::ledger::VERSION);
    assert_eq!(
        c.get(1).await.unwrap(),
        Some(v3::Entry { qty: 7, memo: None })
    );
    let Err(Error::UnsupportedRequest { version, source }) = c.purge().await else {
        panic!("Purge must be refused locally")
    };
    assert_eq!(version.label(), 2);
    assert_eq!(source.method(), "Purge");
    assert_eq!(handler.0.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn a_field_added_in_3_reaches_a_2_client_as_its_converted_shape() {
    let server = node().await;
    let peer = node().await;
    let handler = Current::default();
    let _running = v3::ledger::serve(&server, handler.clone(), Allowlist::anyone()).unwrap();
    let c = v2::ledger::Client::connect(&peer, server.endpoint().addr())
        .await
        .unwrap();
    assert_eq!(c.get(1).await.unwrap(), Some(v2::Entry { qty: 7 }));
    assert_eq!(c.add(v2::Entry { qty: 4 }).await.unwrap(), 1);
    assert_eq!(
        *handler.added.lock().unwrap(),
        Some(v3::Entry { qty: 4, memo: None })
    );
}
#[tokio::test]
async fn no_common_version_names_both_windows() {
    let server = node().await;
    let peer = node().await;
    let _running = v3::ledger::serve(&server, Current::default(), Allowlist::anyone()).unwrap();
    let err = v5::ledger::Client::connect(&peer, server.endpoint().addr())
        .await
        .err()
        .unwrap();
    let Error::SchemaMismatch(m) = &err else {
        panic!("{err}")
    };
    assert_eq!(
        m.ours()
            .versions()
            .iter()
            .map(keylink::Version::label)
            .collect::<Vec<_>>(),
        [5]
    );
    let PeerWindow::Speaks(w) = m.theirs() else {
        panic!("probe did not name peer's window")
    };
    assert_eq!(
        w.versions()
            .iter()
            .map(keylink::Version::label)
            .collect::<Vec<_>>(),
        [3, 2]
    );
    assert!(err.to_string().contains("keylink/Ledger/5"));
    assert!(err.to_string().contains("keylink/Ledger/2..=3"));
}
#[tokio::test]
async fn one_label_with_two_shapes_is_named() {
    let server = node().await;
    let peer = node().await;
    let _running = v3::ledger::serve(&server, Current::default(), Allowlist::anyone()).unwrap();
    let err = v3_drifted::ledger::Client::connect(&peer, server.endpoint().addr())
        .await
        .err()
        .unwrap();
    assert!(
        matches!(err, Error::SchemaMismatch(ref m) if matches!(m.theirs(), PeerWindow::Speaks(_)))
    );
    assert!(err
        .to_string()
        .contains("both claim 3 with different shapes"));
}
#[tokio::test]
async fn a_keyed_registry_answers_an_older_registry_client() {
    let server = node().await;
    let peer = node().await;
    let registry: v3::ledger::Registry =
        v3::ledger::Registry::builder(keylink::Eviction::Never).build();
    let id = keylink::ActorId::new("one").unwrap();
    registry
        .mount(id.clone(), v3::ledger::spawn(Current::default()))
        .unwrap();
    let _running = keylink::Server::builder(&server)
        .service(registry.service(Allowlist::anyone()).build())
        .spawn()
        .unwrap();
    let c = v2::ledger::RegistryClient::connect(&peer, server.endpoint().addr())
        .await
        .unwrap();
    assert_eq!(c.version().label(), 2);
    assert_eq!(c.at(id).get(1).await.unwrap(), Some(v2::Entry { qty: 7 }));
}

/// Both versions share one ledger, but its binding distinguishes their labels.
/// Get has identical received bytes in both versions, so only the label can
/// separate these calls. A same-version retry must still replay without dispatch.
/// Watched red by zeroing the label contribution in replay_binding: the newer
/// caller received the older call's record instead of RequestMismatch (0 passed,
/// 1 failed). Restored the label commitment shared with CallId v2.
#[tokio::test]
async fn contract_version_is_part_of_the_request_binding() {
    assert_eq!(
        postcard::to_allocvec(&keylink::WireRequest::Call(v2::ledger::Request::Get {
            id: 1
        }))
        .unwrap(),
        postcard::to_allocvec(&keylink::WireRequest::Call(v3::ledger::Request::Get {
            id: 1
        }))
        .unwrap(),
    );
    let server = node().await;
    let peer = node().await;
    let handler = Current::default();
    let _running = v3::ledger::serve(&server, handler.clone(), Allowlist::anyone()).unwrap();
    let old = v2::ledger::Client::connect(&peer, server.endpoint().addr())
        .await
        .unwrap();
    let new = v3::ledger::Client::connect(&peer, server.endpoint().addr())
        .await
        .unwrap();
    let key = keylink::IdempotencyKey::new("one-call").unwrap();
    assert_eq!(
        old.with_idempotency(key.clone()).get(1).await.unwrap(),
        Some(v2::Entry { qty: 7 })
    );
    assert!(matches!(
        new.with_idempotency(key.clone()).get(1).await,
        Err(Error::Remote(keylink::RemoteError::Idempotency(
            keylink::IdempotencyRefusal::RequestMismatch
        )))
    ));
    assert_eq!(
        old.with_idempotency(key).get(1).await.unwrap(),
        Some(v2::Entry { qty: 7 })
    );
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
}

#[cfg(feature = "persist-file")]
mod recorded {
    use super::*;
    struct Scratch(std::path::PathBuf);
    impl Scratch {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            Self(std::env::temp_dir().join(format!(
                "keylink-versions-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::SeqCst)
            )))
        }
    }
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    struct Unrelated(Arc<AtomicU64>);
    impl v5::ledger::Handler for Unrelated {
        async fn get(&self, _cx: &keylink::Context, _id: u64) -> u8 {
            self.0.fetch_add(1, Ordering::SeqCst);
            9
        }
    }
    async fn records(files: &keylink_persist::file::Files) -> keylink::Records {
        keylink::Records::Durable(
            keylink::DurableRecords::open(
                files,
                &keylink_persist::StateId::new("versions/ledger").unwrap(),
            )
            .await
            .unwrap(),
        )
    }
    #[tokio::test]
    async fn durable_recovery_upgrades_known_shapes_and_forgets_unknown_ones() {
        let scratch = Scratch::new();
        let files = keylink_persist::file::Files::open(&scratch.0)
            .await
            .unwrap();
        let peer = node().await;
        let key = keylink::IdempotencyKey::new("recorded-at-two").unwrap();
        {
            let server = node().await;
            let service = v2::ledger::service(Older::default(), Allowlist::anyone())
                .records(records(&files).await)
                .build();
            let running = keylink::Server::builder(&server)
                .service(service)
                .spawn()
                .unwrap();
            let c = v2::ledger::Client::connect(&peer, server.endpoint().addr())
                .await
                .unwrap();
            assert_eq!(
                c.with_idempotency(key.clone()).get(1).await.unwrap(),
                Some(v2::Entry { qty: 7 })
            );
            c.close().await.unwrap();
            running.shutdown().await.unwrap();
        }
        {
            let server = node().await;
            let handler = Current::default();
            let service = v3::ledger::service(handler.clone(), Allowlist::anyone())
                .records(records(&files).await)
                .build();
            let running = keylink::Server::builder(&server)
                .service(service)
                .spawn()
                .unwrap();
            let c = v3::ledger::Client::connect(&peer, server.endpoint().addr())
                .await
                .unwrap();
            assert_eq!(
                c.key_state(key.clone()).await.unwrap(),
                keylink::KeyState::Done
            );
            assert!(matches!(
                c.with_idempotency(key.clone()).get(1).await,
                Err(Error::Remote(keylink::RemoteError::Idempotency(
                    keylink::IdempotencyRefusal::RequestMismatch
                )))
            ));
            let old = v2::ledger::Client::connect(&peer, server.endpoint().addr())
                .await
                .unwrap();
            assert_eq!(
                old.with_idempotency(key.clone()).get(1).await.unwrap(),
                Some(v2::Entry { qty: 7 })
            );
            old.close().await.unwrap();
            assert_eq!(handler.calls.load(Ordering::SeqCst), 0);
            c.close().await.unwrap();
            running.shutdown().await.unwrap();
        }
        {
            let server = node().await;
            let calls = Arc::new(AtomicU64::new(0));
            let service = v5::ledger::service(Unrelated(calls.clone()), Allowlist::anyone())
                .records(records(&files).await)
                .build();
            let running = keylink::Server::builder(&server)
                .service(service)
                .spawn()
                .unwrap();
            let c = v5::ledger::Client::connect(&peer, server.endpoint().addr())
                .await
                .unwrap();
            assert_eq!(
                c.key_state(key.clone()).await.unwrap(),
                keylink::KeyState::Forgotten
            );
            assert!(matches!(
                c.with_idempotency(key).get(1).await,
                Err(Error::Remote(keylink::RemoteError::Idempotency(
                    keylink::IdempotencyRefusal::RequestMismatch
                )))
            ));
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            c.close().await.unwrap();
            running.shutdown().await.unwrap();
        }
    }
}

mod chain {
    mod one {
        keylink::service! { service Chain; version 1; rpc Echo(n: u64) -> u64; }
    }
    mod two {
        keylink::service! { service Chain; version 2; rpc Echo(n: u64) -> u64; }
    }
    keylink::service! {
        service Chain;
        version 3;
        rpc Echo(n: u64) -> u64;
        previous 2 { hash two::chain::VERSION.hash(); keyed_hash two::chain::KEYED_VERSION.hash(); rpc Echo(n: u64) -> u64; }
        previous 1 { hash one::chain::VERSION.hash(); keyed_hash one::chain::KEYED_VERSION.hash(); rpc Echo(n: u64) -> u64; }
    }
    // No changed methods. Any application conversion being reached is a defect.
    macro_rules! plain {
        ($old:ident, $new:path) => {
            impl chain::$old::Convert for chain::$old::Step {
                fn upgrade_request(
                    _: chain::$old::Request,
                ) -> Result<<$new as keylink::Hop>::NewerRequest, keylink::Unsupported> {
                    panic!("plain requests are generated")
                }
                fn downgrade_request(
                    _: <$new as keylink::Hop>::NewerRequest,
                ) -> Result<chain::$old::Request, keylink::Unsupported> {
                    panic!("plain requests are generated")
                }
                fn upgrade_response(
                    _: chain::$old::Response,
                ) -> Result<<$new as keylink::Hop>::NewerResponse, keylink::Unsupported> {
                    panic!("plain replies are generated")
                }
                fn downgrade_response(
                    _: <$new as keylink::Hop>::NewerResponse,
                ) -> Result<chain::$old::Response, keylink::Unsupported> {
                    panic!("plain replies are generated")
                }
            }
        };
    }
    plain!(version_2, chain::version_2::Step);
    plain!(version_1, chain::version_1::Step);
    struct Handler;
    impl chain::Handler for Handler {
        async fn echo(&self, _cx: &keylink::Context, n: u64) -> u64 {
            n
        }
    }
    #[tokio::test]
    async fn adjacent_steps_compose_across_two_previous_blocks() {
        use keylink::Wire;
        let server = super::node().await;
        let peer = super::node().await;
        let _running = chain::serve(&server, Handler, keylink::Allowlist::anyone()).unwrap();
        let c = one::chain::Client::connect(&peer, server.endpoint().addr())
            .await
            .unwrap();
        assert_eq!(c.version().label(), 1);
        assert_eq!(c.echo(17).await.unwrap(), 17);
    }
}

/// Watched red: classify a client reply conversion as an unsent request.
/// This isolated test failed after the old handler ran once: 0 passed, 1 failed.
/// Restored the separate response error.
#[tokio::test]
async fn conversion_failures_name_the_execution_stage() {
    let server = node().await;
    let peer = node().await;
    let old_handler = Older::default();
    let _old_server = v2::ledger::serve(&server, old_handler.clone(), Allowlist::anyone()).unwrap();
    let newer = v3::ledger::Client::connect(&peer, server.endpoint().addr())
        .await
        .unwrap();
    assert!(matches!(
        newer
            .add(v3::Entry {
                qty: 13,
                memo: None
            })
            .await,
        Err(Error::UnsupportedResponse { .. })
    ));
    assert_eq!(
        old_handler.0.load(Ordering::SeqCst),
        1,
        "a reply conversion error does not mean nothing ran"
    );
    let server = node().await;
    let current = Current::default();
    let _new_server = v3::ledger::serve(&server, current.clone(), Allowlist::anyone()).unwrap();
    let older = v2::ledger::Client::connect(&peer, server.endpoint().addr())
        .await
        .unwrap();
    let key = keylink::IdempotencyKey::new("unconvertible-request").unwrap();
    assert!(matches!(
        older
            .with_idempotency(key.clone())
            .add(v2::Entry { qty: 15 })
            .await,
        Err(Error::Remote(
            keylink::RemoteError::UnsupportedRequest { .. }
        ))
    ));
    assert_eq!(current.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        older.key_state(key).await.unwrap(),
        keylink::KeyState::Fresh
    );
    assert!(matches!(
        older.add(v2::Entry { qty: 14 }).await,
        Err(Error::Remote(
            keylink::RemoteError::UnsupportedResponse { .. }
        ))
    ));
    assert_eq!(current.calls.load(Ordering::SeqCst), 1);
}

/// Watched red: replace the call health check's close reason with
/// `None::<ConnectionError>`. The isolated test returned UnsupportedRequest
/// instead of renegotiating: 0 passed, 1 failed. Restored.
#[tokio::test]
async fn a_closed_older_codec_cannot_block_renegotiation() {
    let server = node().await;
    let caller = node().await;
    let handler = Current::default();
    let _running = v3::ledger::serve(&server, handler.clone(), Allowlist::anyone()).unwrap();
    // Initially advertise only 2. The router already has both handlers, so
    // changing the advertised window models an upgrade without changing routes.
    server
        .endpoint()
        .set_alpns(vec![v2::ledger::alpn().to_vec()]);
    let peer: keylink::Peer<v3::ledger::Client> =
        keylink::Peer::new(&caller, server.endpoint().addr());
    let cached = peer.connect().await.unwrap();
    assert_eq!(cached.version().label(), 2);
    cached.as_ref().clone().close().await.unwrap();
    server.endpoint().set_alpns(vec![
        v3::ledger::alpn().to_vec(),
        v2::ledger::alpn().to_vec(),
    ]);
    assert_eq!(
        peer.call(|client| async move { client.purge().await })
            .await
            .unwrap(),
        0
    );
    assert_eq!(peer.connect().await.unwrap().version().label(), 3);
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
}

mod blob_versions {
    use super::*;
    static REPLACEMENT: std::sync::OnceLock<keylink::BlobId> = std::sync::OnceLock::new();
    mod old {
        keylink::service! { service Content; version 1; blob Get(mode: u8) -> u8; }
    }
    keylink::service! {
        service Content; version 2;
        blob Get(mode: u8) -> u8;
        previous 1 { hash old::content::VERSION.hash(); keyed_hash old::content::KEYED_VERSION.hash(); changed blob Get(mode: u8) -> u8; }
    }
    impl content::version_1::Convert for content::version_1::Step {
        fn upgrade_request(
            r: content::version_1::Request,
        ) -> Result<content::Request, Unsupported> {
            match r {
                content::version_1::Request::Get { mode } => Ok(content::Request::Get { mode }),
                _ => unreachable!(),
            }
        }
        fn downgrade_request(
            r: content::Request,
        ) -> Result<content::version_1::Request, Unsupported> {
            match r {
                content::Request::Get { mode } => Ok(content::version_1::Request::Get { mode }),
                _ => unreachable!(),
            }
        }
        fn upgrade_response(
            r: content::version_1::Response,
        ) -> Result<content::Response, Unsupported> {
            match r {
                content::version_1::Response::Get((3, _)) => Ok(content::Response::Get((
                    13,
                    (*REPLACEMENT.get().unwrap()).into(),
                ))),
                content::version_1::Response::Get(v) => Ok(content::Response::Get(v)),
                _ => unreachable!(),
            }
        }
        fn downgrade_response(
            r: content::Response,
        ) -> Result<content::version_1::Response, Unsupported> {
            match r {
                content::Response::Get((0, _)) => {
                    Err(Unsupported::new("Get", "no older representation"))
                }
                content::Response::Get((1, _)) => Ok(content::version_1::Response::Get((
                    1,
                    (*REPLACEMENT.get().unwrap()).into(),
                ))),
                content::Response::Get((4, content)) => Ok(content::version_1::Response::Get((
                    4,
                    keylink::Content::Closure(content.root()),
                ))),
                content::Response::Get((n, id)) => {
                    Ok(content::version_1::Response::Get((n + 10, id)))
                }
                _ => unreachable!(),
            }
        }
    }
    struct Handler(keylink::BlobId);
    impl content::Handler for Handler {
        async fn get(&self, _cx: &keylink::Context, mode: u8) -> (u8, keylink::Grant) {
            (mode, self.0.into())
        }
    }
    impl old::content::Handler for Handler {
        async fn get(&self, _cx: &keylink::Context, mode: u8) -> (u8, keylink::Grant) {
            (mode, self.0.into())
        }
    }
    /// Watched red: grant the current response's address instead of the
    /// encoded response's address. The refusal leaked a grant in this isolated
    /// test: 0 passed, 1 failed. Restored the converted grant source.
    ///
    /// The identity guard was also watched red by allowing the converted blob
    /// address to differ: this test accepted the substituted reply instead of
    /// UnsupportedResponse (0 passed, 1 failed). Restored identity preservation.
    #[tokio::test]
    async fn blob_conversions_keep_identity_and_refuse_address_changes() {
        let server = node().await;
        let caller = node().await;
        let store = keylink::Blobs::memory();
        let original = store.add_bytes(b"original".as_slice()).await.unwrap();
        let replacement = store.add_bytes(b"replacement".as_slice()).await.unwrap();
        REPLACEMENT.set(replacement).unwrap();
        let _running = keylink::Server::builder(&server)
            .service(
                content::service(Handler(original), Allowlist::anyone())
                    .with_blobs(&store)
                    .build(),
            )
            .spawn()
            .unwrap();
        let c = old::content::Client::connect(&caller, server.endpoint().addr())
            .await
            .unwrap();
        assert!(matches!(
            c.get(0).await,
            Err(Error::Remote(
                keylink::RemoteError::UnsupportedResponse { .. }
            ))
        ));
        let guessed = keylink::BlobHandle::new(&caller, server.endpoint().addr(), original);
        assert!(
            matches!(guessed.verified_len().await, Err(Error::BlobRefused { .. })),
            "a conversion refusal must grant no address"
        );
        assert!(matches!(
            c.get(1).await,
            Err(Error::Remote(
                keylink::RemoteError::UnsupportedResponse { .. }
            ))
        ));
        assert!(
            matches!(guessed.verified_len().await, Err(Error::BlobRefused { .. })),
            "an address-changing conversion must not grant the current address"
        );
        let substituted = keylink::BlobHandle::new(&caller, server.endpoint().addr(), replacement);
        assert!(
            matches!(
                substituted.verified_len().await,
                Err(Error::BlobRefused { .. })
            ),
            "an address-changing conversion must not grant its substituted address"
        );
        assert!(
            matches!(
                c.get(4).await,
                Err(Error::Remote(
                    keylink::RemoteError::UnsupportedResponse { .. }
                ))
            ),
            "conversion cannot broaden a blob into a closure with the same root"
        );
        let (metadata, handle) = c.get(2).await.unwrap();
        assert_eq!(metadata, 12);
        assert_eq!(handle.id(), original);
        assert_eq!(handle.verified_len().await.unwrap(), 8);
        // The shared record and both negotiated replies carry the same address.
        // Replay relies on the existing grant; it does not renew one.
        let key = keylink::IdempotencyKey::new("blob-replay").unwrap();
        let old_raw =
            keylink::RpcClient::connect(&caller, server.endpoint().addr(), old::content::lineage())
                .await
                .unwrap();
        let _ = old_raw
            .with_idempotency(key.clone())
            .call(
                old::content::Request::Get { mode: 2 },
                keylink::Body::empty(),
            )
            .await
            .unwrap();
        let current_raw =
            keylink::RpcClient::connect(&caller, server.endpoint().addr(), content::lineage())
                .await
                .unwrap();
        assert!(matches!(
            current_raw
                .with_idempotency(key)
                .call(content::Request::Get { mode: 2 }, keylink::Body::empty())
                .await,
            Err(Error::Remote(keylink::RemoteError::Idempotency(
                keylink::IdempotencyRefusal::RequestMismatch
            )))
        ));
        assert_eq!(guessed.verified_len().await.unwrap(), 8);
        let older_server = node().await;
        let _old_running = keylink::Server::builder(&older_server)
            .service(
                old::content::service(Handler(original), Allowlist::anyone())
                    .with_blobs(&store)
                    .build(),
            )
            .spawn()
            .unwrap();
        let newer_client = content::Client::connect(&caller, older_server.endpoint().addr())
            .await
            .unwrap();
        assert!(
            matches!(
                newer_client.get(3).await,
                Err(Error::UnsupportedResponse { .. })
            ),
            "client reply upgrades cannot substitute the blob address either"
        );
    }
}

mod attributed {
    mod old {
        keylink::service! {
            service Attributed; version 1;
            #[serde(rename = "wire-name")]
            rpc Echo(value: u32) -> u32;
            #[cfg(any())]
            rpc Hidden(value: TypeThatMustNeverResolve) -> u8;
            #[cfg_attr(all(), cfg(any()))]
            rpc Nested(value: AnotherAbsentType) -> u8;
        }
    }
    keylink::service! {
        service Attributed; version 2;
        #[serde(rename = "wire-name")]
        rpc Echo(value: u32) -> u32;
        #[cfg(any())]
        rpc Hidden(value: TypeThatMustNeverResolve) -> u8;
        #[cfg_attr(all(), cfg(any()))]
        rpc Nested(value: AnotherAbsentType) -> u8;
        previous 1 {
            hash old::attributed::VERSION.hash(); keyed_hash old::attributed::KEYED_VERSION.hash();
            #[serde(rename = "wire-name")]
            rpc Echo(value: u32) -> u32;
            #[cfg(any())]
            rpc Hidden(value: TypeThatMustNeverResolve) -> u8;
            #[cfg_attr(all(), cfg(any()))]
            rpc Nested(value: AnotherAbsentType) -> u8;
        }
    }
    impl attributed::version_1::Convert for attributed::version_1::Step {
        fn upgrade_request(
            _: attributed::version_1::Request,
        ) -> Result<attributed::Request, keylink::Unsupported> {
            unreachable!()
        }
        fn downgrade_request(
            _: attributed::Request,
        ) -> Result<attributed::version_1::Request, keylink::Unsupported> {
            unreachable!()
        }
        fn upgrade_response(
            _: attributed::version_1::Response,
        ) -> Result<attributed::Response, keylink::Unsupported> {
            unreachable!()
        }
        fn downgrade_response(
            _: attributed::Response,
        ) -> Result<attributed::version_1::Response, keylink::Unsupported> {
            unreachable!()
        }
    }
    struct Handler;
    impl attributed::Handler for Handler {
        async fn echo(&self, _cx: &keylink::Context, value: u32) -> u32 {
            value
        }
    }
    /// Watched red: retain only cfg attributes in plain frozen declarations,
    /// dropping serde metadata. Compilation failed with E0080 at this fixture's
    /// pinned hash, not at an absent cfg type. Restored full metadata.
    #[tokio::test]
    async fn previous_keeps_serde_and_configuration_attributes() {
        let server = super::node().await;
        let caller = super::node().await;
        let _running = attributed::serve(&server, Handler, keylink::Allowlist::anyone()).unwrap();
        let c = old::attributed::Client::connect(&caller, server.endpoint().addr())
            .await
            .unwrap();
        assert_eq!(c.echo(19).await.unwrap(), 19);
        assert_eq!(attributed::version_1::VERSION, old::attributed::VERSION);
    }
}
