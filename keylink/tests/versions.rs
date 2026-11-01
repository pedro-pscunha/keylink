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
            hash v2::ledger::VERSION.hash();
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

/// A retry through a different negotiated version still meets the same ledger.
#[tokio::test]
async fn every_version_shares_one_current_reply_ledger() {
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
    assert_eq!(
        new.with_idempotency(key).get(1).await.unwrap(),
        Some(v3::Entry {
            qty: 7,
            memo: Some("late".into())
        })
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
            assert_eq!(
                c.with_idempotency(key.clone()).get(1).await.unwrap(),
                Some(v3::Entry { qty: 7, memo: None })
            );
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
                    keylink::IdempotencyRefusal::Forgotten
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
        previous 2 { hash two::chain::VERSION.hash(); rpc Echo(n: u64) -> u64; }
        previous 1 { hash one::chain::VERSION.hash(); rpc Echo(n: u64) -> u64; }
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
