#![allow(clippy::expect_used)]

use http::HeaderValue;
use keylink_http::{Challenge, ConfigError, JsonLimits, Principal};

#[test]
fn principal_accepts_its_exact_inclusive_boundary() {
    Principal::new(vec![b'x'; 255]).expect("limit minus one");
    Principal::new(vec![b'x'; 256]).expect("exact limit");
    assert!(matches!(
        Principal::new(vec![b'x'; 257]),
        Err(ConfigError::PrincipalTooLong { max: 256 })
    ));
}

#[test]
fn principal_and_constructor_errors_redact_namespace_bytes() {
    let principal = Principal::new(b"tenant-secret").expect("valid principal");
    assert_eq!(format!("{principal:?}"), "Principal([REDACTED])");

    let error = Principal::new(vec![b's'; 257]).expect_err("over the finite cap");
    for rendered in [format!("{error}"), format!("{error:?}")] {
        assert!(!rendered.contains("secret"));
        assert!(!rendered.contains("ssss"));
    }
    assert!(matches!(
        Principal::new(b""),
        Err(ConfigError::EmptyPrincipal)
    ));
}

#[test]
fn challenge_refuses_empty_and_non_text_header_values() {
    assert!(matches!(
        Challenge::new(HeaderValue::from_static("")),
        Err(ConfigError::InvalidChallenge)
    ));
    let opaque = HeaderValue::from_bytes(&[0xff]).expect("HTTP permits opaque header bytes");
    assert!(matches!(
        Challenge::new(opaque),
        Err(ConfigError::InvalidChallenge)
    ));
    Challenge::new(HeaderValue::from_static("Bearer realm=keylink")).expect("valid challenge");
}

#[test]
fn json_limits_require_two_positive_ceilings() {
    assert!(matches!(
        JsonLimits::new(0, 1),
        Err(ConfigError::ZeroRequestLimit)
    ));
    assert!(matches!(
        JsonLimits::new(1, 0),
        Err(ConfigError::ZeroResponseLimit)
    ));
    JsonLimits::new(1, 1).expect("tiny positive limits are valid");
}

#[test]
fn http_surface_is_immutable_and_streaming_is_explicit() {
    use keylink_http::{Addressing, HttpSurface, StreamingLimits};

    let singleton = HttpSurface::new("/v1/inventory", Addressing::Singleton).expect("valid prefix");
    assert_eq!(singleton.prefix(), "/v1/inventory");
    assert_eq!(singleton.addressing(), Addressing::Singleton);
    assert!(singleton.streaming().is_none());

    let limits = StreamingLimits::new(1024, 4096, 8192, 16).expect("positive streaming limits");
    let actors = HttpSurface::new("/v1/inventory", Addressing::Actors)
        .expect("valid prefix")
        .with_streaming(limits);
    assert_eq!(actors.addressing(), Addressing::Actors);
    assert_eq!(actors.streaming(), Some(limits));
}

/// Streaming is opt-in at the surface, so an operator who never supplied a
/// finite upload ceiling never gets a route that would need one.
#[test]
fn streaming_routes_require_explicit_limits() {
    use keylink_http::__contract::RouteKind;
    use keylink_http::{Addressing, HttpSurface, StreamingLimits};

    let plain = HttpSurface::new("/v1", Addressing::Singleton).expect("valid surface");
    assert!(
        plain.routes().all(|route| route.kind() == RouteKind::Rpc),
        "a surface without limits offers no streaming route"
    );

    let limits = StreamingLimits::new(1024, 4096, 8192, 16).expect("positive streaming limits");
    let streaming = HttpSurface::new("/v1", Addressing::Singleton)
        .expect("valid surface")
        .with_streaming(limits);
    assert!(streaming
        .routes()
        .any(|route| route.kind() == RouteKind::Send));
}

#[test]
fn streaming_limits_reject_each_zero_field() {
    use keylink_http::StreamingLimits;

    for values in [(0, 1, 1, 1), (1, 0, 1, 1), (1, 1, 0, 1), (1, 1, 1, 0)] {
        assert!(StreamingLimits::new(values.0, values.1, values.2, values.3).is_err());
    }
}

#[test]
fn hidden_contract_exposes_the_runtime_route_and_error_inventories() {
    use keylink_http::__contract::{ErrorKind, RouteKind};
    use keylink_http::{Addressing, HttpSurface};

    let surface = HttpSurface::new("/v1", Addressing::Singleton).expect("valid surface");
    assert_eq!(
        surface
            .routes()
            .map(|route| route.kind())
            .collect::<Vec<_>>(),
        [RouteKind::Rpc]
    );
    assert!(ErrorKind::ALL
        .iter()
        .all(|kind| !kind.spec().code().is_empty()));
}

keylink::service! {
    service Silent;
    // `tell` is the one declaration that produces NO external method, so this
    // is a service whose generated inventory is empty on both address spaces.
    tell Ignored(value: u64) -> ();
}

#[derive(Clone)]
struct Quiet;

impl silent::Handler for Quiet {
    async fn ignored(&self, _cx: &keylink::Context, _value: u64) {}
}

#[derive(Clone)]
struct Anyone;

impl keylink_http::Access for Anyone {
    async fn authorize(
        &self,
        _call: keylink_http::Call<'_>,
    ) -> Result<(), keylink_http::AccessError> {
        Ok(())
    }
}

/// A service with nothing to serve is refused at configuration, not per request.
///
/// `METHODS` carries one entry per `rpc`, `send`, `recv` or `blob`, so a
/// `tell`-only service produces an EMPTY generated inventory. A surface that
/// mounted one would install `/rpc/{method}` and answer every request from it,
/// which says at request time what a builder can say at startup.
///
/// The refusal used to be written on `ActorGateway::router` alone, while its
/// own comment stated the rule as general. It lives in `install` now, which
/// both routers share, and this row is what holds it there — there was no test
/// for it on either side.
///
/// **Only the singleton half is exercised, and the keyed half is unreachable
/// rather than untested.** `keylink::Registry<M>` implements `Servable` only
/// for a mailbox type carrying an `Rpc` request, and a `tell`-only service has
/// none — `registry.service(..)` does not compile for `Silent`. So no keyed
/// server for such a service can exist, and no `ActorGateway` over it can ever
/// hold a connected client. The guard the two share is exercised by the half
/// that can be built.
///
/// Watched red with `scripts/break-once.sh` (row `HK45`) by replacing
/// `    if mount.methods().is_empty() {`
/// with `    if false {`. The router answered `Ok` where this asserts
/// `MissingRuntimeRoute`.
#[tokio::test]
async fn an_empty_generated_inventory_is_refused_at_configuration() {
    let server_node = keylink::Node::from_endpoint(
        iroh::Endpoint::bind(iroh::endpoint::presets::Minimal)
            .await
            .expect("local endpoint binds"),
    );
    let client_node = keylink::Node::from_endpoint(
        iroh::Endpoint::bind(iroh::endpoint::presets::Minimal)
            .await
            .expect("local endpoint binds"),
    );
    let _server = keylink::Server::builder(&server_node)
        .service(silent::service(Quiet, keylink::Allowlist::anyone()).build())
        .spawn()
        .expect("a service with nothing external to serve");
    let client = silent::Client::connect(&client_node, server_node.endpoint().addr())
        .await
        .expect("client connects");

    let router = keylink_http::Gateway::<silent::External, _>::new(
        client,
        Anyone,
        Challenge::new(HeaderValue::from_static("Bearer realm=keylink")).expect("valid challenge"),
    )
    .router::<()>(
        keylink_http::HttpSurface::new("/v1", keylink_http::Addressing::Singleton)
            .expect("valid singleton surface"),
    );
    assert!(matches!(router, Err(ConfigError::MissingRuntimeRoute)));
}
