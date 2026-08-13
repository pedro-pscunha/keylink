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
