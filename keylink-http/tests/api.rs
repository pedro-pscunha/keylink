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
