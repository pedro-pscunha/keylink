#![allow(clippy::expect_used)]

use keylink_http::{ConfigError, Principal};

#[test]
fn principal_is_bounded_and_redacted() {
    let principal = Principal::new(b"tenant-secret").expect("valid principal");
    assert_eq!(format!("{principal:?}"), "Principal([REDACTED])");
    assert!(matches!(
        Principal::new(b""),
        Err(ConfigError::EmptyPrincipal)
    ));
    assert!(matches!(
        Principal::new(vec![b'x'; 257]),
        Err(ConfigError::PrincipalTooLong { max: 256 })
    ));
}

use keylink_http::JsonLimits;

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

use http::{Extensions, HeaderMap, HeaderValue};
use keylink_http::{Access, AccessError, Call, Challenge};

#[derive(Clone)]
struct MethodAccess;

impl Access for MethodAccess {
    async fn authorize(&self, call: Call<'_>) -> Result<(), AccessError> {
        if call.method == "Allowed" && call.extensions.get::<u64>() == Some(&7) {
            Ok(())
        } else {
            Err(AccessError::Forbidden)
        }
    }
}

#[tokio::test]
async fn challenge_is_validated_once_and_access_can_be_async() {
    assert!(matches!(
        Challenge::new(HeaderValue::from_static("")),
        Err(ConfigError::InvalidChallenge)
    ));
    Challenge::new(HeaderValue::from_static("Bearer realm=keylink")).expect("valid challenge");

    let principal = Principal::new(b"tenant").expect("valid principal");
    let mut extensions = Extensions::new();
    extensions.insert(7_u64);
    MethodAccess
        .authorize(Call {
            extensions: &extensions,
            headers: &HeaderMap::new(),
            principal: &principal,
            service: "Inventory",
            method: "Allowed",
        })
        .await
        .expect("allowed");
}
