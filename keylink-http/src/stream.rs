//! Bounded metadata and body adapters for streaming HTTP routes.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use http::{header, HeaderMap};

const ARGUMENTS_HEADER: &str = "keylink-arguments";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MetadataError {
    EncodedTooLong,
    DecodedTooLong,
    Invalid,
}

pub(crate) fn is_octet_stream(headers: &HeaderMap) -> bool {
    let mut values = headers.get_all(header::CONTENT_TYPE).iter();
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }
    let Ok(value) = value.to_str() else {
        return false;
    };
    let Ok(media) = value.parse::<mime::Mime>() else {
        return false;
    };
    media.type_() == mime::APPLICATION
        && media.subtype() == mime::OCTET_STREAM
        && media.suffix().is_none()
        && media.params().next().is_none()
}

pub(crate) fn arguments(
    headers: &HeaderMap,
    decoded_limit: usize,
) -> Result<Vec<u8>, MetadataError> {
    let mut values = headers.get_all(ARGUMENTS_HEADER).iter();
    let value = values.next().ok_or(MetadataError::Invalid)?;
    if values.next().is_some() {
        return Err(MetadataError::Invalid);
    }
    let value = value.to_str().map_err(|_| MetadataError::Invalid)?;
    let encoded_limit = decoded_limit
        .checked_add(2)
        .ok_or(MetadataError::EncodedTooLong)?
        / 3;
    let encoded_limit = encoded_limit
        .checked_mul(4)
        .ok_or(MetadataError::EncodedTooLong)?;
    if value.len() > encoded_limit {
        return Err(MetadataError::EncodedTooLong);
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| MetadataError::Invalid)?;
    if decoded.len() > decoded_limit {
        return Err(MetadataError::DecodedTooLong);
    }
    std::str::from_utf8(&decoded).map_err(|_| MetadataError::Invalid)?;
    if URL_SAFE_NO_PAD.encode(&decoded) != value {
        return Err(MetadataError::Invalid);
    }
    Ok(decoded)
}

/// The header name carrying a `recv` method's declared return value.
const RESULT_HEADER: &str = "keylink-result";

/// Encode one declared return value into the bounded `Keylink-Result` header.
///
/// The ceiling is checked against the *decoded* JSON, not the base64url. That
/// is the same limit [`arguments`] enforces on the way in, so one configured
/// number governs both directions and neither can be tightened by accident.
/// Encoding only ever grows the value by a fixed ratio, so bounding the decoded
/// form bounds what reaches the wire.
pub(crate) fn result_header(
    value: &[u8],
    decoded_limit: usize,
) -> Result<(http::HeaderName, http::HeaderValue), MetadataError> {
    if value.len() > decoded_limit {
        return Err(MetadataError::DecodedTooLong);
    }
    let name = http::HeaderName::from_static(RESULT_HEADER);
    // Unreachable in practice — every base64url byte is a valid header byte —
    // but an adapter that cannot say "no" here would have to `expect`, and an
    // impossible branch costs one line while a panic costs the process.
    let encoded = http::HeaderValue::from_str(&URL_SAFE_NO_PAD.encode(value))
        .map_err(|_| MetadataError::Invalid)?;
    Ok((name, encoded))
}

/// Anything Keylink hands over one verified chunk at a time.
///
/// Two implementors and one consumer, which is what makes this a seam rather
/// than a hypothetical one: `BodyReader` carries a `recv` reply and
/// `BlobReader` carries content-addressed bytes, and the download path cares
/// about neither. Copying the loop instead would give the blob routes their
/// own bounded-stream bug to fix twice.
pub(crate) trait Chunks {
    fn next_chunk(
        &mut self,
    ) -> impl std::future::Future<Output = keylink::Result<Option<axum::body::Bytes>>> + Send;
}

impl Chunks for keylink::BodyReader {
    async fn next_chunk(&mut self) -> keylink::Result<Option<axum::body::Bytes>> {
        keylink::BodyReader::next_chunk(self).await
    }
}

impl Chunks for keylink::BlobReader {
    async fn next_chunk(&mut self) -> keylink::Result<Option<axum::body::Bytes>> {
        keylink::BlobReader::next_chunk(self).await
    }
}

/// Stream a Keylink response body as HTTP bytes, stopping at `limit`.
///
/// The limit is inclusive and counted as bytes arrive, because an undeclared
/// length is exactly the case where nothing can be refused up front. Crossing
/// it ends the HTTP body in an error, which is all a response can still say
/// once its headers have gone.
pub(crate) fn download_body<R: Chunks + Send + 'static>(reader: R, limit: u64) -> axum::body::Body {
    /// Reading, or finished for good. Without the second state an error would
    /// be yielded forever, because `unfold` ends only on `None`.
    enum Download<R> {
        Reading(R, u64),
        Ended,
    }

    axum::body::Body::from_stream(n0_future::stream::unfold(
        Download::<R>::Reading(reader, 0),
        move |state| async move {
            let Download::Reading(mut reader, observed) = state else {
                return None;
            };
            match reader.next_chunk().await {
                Ok(None) => None,
                // Deliberately not the upstream text: past the headers there is
                // no envelope left to redact it in.
                Err(_) => Some((
                    Err(std::io::Error::other("the response stream failed")),
                    Download::Ended,
                )),
                Ok(Some(chunk)) => {
                    let observed = observed.saturating_add(chunk.len() as u64);
                    if observed > limit {
                        return Some((
                            Err(std::io::Error::other(
                                "the response stream exceeds its limit",
                            )),
                            Download::Ended,
                        ));
                    }
                    Some((Ok(chunk), Download::Reading(reader, observed)))
                }
            }
        },
    ))
}

pub(crate) fn upload_body(
    body: axum::body::Body,
    declared_len: Option<u64>,
    limit: u64,
) -> keylink::Body {
    use n0_future::StreamExt as _;

    let mut observed = 0_u64;
    let chunks = body.into_data_stream().map(move |result| {
        let chunk = result.map_err(std::io::Error::other)?;
        observed = observed
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| std::io::Error::other("request stream exceeds its limit"))?;
        if observed > limit {
            return Err(std::io::Error::other("request stream exceeds its limit"));
        }
        Ok(chunk)
    });
    keylink::Body::from_reader(tokio_util::io::StreamReader::new(chunks), declared_len)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use http::{header, HeaderMap, HeaderValue};

    use super::{arguments, is_octet_stream, MetadataError};

    fn headers(name: &'static str, values: &[&str]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for value in values {
            headers.append(name, HeaderValue::from_str(value).expect("header value"));
        }
        headers
    }

    #[test]
    fn send_requires_one_exact_octet_stream_content_type() {
        for accepted in ["application/octet-stream", "APPLICATION/OCTET-STREAM"] {
            let headers = headers(header::CONTENT_TYPE.as_str(), &[accepted]);
            assert!(is_octet_stream(&headers), "{accepted}");
        }
        for rejected in [
            vec![],
            vec!["application/octet-stream; charset=binary"],
            vec!["application/octet-stream+json"],
            vec!["text/octet-stream"],
            vec!["application/json"],
            vec!["application/octet-stream", "application/octet-stream"],
        ] {
            let headers = headers(header::CONTENT_TYPE.as_str(), &rejected);
            assert!(!is_octet_stream(&headers), "{rejected:?}");
        }
    }

    #[test]
    fn send_arguments_header_enforces_canonical_base64url() {
        for rejected in [
            vec![],
            vec!["e30="],
            vec!["e 30"],
            vec!["e3+"],
            vec!["e3/"],
            vec!["e31"],
            vec!["_w"],
            vec!["e30", "e30"],
        ] {
            let headers = headers("keylink-arguments", &rejected);
            assert_eq!(
                arguments(&headers, 16),
                Err(MetadataError::Invalid),
                "{rejected:?}"
            );
        }
        assert_eq!(
            arguments(&headers("keylink-arguments", &["e30"]), 16).as_deref(),
            Ok("{}".as_bytes())
        );
    }

    #[test]
    fn send_arguments_header_checks_encoded_limit_before_decode() {
        let headers = headers("keylink-arguments", &["%%%%%"]);
        assert_eq!(arguments(&headers, 3), Err(MetadataError::EncodedTooLong));
        assert_eq!(
            arguments(&headers, usize::MAX),
            Err(MetadataError::EncodedTooLong)
        );
    }

    #[test]
    fn send_arguments_json_accepts_limit_and_rejects_limit_plus_one() {
        let at = headers("keylink-arguments", &["e30"]);
        assert_eq!(arguments(&at, 2).as_deref(), Ok("{}".as_bytes()));
        assert_eq!(arguments(&at, 1), Err(MetadataError::DecodedTooLong));
    }
}
