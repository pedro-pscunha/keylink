//! Bounded parsing for raw URI path segments.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Shape {
    Segment { max_bytes: usize },
    BlobId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ParseError {
    EncodedTooLong,
    DecodedTooLong,
    InvalidEncoding,
    InvalidBlobId,
}

pub(crate) fn parse(raw: &str, shape: Shape) -> Result<String, ParseError> {
    if shape == Shape::BlobId {
        return if raw.len() == 64
            && raw
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            Ok(raw.to_owned())
        } else {
            Err(ParseError::InvalidBlobId)
        };
    }

    let decoded_limit = match shape {
        Shape::Segment { max_bytes } => max_bytes,
        Shape::BlobId => unreachable!(),
    };
    let encoded_limit = decoded_limit
        .checked_mul(3)
        .ok_or(ParseError::EncodedTooLong)?;
    if raw.len() > encoded_limit {
        return Err(ParseError::EncodedTooLong);
    }

    let bytes = raw.as_bytes();
    let mut decoded = Vec::with_capacity(raw.len().min(decoded_limit));
    let mut index = 0;
    while index < bytes.len() {
        let byte = if bytes[index] == b'%' {
            let pair = bytes
                .get(index + 1..index + 3)
                .ok_or(ParseError::InvalidEncoding)?;
            let high = hex(pair[0]).ok_or(ParseError::InvalidEncoding)?;
            let low = hex(pair[1]).ok_or(ParseError::InvalidEncoding)?;
            index += 3;
            (high << 4) | low
        } else {
            let byte = bytes[index];
            index += 1;
            byte
        };
        if decoded.len() == decoded_limit {
            return Err(ParseError::DecodedTooLong);
        }
        decoded.push(byte);
    }
    String::from_utf8(decoded).map_err(|_| ParseError::InvalidEncoding)
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{parse, ParseError, Shape};

    fn utf8_bytes(len: usize) -> String {
        let mut value = "é".repeat(len / 2);
        if len % 2 == 1 {
            value.push('a');
        }
        assert_eq!(value.len(), len);
        value
    }

    #[test]
    fn raw_segment_parser_enforces_each_shape_before_allocation() {
        let method_limit = 3 * 3;
        let method_below = &"%61%62%6"[..method_limit - 1];
        assert_eq!(
            parse(method_below, Shape::Segment { max_bytes: 3 }),
            Err(ParseError::InvalidEncoding)
        );
        assert_eq!(
            parse("%61%62%63", Shape::Segment { max_bytes: 3 }).as_deref(),
            Ok("abc")
        );
        assert_eq!(
            parse("%61%62%63%", Shape::Segment { max_bytes: 3 }),
            Err(ParseError::EncodedTooLong)
        );

        let actor_at = "%61".repeat(keylink::MAX_KEY_LEN);
        let actor_below = &actor_at[..actor_at.len() - 1];
        assert_eq!(
            parse(
                actor_below,
                Shape::Segment {
                    max_bytes: keylink::MAX_KEY_LEN
                }
            ),
            Err(ParseError::InvalidEncoding)
        );
        assert_eq!(
            parse(
                &actor_at,
                Shape::Segment {
                    max_bytes: keylink::MAX_KEY_LEN
                }
            )
            .as_deref(),
            Ok("a".repeat(keylink::MAX_KEY_LEN).as_str())
        );
        assert_eq!(
            parse(
                &(actor_at + "%"),
                Shape::Segment {
                    max_bytes: keylink::MAX_KEY_LEN
                }
            ),
            Err(ParseError::EncodedTooLong)
        );
        for len in [127, 128] {
            let value = utf8_bytes(len);
            assert_eq!(
                parse(
                    &value,
                    Shape::Segment {
                        max_bytes: keylink::MAX_KEY_LEN
                    }
                )
                .as_deref(),
                Ok(value.as_str())
            );
        }
        assert_eq!(
            parse(
                &utf8_bytes(129),
                Shape::Segment {
                    max_bytes: keylink::MAX_KEY_LEN
                }
            ),
            Err(ParseError::DecodedTooLong)
        );

        for raw in ["a".repeat(63), "a".repeat(65)] {
            assert_eq!(parse(&raw, Shape::BlobId), Err(ParseError::InvalidBlobId));
        }
        assert!(parse(&"a".repeat(64), Shape::BlobId).is_ok());
        for raw in [
            "A".repeat(64),
            "g".repeat(64),
            format!("%{}", "a".repeat(63)),
        ] {
            assert_eq!(parse(&raw, Shape::BlobId), Err(ParseError::InvalidBlobId));
        }
    }
}
