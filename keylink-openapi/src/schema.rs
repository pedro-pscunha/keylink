//! The schema shapes this crate builds by hand rather than deriving.

use serde_json::{json, Value};

/// A schema built from a Rust type by `schemars`, in draft 2020-12.
///
/// A newtype rather than a bare `Value`, so a caller cannot pass an OpenAPI
/// fragment where a JSON Schema belongs.
#[derive(Debug, Clone)]
pub struct RawSchema(Value);

impl RawSchema {
    /// Wrap a schema `schemars` produced.
    #[must_use]
    pub const fn new(value: Value) -> Self {
        Self(value)
    }

    /// The schema as JSON.
    #[must_use]
    pub fn into_value(self) -> Value {
        self.0
    }
}

/// The one raw-octet schema, shared by `send`, `recv` and blob bytes.
///
/// `type: string` plus `contentMediaType` is how draft 2020-12 says "these are
/// bytes". Describing a byte stream as a JSON string WITHOUT the media type
/// would tell a generator to parse it, which is the failure this shape exists
/// to prevent.
#[must_use]
pub fn octet_stream_schema() -> Value {
    json!({
        "type": "string",
        "contentMediaType": "application/octet-stream",
        "description": "Raw bytes. Not JSON, and not base64 on the wire.",
    })
}

/// The one refusal shape every route answers with.
///
/// Built here rather than derived, because the runtime's error body is
/// assembled by `keylink-http` from a fieldless enum and carries no Rust type
/// to derive from. Kept in step with it by
/// `the_error_envelope_matches_the_runtime_one`.
#[must_use]
pub fn error_envelope() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["error"],
        "properties": {
            "error": {
                "type": "object",
                "additionalProperties": false,
                "required": ["code", "message"],
                "properties": {
                    "code": {
                        "type": "string",
                        "description": "A stable machine-readable category.",
                    },
                    "message": {
                        "type": "string",
                        "description": "A fixed sentence. Never carries caller input.",
                    },
                },
            },
        },
    })
}
