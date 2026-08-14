//! OpenAPI 3.1 and standalone JSON Schema for a `keylink-http` surface.
//!
//! Opt-in and separate on purpose. `schemars` and this crate's JSON model live
//! here and nowhere else, so a `service!` user who wants no documents pays for
//! neither — proven by `cargo tree -p keylink` and `-p keylink-http` carrying no
//! schema edge.
//!
//! # What a caller does
//!
//! Invoke [`document_service!`] once, in the crate that wrote the `service!`
//! block, and it implements [`Documented`] for that module's `External`. Then
//! build a [`Document`] from the same [`HttpSurface`] the gateway is mounted
//! with, so the description and the runtime cannot describe different routes.
//!
//! # What it will not do
//!
//! No Swagger UI, no YAML, no CORS, no runtime reflection, no generated SDKs,
//! and no automatic mounting — [`router`] exists and is never installed for
//! you, because a document is a decision about what a deployment publishes.

#![deny(missing_docs)]

use std::collections::BTreeMap;

use keylink_http::__contract::{ErrorKind, Route, RouteKind};
use keylink_http::{Addressing, HttpSurface};
use serde_json::{json, Map, Value};

mod digest;
mod document_macro;
mod endpoints;
mod schema;

#[doc(hidden)]
pub use keylink_http::__contract;

pub use digest::component_name;
pub use endpoints::router;
pub use schema::{octet_stream_schema, RawSchema};

/// The declaration list of one documented service.
///
/// Implemented by [`document_service!`] for a generated `External` marker, and
/// by nothing else. The trait is foreign to every crate that writes a
/// `service!`, and the macro that implements it is reachable only from that
/// crate — which is what keeps one service from being documented twice, in two
/// crates, two different ways.
pub trait Documented {
    /// The service name, as declared.
    const SERVICE: &'static str;

    /// Every external declaration, in declaration order.
    ///
    /// Returns owned schemas rather than borrowing a table, because a schema is
    /// built by `schemars` at call time and there is nowhere static to put it.
    fn operations() -> Vec<Operation>;
}

/// One documented declaration: what it is called, what it takes, what it gives.
#[derive(Debug, Clone)]
pub struct Operation {
    /// The declared method name, exactly as the router matches it.
    pub method: &'static str,
    /// Which route family serves it.
    pub kind: RouteKind,
    /// Whether the actor surface serves it as well as the singleton one.
    pub keyed: bool,
    /// The request contract: an object with one property per argument, and
    /// `additionalProperties: false` because the runtime decodes with
    /// `deny_unknown_fields`.
    pub request: Value,
    /// The response contract: the declared return type, serialized.
    pub response: Value,
}

/// How a deployment authenticates, stated rather than invented.
///
/// A required argument of [`DocumentConfig::new`], so a document cannot be
/// built without one. There is no default and there is no `Bearer` fallback: a
/// gateway reads its principal from an extension a deployment inserts, and
/// guessing which header carried it would publish a credential scheme nobody
/// implements.
#[derive(Debug, Clone)]
pub enum Security {
    /// The deployment authenticates ahead of this gateway and the document says
    /// so, with no scheme invented. Rendered as an `x-keylink-authentication`
    /// extension carrying this description.
    External {
        /// What a caller has to do, in the deployment's own words.
        description: String,
    },
    /// An HTTP authentication scheme the deployment really implements.
    Http {
        /// The scheme token, e.g. `bearer`.
        scheme: String,
        /// The optional `bearerFormat`.
        format: Option<String>,
    },
    /// An API key in a named header.
    ApiKey {
        /// The header the deployment reads.
        header: String,
    },
}

/// Everything a document needs that the surface does not carry.
#[derive(Debug, Clone)]
pub struct DocumentConfig {
    title: String,
    version: String,
    security: Security,
}

impl DocumentConfig {
    /// Build a configuration. [`Security`] is required, not optional.
    #[must_use]
    pub fn new(title: impl Into<String>, version: impl Into<String>, security: Security) -> Self {
        Self {
            title: title.into(),
            version: version.into(),
            security,
        }
    }
}

/// A rendered document and the bytes it serializes to.
#[derive(Debug, Clone)]
pub struct Document {
    value: Value,
    bytes: Vec<u8>,
}

impl Document {
    /// The document as JSON.
    #[must_use]
    pub const fn value(&self) -> &Value {
        &self.value
    }

    /// The exact bytes an endpoint serves. Deterministic across runs and
    /// processes: every map is a `BTreeMap` and every list is built in
    /// declaration or inventory order.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Errors a document can be refused with.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DocumentError {
    /// Two operations would occupy one path and verb.
    PathCollision {
        /// The path both wanted.
        path: String,
    },
    /// Two operations would share one operation id.
    OperationIdCollision {
        /// The id both wanted.
        id: String,
    },
    /// Two different schemas would occupy one component name.
    ComponentCollision {
        /// The component name both wanted.
        name: String,
    },
    /// A `$ref` names a component that was never registered.
    UnresolvedReference {
        /// The reference as rendered.
        reference: String,
    },
}

impl std::fmt::Display for DocumentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PathCollision { path } => write!(f, "two operations claim the path {path}"),
            Self::OperationIdCollision { id } => write!(f, "two operations claim the id {id}"),
            Self::ComponentCollision { name } => {
                write!(f, "two schemas claim the component {name}")
            }
            Self::UnresolvedReference { reference } => {
                write!(f, "the reference {reference} resolves to nothing")
            }
        }
    }
}

impl std::error::Error for DocumentError {}

/// Build the OpenAPI 3.1 document for one documented service on one surface.
///
/// # Errors
///
/// Every variant of [`DocumentError`]: a document that would overwrite one of
/// its own entries is refused rather than silently keeping the last one.
pub fn document<D: Documented>(
    surface: &HttpSurface,
    config: &DocumentConfig,
) -> Result<Document, DocumentError> {
    let mut components: BTreeMap<String, Value> = BTreeMap::new();
    let mut paths: BTreeMap<String, Value> = BTreeMap::new();
    let mut ids: BTreeMap<String, ()> = BTreeMap::new();

    let envelope = schema::error_envelope();
    __register(&mut components, "ErrorEnvelope", envelope)?;

    let keyed = surface.addressing() == Addressing::Actors;
    for mut operation in D::operations() {
        if keyed && !operation.keyed {
            continue;
        }
        hoist(
            &mut operation.request,
            &mut components,
            "#/components/schemas",
        )?;
        hoist(
            &mut operation.response,
            &mut components,
            "#/components/schemas",
        )?;
        for route in surface.routes() {
            if !serves(operation.kind, route.kind()) {
                continue;
            }
            let path = render_path(surface.prefix(), route.template(), operation.method);
            let id = operation_id(D::SERVICE, &operation, route);
            if ids.insert(id.clone(), ()).is_some() {
                return Err(DocumentError::OperationIdCollision { id });
            }
            let item = operation_object(D::SERVICE, &operation, route);
            let verb = route.method().as_str().to_ascii_lowercase();
            let entry = paths.entry(path.clone()).or_insert_with(|| json!({}));
            let Some(map) = entry.as_object_mut() else {
                return Err(DocumentError::PathCollision { path });
            };
            if map.insert(verb, item).is_some() {
                return Err(DocumentError::PathCollision { path });
            }
        }
    }

    let mut root = Map::new();
    root.insert("openapi".to_owned(), json!("3.1.0"));
    root.insert(
        "info".to_owned(),
        json!({ "title": config.title, "version": config.version }),
    );
    if let Some(limits) = surface.streaming() {
        // The bounds a caller meets as a `413` or a `503` if they are not
        // published. Named as an extension because OpenAPI has no field for
        // "this many bytes", and unpublished they are learned by being refused.
        root.insert(
            "x-keylink-limits".to_owned(),
            json!({
                "maxUploadBytes": limits.max_upload_bytes(),
                "maxDownloadBytes": limits.max_download_bytes(),
                "metadataJsonBytes": limits.metadata_json_bytes(),
                "maxBlobTickets": limits.max_blob_tickets(),
            }),
        );
    }
    root.insert(
        "tags".to_owned(),
        json!([{ "name": D::SERVICE, "description": "One keylink service." }]),
    );
    root.insert("paths".to_owned(), to_value(paths));
    root.insert(
        "components".to_owned(),
        json!({ "schemas": to_value(components) }),
    );
    match &config.security {
        // No scheme is invented for a deployment that authenticates ahead of
        // this gateway. An extension says so; a `securitySchemes` entry would
        // publish a credential nobody reads.
        Security::External { description } => {
            root.insert(
                "x-keylink-authentication".to_owned(),
                json!({ "kind": "external", "description": description }),
            );
        }
        // A scheme the deployment really implements goes where a generator
        // looks for one, and carries a `security` requirement so it is applied
        // rather than merely described.
        Security::Http { scheme, format } => {
            let mut entry = json!({ "type": "http", "scheme": scheme });
            if let (Some(map), Some(format)) = (entry.as_object_mut(), format) {
                map.insert("bearerFormat".to_owned(), json!(format));
            }
            security_scheme(&mut root, "keylink", entry);
        }
        Security::ApiKey { header } => security_scheme(
            &mut root,
            "keylink",
            json!({ "type": "apiKey", "in": "header", "name": header }),
        ),
    }

    let value = Value::Object(root);
    check_references(&value, "#/components/schemas/", &["components", "schemas"])?;
    let bytes = render(&value);
    Ok(Document { value, bytes })
}

/// The standalone JSON Schema document, `$defs`-rooted rather than
/// `components`-rooted, for callers that want the contracts without the paths.
///
/// # Errors
///
/// [`DocumentError::ComponentCollision`] when two contracts claim one name.
pub fn schemas<D: Documented>(surface: &HttpSurface) -> Result<Document, DocumentError> {
    let mut defs: BTreeMap<String, Value> = BTreeMap::new();
    __register(&mut defs, "ErrorEnvelope", schema::error_envelope())?;
    let keyed = surface.addressing() == Addressing::Actors;
    for mut operation in D::operations() {
        if keyed && !operation.keyed {
            continue;
        }
        // The SAME route intersection the OpenAPI document uses. Without it the
        // two published documents disagree: a surface with no streaming would
        // describe `send` and `recv` contracts in `/schema.json` that
        // `/openapi.json` has no path for.
        if !surface
            .routes()
            .any(|route| serves(operation.kind, route.kind()))
        {
            continue;
        }
        // A standalone document resolves against `#/$defs`, which is where a
        // JSON Schema reader looks. Same hoist, different destination.
        hoist(&mut operation.request, &mut defs, "#/$defs")?;
        hoist(&mut operation.response, &mut defs, "#/$defs")?;
        __register(
            &mut defs,
            &format!("{}.{}.request", D::SERVICE, operation.method),
            operation.request.clone(),
        )?;
        __register(
            &mut defs,
            &format!("{}.{}.response", D::SERVICE, operation.method),
            operation.response.clone(),
        )?;
    }
    let value = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": format!("urn:keylink:{}", D::SERVICE),
        "$defs": to_value(defs),
    });
    // The same check the OpenAPI document runs, against the space THIS document
    // resolves in. Running it on only one of the two published documents would
    // make every dangling reference silent in the other.
    check_references(&value, "#/$defs/", &["$defs"])?;
    let bytes = serde_json::to_vec_pretty(&value).unwrap_or_default();
    Ok(Document { value, bytes })
}

/// Move a schemars schema's `$defs` into the shared table and rewrite its
/// references to point there.
///
/// `schemars` emits `#/$defs/Name` and keeps the definitions inside the schema
/// it produced. An OpenAPI document resolves against `#/components/schemas/`,
/// so an inlined `$defs` block is a reference to a location the document does
/// not have — which `check_references` catches, and which is how this function
/// came to exist rather than being designed in.
///
/// The component name carries the logical name AND a digest OF THAT NAME. The
/// name is for the person reading the document; the digest makes the rendered
/// component id stable and pins it to a recorded vector, so a rename is visible
/// in every published document rather than silent.
///
/// It does NOT disambiguate two different schemas that share a logical name —
/// it cannot, being a pure function of that name. A review reported this crate
/// claiming otherwise, and the claim was wrong. What separates them is
/// [`__register`], which refuses two shapes under one component instead of
/// keeping whichever the iteration reached second.
/// Serialize a document, or say so.
///
/// An empty `Vec` here would be served with `200` and the OpenAPI content type,
/// which is a document that says nothing while looking like one. `serde_json`
/// fails only on a non-string map key or a non-finite float, neither of which a
/// schema can contain — so this is unreachable, and it is loud rather than
/// silent because being wrong about that is worse than a panic in a build step.
fn render(value: &Value) -> Vec<u8> {
    match serde_json::to_vec_pretty(value) {
        Ok(bytes) => bytes,
        Err(error) => panic!("a document that cannot be serialized is not a document: {error}"),
    }
}

fn hoist(
    schema: &mut Value,
    components: &mut BTreeMap<String, Value>,
    into: &str,
) -> Result<(), DocumentError> {
    // `$schema` is legal inside an OpenAPI 3.1 Schema Object and says nothing a
    // reader of THIS document needs — the dialect is fixed by the OpenAPI
    // version. Stripped so it is not repeated on every contract.
    if let Some(map) = schema.as_object_mut() {
        map.remove("$schema");
    }
    let taken = schema
        .as_object_mut()
        .and_then(|map| map.remove("$defs"))
        .and_then(|defs| defs.as_object().cloned());
    let Some(defs) = taken else {
        rewrite_refs(schema, into, &BTreeMap::new(), None);
        return Ok(());
    };
    // The WHOLE map first, then one rewrite pass. Built the other way round —
    // register, then learn the name — a definition that references itself is
    // rewritten while its own name is still unknown, and `rewrite_refs` falls
    // back to the bare logical name. The result is a `$ref` to a component that
    // does not exist, which `check_references` then refuses. Mutual recursion
    // fails the same way in one direction.
    let names: BTreeMap<String, String> = defs
        .keys()
        .map(|logical| {
            (
                logical.clone(),
                format!("{logical}_{}", digest::component_name(logical)),
            )
        })
        .collect();
    for (logical, mut definition) in defs {
        rewrite_refs(&mut definition, into, &names, None);
        let component = names
            .get(&logical)
            .cloned()
            .unwrap_or_else(|| logical.clone());
        __register(components, &component, definition)?;
    }
    // The root's own component name, so a `"$ref": "#"` — how a schema says "me"
    // — lands on the component the root was hoisted into rather than on the
    // document root, which constrains nothing and would validate anything.
    let root = schema
        .get("title")
        .and_then(Value::as_str)
        .and_then(|title| names.get(title).cloned());
    rewrite_refs(schema, into, &names, root.as_deref());
    Ok(())
}

/// Rewrite every `#/$defs/Name` to `<into>/<component>`, and a bare `#` to the
/// root's own component when it has one.
fn rewrite_refs(
    value: &mut Value,
    into: &str,
    names: &BTreeMap<String, String>,
    root: Option<&str>,
) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(reference)) = map.get_mut("$ref") {
                if let Some(logical) = reference.strip_prefix("#/$defs/") {
                    let target = names
                        .get(logical)
                        .cloned()
                        .unwrap_or_else(|| logical.to_owned());
                    *reference = format!("{into}/{target}");
                } else if reference == "#" {
                    if let Some(root) = root {
                        *reference = format!("{into}/{root}");
                    }
                }
            }
            for child in map.values_mut() {
                rewrite_refs(child, into, names, root);
            }
        }
        Value::Array(items) => {
            for item in items {
                rewrite_refs(item, into, names, root);
            }
        }
        _ => {}
    }
}

/// Register one schema under one name, or refuse.
///
/// `#[doc(hidden)]` and public only so a test can drive it directly: the
/// collision it guards is reachable through `document()` only by constructing
/// two services that disagree, and a row built that way would measure the
/// fixture more than the guard.
#[doc(hidden)]
pub fn __register(
    into: &mut BTreeMap<String, Value>,
    name: &str,
    schema: Value,
) -> Result<(), DocumentError> {
    match into.get(name) {
        // The same schema under the same name is one contract described twice,
        // which is ordinary. A DIFFERENT schema is two contracts fighting over
        // one name, and keeping the last would publish whichever the iteration
        // order happened to reach second.
        Some(existing) if existing == &schema => Ok(()),
        Some(_) => Err(DocumentError::ComponentCollision {
            name: name.to_owned(),
        }),
        None => {
            into.insert(name.to_owned(), schema);
            Ok(())
        }
    }
}

/// Put one scheme where a generator looks for it, and require it.
fn security_scheme(root: &mut Map<String, Value>, name: &str, entry: Value) {
    if let Some(Value::Object(components)) = root.get_mut("components") {
        components.insert("securitySchemes".to_owned(), json!({ name: entry }));
    }
    root.insert("security".to_owned(), json!([{ name: [] }]));
}

fn to_value(map: BTreeMap<String, Value>) -> Value {
    Value::Object(map.into_iter().collect())
}

/// `/v1/rpc/{method}` with the method substituted, and every other placeholder
/// left as OpenAPI path syntax.
fn render_path(prefix: &str, template: &str, method: &str) -> String {
    format!("{prefix}{}", template.replace("{method}", method))
}

/// `Service.space.route.Method`, stable across runs because every part is
/// declared.
///
/// Named after the ROUTE family rather than the declaration, which matters for
/// exactly one case and cost one red to find: a `blob` declaration is served by
/// three routes, and naming all three after the declaration made them one id.
/// The document refused itself, which is what
/// [`DocumentError::OperationIdCollision`] is for.
fn operation_id(service: &str, operation: &Operation, route: &Route) -> String {
    let space = if route.addressing() == Addressing::Actors {
        "actor"
    } else {
        "singleton"
    };
    format!(
        "{service}.{space}.{}.{}",
        __kind_name(route.kind()),
        operation.method
    )
}

/// Which route families one declaration is served by.
///
/// A `blob` declaration is THREE routes: the lookup that mints a grant, and the
/// `GET` and `HEAD` that serve the bytes it addresses. Documenting only the
/// lookup would publish a resource whose `Location` header points at a path the
/// document does not describe.
const fn serves(declared: RouteKind, route: RouteKind) -> bool {
    match declared {
        RouteKind::BlobLookup => matches!(
            route,
            RouteKind::BlobLookup | RouteKind::BlobGet | RouteKind::BlobHead
        ),
        other => matches!(
            (other, route),
            (RouteKind::Rpc, RouteKind::Rpc)
                | (RouteKind::Send, RouteKind::Send)
                | (RouteKind::Recv, RouteKind::Recv)
        ),
    }
}

/// Which refusals one route family can actually answer with.
///
/// Every operation carried every category, which is a document that says a
/// `rpc` can answer `416 Range Not Satisfiable` and a blob byte route can
/// answer `415 Unsupported Media Type`. A generator writes an arm for each, and
/// a reader learns nothing about which are real.
///
/// The shared set is everything a request meets before its family matters: the
/// prelude, the principal, the policy and the upstream. The rest is per family
/// and follows the handler, not a guess — a category listed here that the
/// handler cannot raise is the same lie in the other direction.
fn errors_for(kind: RouteKind) -> Vec<ErrorKind> {
    let shared = [
        ErrorKind::InvalidRequest,
        ErrorKind::Unauthenticated,
        ErrorKind::Forbidden,
        ErrorKind::MethodNotFound,
        ErrorKind::MethodNotAllowed,
        ErrorKind::UpstreamError,
        ErrorKind::UpstreamUnavailable,
        ErrorKind::UpstreamTimeout,
    ];
    let extra: &[ErrorKind] = match kind {
        RouteKind::Rpc => &[
            ErrorKind::InvalidIdempotencyKey,
            ErrorKind::IdempotencyConflict,
            ErrorKind::IdempotencyCapacity,
            ErrorKind::RequestTooLarge,
            ErrorKind::UnsupportedMediaType,
            ErrorKind::ResponseTooLarge,
            ErrorKind::ActorNotFound,
        ],
        RouteKind::Send => &[
            ErrorKind::IdempotencyNotSupported,
            ErrorKind::InvalidStreamMetadata,
            ErrorKind::RangeNotSupported,
            ErrorKind::RequestStreamTooLarge,
            ErrorKind::UnsupportedMediaType,
            ErrorKind::ResponseTooLarge,
        ],
        RouteKind::Recv => &[
            ErrorKind::IdempotencyNotSupported,
            ErrorKind::RangeNotSupported,
            ErrorKind::RequestTooLarge,
            ErrorKind::UnsupportedMediaType,
            ErrorKind::ResponseStreamTooLarge,
            ErrorKind::ResponseTooLarge,
            ErrorKind::ActorNotFound,
        ],
        RouteKind::BlobLookup => &[
            ErrorKind::IdempotencyNotSupported,
            ErrorKind::RequestTooLarge,
            ErrorKind::UnsupportedMediaType,
            ErrorKind::ResponseTooLarge,
            ErrorKind::ActorNotFound,
        ],
        RouteKind::BlobGet | RouteKind::BlobHead => &[
            ErrorKind::IdempotencyNotSupported,
            ErrorKind::UnsupportedMediaType,
            ErrorKind::ResponseStreamTooLarge,
            ErrorKind::BlobNotFound,
            ErrorKind::BlobGone,
            ErrorKind::RangeNotSatisfiable,
        ],
        // A family this crate has not been taught. Everything, which is the
        // honest answer for a route nobody here has read.
        _ => ErrorKind::ALL,
    };
    let mut out = shared.to_vec();
    out.extend(extra.iter().copied());
    out.sort_unstable_by_key(|kind| kind.spec().status().as_u16());
    out.dedup_by_key(|kind| kind.spec().code());
    out
}

/// `#[doc(hidden)] pub` so `every_route_family_has_its_own_name` can walk the
/// inventory and assert this mapping is total and injective. The `_` arm below
/// is over a `#[non_exhaustive]` enum and cannot be removed.
#[doc(hidden)]
pub const fn __kind_name(kind: RouteKind) -> &'static str {
    match kind {
        RouteKind::Rpc => "rpc",
        RouteKind::Send => "send",
        RouteKind::Recv => "recv",
        RouteKind::BlobLookup => "blob",
        RouteKind::BlobGet => "blobGet",
        RouteKind::BlobHead => "blobHead",
        _ => "unknown",
    }
}

fn operation_object(service: &str, operation: &Operation, route: &Route) -> Value {
    let mut object = Map::new();
    object.insert(
        "operationId".to_owned(),
        json!(operation_id(service, operation, route)),
    );
    object.insert("tags".to_owned(), json!([service]));
    object.insert(
        "parameters".to_owned(),
        Value::Array(parameters(route.template())),
    );
    // A raw-octet route carries its arguments in a header, not in the body.
    // Documenting the body without the header would describe a request that is
    // half a request.
    if route.request_media() == Some("application/octet-stream") {
        if let Some(Value::Array(parameters)) = object.get_mut("parameters") {
            parameters.push(json!({
                "name": "keylink-arguments",
                "in": "header",
                "required": true,
                "description": "The declared arguments, as base64url with no \
                                padding, decoding to the schema this operation's \
                                x-keylink-arguments-schema describes.",
                "schema": { "type": "string", "contentEncoding": "base64url" },
            }));
        }
        object.insert(
            "x-keylink-arguments-schema".to_owned(),
            operation.request.clone(),
        );
    }
    if let Some(media) = route.request_media() {
        // A `send` takes RAW BYTES with the arguments in a header, so rendering
        // the argument object as the body would document a request the gateway
        // refuses with `InvalidStreamMetadata` — and every generated client
        // would send it.
        let body = if media == "application/octet-stream" {
            schema::octet_stream_schema()
        } else {
            operation.request.clone()
        };
        object.insert(
            "requestBody".to_owned(),
            json!({ "required": true, "content": { media: { "schema": body } } }),
        );
    }
    let mut responses = Map::new();
    let success = route.success_status().as_u16().to_string();
    let body = match route.response_media() {
        // A byte reply's DECLARED value is not in the body — it rides
        // `Keylink-Result` as base64url. Documenting only the bytes would leave
        // a `recv`'s return type undocumented anywhere in the document.
        Some("application/octet-stream") if route.kind() == RouteKind::Recv => json!({
            "description": "Raw bytes. The declared return value is in the \
                            Keylink-Result header.",
            "headers": {
                "Keylink-Result": {
                    "required": true,
                    "description": "The declared return value, as base64url \
                                    with no padding, decoding to JSON matching \
                                    x-keylink-result-schema.",
                    "schema": { "type": "string", "contentEncoding": "base64url" },
                },
            },
            "content": { "application/octet-stream": { "schema": schema::octet_stream_schema() } },
            "x-keylink-result-schema": operation.response.clone(),
        }),
        Some("application/octet-stream") => json!({
            "description": "Raw bytes.",
            "content": { "application/octet-stream": { "schema": schema::octet_stream_schema() } },
        }),
        Some(media) => json!({
            "description": "The declared return value.",
            "content": { media: { "schema": operation.response.clone() } },
        }),
        None => json!({ "description": "No body." }),
    };
    responses.insert(success, body);
    for kind in errors_for(route.kind()) {
        responses.insert(
            kind.spec().status().as_u16().to_string(),
            json!({
                "description": "A refusal, in the one shared envelope.",
                "content": {
                    "application/json": {
                        "schema": { "$ref": "#/components/schemas/ErrorEnvelope" }
                    }
                },
            }),
        );
    }
    object.insert("responses".to_owned(), Value::Object(responses));
    Value::Object(object)
}

/// Every `{placeholder}` a template still carries after the method is filled in.
///
/// The actor id is described as a DECODED logical string. A caller writes the
/// id it means and URI serialization percent-encodes it once; describing the
/// encoded form would invite encoding it twice. The byte cap is stated
/// separately from `maxLength`, because `maxLength` counts characters and the
/// runtime bound is UTF-8 bytes.
fn parameters(template: &str) -> Vec<Value> {
    let mut out = Vec::new();
    if template.contains("{actor}") {
        out.push(json!({
            "name": "actor",
            "in": "path",
            "required": true,
            "description": "The actor id, as a decoded logical string. URI \
                            serialization percent-encodes it exactly once. The \
                            runtime bound is on UTF-8 BYTES, not characters.",
            "x-keylink-max-utf8-bytes": keylink::MAX_KEY_LEN,
            "schema": { "type": "string", "minLength": 1 },
        }));
    }
    if template.contains("{id}") {
        out.push(json!({
            "name": "id",
            "in": "path",
            "required": true,
            "description": "A content address, lowercase hexadecimal.",
            "schema": { "type": "string", "pattern": "^[0-9a-f]{64}$" },
        }));
    }
    out
}

/// Every `$ref` in a document resolves to something that document carries.
///
/// Parameterized on the ref space, because the two published documents have
/// different ones: an OpenAPI reader resolves `#/components/schemas/`, a JSON
/// Schema reader resolves `#/$defs/`. One function, two call sites.
fn check_references(value: &Value, prefix: &str, container: &[&str]) -> Result<(), DocumentError> {
    let mut refs = Vec::new();
    collect_refs(value, &mut refs);
    for reference in refs {
        let Some(name) = reference.strip_prefix(prefix) else {
            return Err(DocumentError::UnresolvedReference { reference });
        };
        let mut here = value;
        for step in container {
            let Some(next) = here.get(step) else {
                return Err(DocumentError::UnresolvedReference { reference });
            };
            here = next;
        }
        if here.get(name).is_none() {
            return Err(DocumentError::UnresolvedReference { reference });
        }
    }
    Ok(())
}

fn collect_refs(value: &Value, into: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if key == "$ref" {
                    if let Some(text) = child.as_str() {
                        into.push(text.to_owned());
                    }
                } else {
                    collect_refs(child, into);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_refs(item, into);
            }
        }
        _ => {}
    }
}
