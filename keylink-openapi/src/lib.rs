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
    register(&mut components, "ErrorEnvelope", envelope)?;

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
            let item = operation_object(D::SERVICE, &operation, route, &config.security);
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
    root.insert(
        "tags".to_owned(),
        json!([{ "name": D::SERVICE, "description": "One keylink service." }]),
    );
    root.insert("paths".to_owned(), to_value(paths));
    root.insert(
        "components".to_owned(),
        json!({ "schemas": to_value(components) }),
    );
    if let Security::External { description } = &config.security {
        root.insert(
            "x-keylink-authentication".to_owned(),
            json!({ "kind": "external", "description": description }),
        );
    }

    let value = Value::Object(root);
    check_references(&value)?;
    let bytes = serde_json::to_vec_pretty(&value).unwrap_or_default();
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
    register(&mut defs, "ErrorEnvelope", schema::error_envelope())?;
    let keyed = surface.addressing() == Addressing::Actors;
    for mut operation in D::operations() {
        if keyed && !operation.keyed {
            continue;
        }
        // A standalone document resolves against `#/$defs`, which is where a
        // JSON Schema reader looks. Same hoist, different destination.
        hoist(&mut operation.request, &mut defs, "#/$defs")?;
        hoist(&mut operation.response, &mut defs, "#/$defs")?;
        register(
            &mut defs,
            &format!("{}.{}.request", D::SERVICE, operation.method),
            operation.request.clone(),
        )?;
        register(
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
/// The component name carries the logical name AND its digest. The name is for
/// the person reading the document; the digest is what keeps two services'
/// `Item` from being one component. Collisions are still refused rather than
/// merged, because two schemas under one digest is a fact worth failing on.
fn hoist(
    schema: &mut Value,
    components: &mut BTreeMap<String, Value>,
    into: &str,
) -> Result<(), DocumentError> {
    let taken = schema
        .as_object_mut()
        .and_then(|map| map.remove("$defs"))
        .and_then(|defs| defs.as_object().cloned());
    let Some(defs) = taken else {
        rewrite_refs(schema, into, &BTreeMap::new());
        return Ok(());
    };
    let mut names = BTreeMap::new();
    for (logical, mut definition) in defs {
        let component = format!("{logical}_{}", digest::component_name(&logical));
        // Depth first: a definition can reference another definition.
        rewrite_refs(&mut definition, into, &names);
        register(components, &component, definition)?;
        names.insert(logical, component);
    }
    // A second pass, so a definition registered early can reference one
    // registered late. Mutual recursion needs both directions.
    for (logical, component) in &names {
        let Some(mut definition) = components.get(component).cloned() else {
            continue;
        };
        rewrite_refs(&mut definition, into, &names);
        components.insert(component.clone(), definition);
        let _ = logical;
    }
    rewrite_refs(schema, into, &names);
    Ok(())
}

/// Rewrite every `#/$defs/Name` to `<into>/<component>`.
fn rewrite_refs(value: &mut Value, into: &str, names: &BTreeMap<String, String>) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(reference)) = map.get_mut("$ref") {
                if let Some(logical) = reference.strip_prefix("#/$defs/") {
                    let target = names
                        .get(logical)
                        .cloned()
                        .unwrap_or_else(|| logical.to_owned());
                    *reference = format!("{into}/{target}");
                }
            }
            for child in map.values_mut() {
                rewrite_refs(child, into, names);
            }
        }
        Value::Array(items) => {
            for item in items {
                rewrite_refs(item, into, names);
            }
        }
        _ => {}
    }
}

fn register(
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
        kind_name(route.kind()),
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

const fn kind_name(kind: RouteKind) -> &'static str {
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

fn operation_object(
    service: &str,
    operation: &Operation,
    route: &Route,
    security: &Security,
) -> Value {
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
    if let Some(media) = route.request_media() {
        object.insert(
            "requestBody".to_owned(),
            json!({
                "required": true,
                "content": { media: { "schema": operation.request.clone() } },
            }),
        );
    }
    let mut responses = Map::new();
    let success = route.success_status().as_u16().to_string();
    let body = match route.response_media() {
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
    for kind in ErrorKind::ALL {
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
    if let Security::Http { scheme, format } = security {
        object.insert(
            "x-keylink-http-auth".to_owned(),
            json!({ "scheme": scheme, "bearerFormat": format }),
        );
    }
    if let Security::ApiKey { header } = security {
        object.insert("x-keylink-api-key".to_owned(), json!({ "header": header }));
    }
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

/// Every `$ref` in the document resolves to something the document carries.
fn check_references(value: &Value) -> Result<(), DocumentError> {
    let mut refs = Vec::new();
    collect_refs(value, &mut refs);
    for reference in refs {
        let Some(name) = reference.strip_prefix("#/components/schemas/") else {
            return Err(DocumentError::UnresolvedReference { reference });
        };
        let present = value
            .get("components")
            .and_then(|components| components.get("schemas"))
            .and_then(|schemas| schemas.get(name))
            .is_some();
        if !present {
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
