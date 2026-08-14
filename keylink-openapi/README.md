# keylink-openapi

`keylink-openapi` builds an OpenAPI 3.1 document and a standalone JSON Schema
document from the same `keylink_http::HttpSurface` a gateway is mounted with. One
value describes the routes and serves them, so the description and the runtime
cannot drift apart.

Part of the [keylink workspace](../README.md). Opt-in and separate on purpose:
`schemars` and this crate's JSON model live here and nowhere else, so a
`service!` user who wants no documents pays for neither. `cargo tree -p keylink`
and `cargo tree -p keylink-http` carry no schema edge.

```toml
[dependencies]
keylink = { git = "https://github.com/pedro-pscunha/keylink" }
keylink-http = { git = "https://github.com/pedro-pscunha/keylink" }
keylink-openapi = { git = "https://github.com/pedro-pscunha/keylink" }
schemars = "1"
```

Not published to crates.io yet. Requires Rust 1.91 or later. `schemars 1`
emits draft 2020-12, which is the dialect OpenAPI 3.1 requires.

## What a caller does

1. Write a `keylink::service!` block.
2. Call `keylink_openapi::document_service!(module)` **once**, in the crate that
   holds that block. It implements `Documented` for the module's `External`.
3. Build the documents from the surface the gateway is mounted with.
4. Mount `router(..)` only if the deployment wants its contract published.

```rust,ignore
use keylink_openapi::{document, schemas, router, DocumentConfig, Security};

keylink_openapi::document_service!(inventory);

let surface = HttpSurface::new("/inventory", Addressing::Singleton)?;

let config = DocumentConfig::new(
    "Inventory",
    env!("CARGO_PKG_VERSION"),
    Security::External {
        description: "A deployment authenticates ahead of this gateway and \
                      inserts a verified Principal."
            .to_owned(),
    },
);

let openapi = document::<inventory::External>(&surface, &config)?;
let schema = schemas::<inventory::External>(&surface)?;

let docs = router::<AppState>(openapi.bytes().to_vec(), schema.bytes().to_vec());
```

Every argument and return type needs `schemars::JsonSchema`. A missing derive is
a compile error naming the type — the generator never falls back to an empty
`{}` schema that would describe nothing while looking complete.

## The public surface

| item | what it is |
| --- | --- |
| `document_service!(module)` | Invoke once, in the crate holding the `service!`. The generated callback is `pub(crate)`, so a downstream crate cannot document the same service a second way. |
| `trait Documented` | `const SERVICE` and `fn operations()`. Implemented by the macro and by nothing else. |
| `struct Operation` | One described route. Values come from `document_service!`; do not build one by hand. |
| `enum Security` | `External { description }`, `Http { scheme, format }`, `ApiKey { header }`. |
| `struct DocumentConfig` | Title, version and security. Security is a required argument — there is no default and no invented `Bearer` fallback. |
| `struct Document` | `value()` for the `serde_json::Value`, `bytes()` for the rendered JSON. |
| `enum DocumentError` | `PathCollision`, `OperationIdCollision`, `ComponentCollision`, `UnresolvedReference`. `#[non_exhaustive]`. |
| `fn document::<D>(&HttpSurface, &DocumentConfig)` | The OpenAPI 3.1 document. |
| `fn schemas::<D>(&HttpSurface)` | The same contracts rooted at `components`, for callers who want types without paths. |
| `fn router::<State>(openapi, schema)` | The two document endpoints, and nothing else. |
| `fn component_name(&str)`, `fn octet_stream_schema()`, `struct RawSchema` | Naming and schema helpers for hand-written cases. |

`Operation::kind` is typed by `RouteKind`, which `keylink-http` exports only
through a `#[doc(hidden)]` module. That type therefore has no rustdoc page, and
matching on `kind` is not a supported use of this API.

## The document endpoints

`router` installs `GET` and `HEAD` on `/openapi.json` and `/schema.json`.

| | |
| --- | --- |
| `/openapi.json` | `application/vnd.oai.openapi+json;version=3.1` |
| `/schema.json` | `application/schema+json` |

Both carry `Cache-Control: public, max-age=0, must-revalidate`. A `HEAD` carries
the same `Content-Length` its `GET` would, per RFC 9110 §9.3.2. The bytes are
captured when the router is built, so the two endpoints cannot drift from each
other or from the document a test snapshotted.

**Nothing mounts this router for you.** Whether a deployment publishes its
contract is a decision, and a crate that installed the route on its own would
make that decision by being linked.

## What it will not do

No Swagger UI, no YAML, no CORS, no runtime reflection, no generated SDKs, and no
automatic mounting.

## Run it

```sh
cargo run -p keylink-openapi --example gateway
mise run http-smoke-full
```

The example serves a singleton surface at `/inventory`, a keyed actor surface at
`/bays`, and both document endpoints, in one process. The keyed surface calls
`.without_fallback()` because axum permits one fallback per router.

`Document::bytes()` is byte-stable across runs and processes — every map is a
`BTreeMap` — which is what lets the committed snapshots under `tests/snapshots/`
be a real check rather than a rewritten one.

## License

MIT or Apache-2.0, at your option. See [LICENSE-MIT](../LICENSE-MIT) and
[LICENSE-APACHE](../LICENSE-APACHE) at the workspace root.
