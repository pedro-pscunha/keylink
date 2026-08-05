# RFC-02 — Runtime mount and unmount of protocols

**Status:** proposal. Nothing here is implemented.
**Target:** `keylink` 0.1.0, iroh 1.0.3, edition 2021, rustc 1.97.
**Author's recommendation in one line:** ship capabilities C and A for foreign protocols, build actors on a single ALPN with a registry, and do not vendor the 350-line router.

> **Revision 2, 2026-08-05.** Revision 1 recommended holding capability A. The
> `iroh-docs` case in [section 9](#9-the-four-protocol-case-iroh-docs) changed
> that. Two facts moved the verdict, and both are verified in section 9: a
> foreign protocol ignores keylink's `Allowlist`, and the cost of a protocol is
> paid when it is **constructed**, not when it is registered. Section 6 states
> the revised judgement and what it rests on.
>
> **Revision 3, 2026-08-05.** The owner confirmed the use case: an actor-like
> framework. Item 6 is answered YES.
> [Section 10](#10-actors-the-answer-changes-the-design) designs the mechanism,
> and the result is that **actor lifecycle must not touch the transport at all**.
> One ALPN carries the actor system, actor identity travels inside the request,
> and mounting is a map write. A per-actor ALPN costs O(N) per mount and O(N)
> per handshake, which §10.2 verifies against the `noq` and rustls sources.
> Sections 1 to 9 are unchanged, but they now describe a foreign-protocol
> feature. Actors need neither the fork nor capability A.
>
> **Revision 4, 2026-08-05.** Two decisions from the owner and the team lead
> reverse revision 2. Both are recorded rather than edited away, because the
> reversal is the useful part.
>
> 1. **A claim in §9.3 was wrong.** It said `serve_only` is "the only admission
>    control keylink has over a foreign protocol". It is not.
>    `ConnectMode::Intercept` gives per-peer admission, and §9.3 now carries the
>    correction and the comparison.
> 2. **`iroh-docs` is not a target.** It is replicated state and keylink is RPC.
>    Section 9 keeps the analysis, marked out of scope, because the batch-mount
>    argument rested on it.
>
> Net effect: **capability C is shipped. Capability A is designed and not
> built.** Section 9 explains what its justification was and why that
> justification is gone.

---

## 0. Summary

The request is "mount and unmount protocols at runtime". Research shows that this
request contains two capabilities with very different costs.

| | Capability | Cost | Status |
|---|---|---|---|
| **A** | Serve, or stop serving, an ALPN whose handler was registered before `spawn()` | ~45 lines, 0 new dependencies | **Designed, not built.** Kept for `rfc-actor`. See §9.7. |
| **B** | Mount an ALPN that no code registered before `spawn()` | ~350 vendored lines, 4 new direct dependencies | **Rejected.** §9.5 and §10.3 dissolve both arguments for it. |
| **C** | Put a foreign protocol (`iroh-blobs`, `iroh-gossip`) on a keylink node | 5 lines, 0 new dependencies | **Shipped.** It corrected a live defect. |

Capability C is separate from the runtime question. Section 5 shows that keylink
**could not** do what `Server::router` promised. That defect is now fixed.

Capability A is designed and deliberately unbuilt. Revision 2 recommended
shipping it. Revision 4 withdraws that, for two reasons recorded in §9.7: the
security argument was wrong, and the coupling argument rested on `iroh-docs`,
which is no longer a target. **Do not delete the design.** If `rfc-actor` lands
on one ALPN per actor, capability A becomes load-bearing.

**Actors are a fourth thing, and they need none of the above.** Section 10
designs them on one ALPN plus a registry. The scope of this table is therefore
foreign protocols, not the actor feature.

> **Version floor, and it is a live trap.** `iroh-blobs` before 0.103.0 and
> `iroh-gossip` before 0.101.0 pin `iroh` to an **exact** release candidate, so
> they cannot coexist with iroh 1.0.3. The failure is a type error at
> `.service(...)`, because a `ProtocolHandler` from a different iroh is a
> different trait. Require `iroh-blobs >= 0.103.0` and
> `iroh-gossip >= 0.101.0`. Details in §5.5.

---

## 1. Vendor, reimplement, or wrap?

### 1.1 What upstream actually is

`https://github.com/n0-computer/iroh-examples/tree/main/custom-router` holds
`custom-router/src/main.rs`. The file is 570 lines. Lines 141 to 570 are
`pub mod router`, which is 430 lines including documentation.

That module is **a fork of iroh's own router**. Compare it against
`iroh-1.0.3/src/protocol.rs`. The two files share `RouterBuilder`, `ProtocolMap`,
`handle_connection`, the `tokio::select!` accept loop, the cancellation token,
and the shutdown order. The fork adds one `mpsc` channel and two message
variants. Everything else is copied.

A fork of an upstream internal file is the worst kind of dependency. It tracks a
file that the upstream author changes without notice.

### 1.2 The fork is already behind iroh 1.0.3

This is verified, not predicted. iroh 1.0.3 contains two features that the
example does not:

1. **`incoming_filter`.** `iroh-1.0.3/src/protocol.rs:472` adds
   `RouterBuilder::incoming_filter(mut self, filter: IncomingFilter) -> Self`.
   It rejects a connection **before** the TLS handshake completes, and it
   returns `IncomingFilterOutcome::{Accept, Retry, Reject, Ignore}`
   (`protocol.rs:170`). The vendored `RouterBuilder` has no such field.
   Vendoring removes this feature from keylink permanently.

2. **`on_accepting`.** iroh's `handle_connection` calls
   `handler.on_accepting(accepting).await` (`protocol.rs:646`). That is a
   `ProtocolHandler` hook (`protocol.rs:248`) which lets a handler intercept the
   connection before it completes. The example's `handle_connection` calls
   `connecting.await` directly (`main.rs:559`) and **never calls
   `on_accepting`**. keylink does not implement `on_accepting` today. A foreign
   protocol can. Vendoring therefore silently breaks any foreign protocol that
   relies on the hook — which is exactly the audience of
   [section 5](#5-foreign-protocols).

The example pins `iroh = "1.0.0"`. iroh released 1.0.3. The drift is already
measurable at two features. It will grow.

### 1.3 The fork does not compile under keylink's lints

`custom-router/src/main.rs` calls `.expect("poisoned")` five times, at lines 256,
265, 269, 274 and 283. The keylink workspace sets:

```toml
[workspace.lints.clippy]
expect_used = "deny"
```

Verbatim vendoring therefore fails the build. Every copy must be edited, which
means keylink owns a patch set against a file it does not control.

### 1.4 The fork adds four direct dependencies

The vendored module imports `n0_error`, `n0_future`, `tokio_util` and `tracing`.
keylink declares none of these in `keylink/Cargo.toml`. All four are already in
`Cargo.lock` through iroh, so the build does not grow. The **direct** dependency
count grows from 9 to 13, and each new direct dependency is a semver surface
keylink must then track.

### 1.5 Licence obligations, stated precisely

These apply only if keylink vendors the file. They are stated here so that the
decision is informed.

- **Source licence.** `n0-computer/iroh-examples` is dual licensed. The
  repository README states: "This project is licensed under either of Apache
  License, Version 2.0 ... MIT license ... at your option."
- **Copyright holder.** `LICENSE-MIT` line 1 reads `Copyright 2026 N0, INC.`
- **Crate metadata.** `custom-router/Cargo.toml` declares **no** `license` field
  and no `publish` field. The crate is not on crates.io. The repository-level
  dual licence governs.
- **No NOTICE file.** The repository root holds `LICENSE-APACHE`, `LICENSE-MIT`,
  `README.md`, `Makefile.toml`, `.github` and `.gitignore`. There is no `NOTICE`
  file, so Apache-2.0 §4(d) propagation does not apply.

If keylink vendors under **MIT**, keylink must reproduce the copyright notice and
the full permission notice in all copies or substantial portions of the software.
In practice: add `keylink/LICENSE-MIT-iroh-examples`, and place a header in the
vendored file.

If keylink vendors under **Apache-2.0**, keylink must additionally satisfy §4(b):
carry prominent notices in each modified file stating that keylink changed it.
Section 1.3 proves keylink must modify the file, so §4(b) always triggers.

**MIT is the simpler arm.** Take MIT. The required header is:

```rust
//! Adapted from `custom-router/src/main.rs` in n0-computer/iroh-examples,
//! commit <SHA>, retrieved 2026-08-05.
//!
//! Copyright 2026 N0, INC. Licensed under the MIT licence; see
//! LICENSE-MIT-iroh-examples at the repository root.
//!
//! Modified by the keylink authors: `expect` calls replaced, `ProtocolMap`
//! narrowed, `incoming_filter` and `on_accepting` restored.
```

**One blocker before any vendoring.** `keylink/Cargo.toml` declares no `license`
field. keylink must declare its own licence, and that licence must be compatible
with MIT, before it vendors third-party code. Correct that first.

### 1.6 Recommendation

**Do not vendor. Do not reimplement. Do not wrap.**

The reason is not the 350 lines. The reason is that
[section 2](#2-api-shape) shows that keylink can obtain capability A with **zero**
vendored lines, because `Endpoint::set_alpns` is public. The fork buys only
capability B, and capability B has no named use case.

If capability B is ever required, prefer a fourth option that this RFC did not
originally consider: **ask n0 to upstream `Router::accept` and
`Router::stop_accepting` into `iroh::protocol::Router`.** The example proves n0
already wrote the code. An upstream API costs keylink nothing and rots for
nobody. File the issue before writing the fork.

---

## 2. API shape

### 2.1 The finding that removes the fork

`Endpoint::set_alpns` is **public**:

```rust
// iroh-1.0.3/src/endpoint.rs:961-968
/// Sets the list of accepted ALPN protocols.
///
/// This will only affect new incoming connections.
/// Note that this *overrides* the current list of ALPNs.
///
/// If the endpoint is closed, this method will log a warning and ignore
/// the request to set new ALPNs.
pub fn set_alpns(&self, alpns: Vec<Vec<u8>>) {
```

`Node::endpoint(&self) -> &Endpoint` already exposes it (`node.rs:89`).

`Router::spawn` calls `set_alpns` exactly once, at `protocol.rs:511`. The router
**never reads the ALPN list again**. After `spawn`, the router dispatches only on
the ALPN string that TLS negotiated, inside `handle_connection`
(`protocol.rs:641`).

Two consequences follow, and they are the whole design:

1. keylink can change the announced ALPN set at any time after `spawn`, without
   touching the router.
2. The handler stays in the router's frozen `ProtocolMap` the whole time. An
   ALPN that keylink stops announcing can never be negotiated, so its handler
   becomes unreachable. When keylink announces it again, the handler answers
   again.

That is mount and unmount for the set known at build time, with no fork.

### 2.2 The common case does not change

This is the current call site, and it stays exactly as short:

```rust
let server = Server::builder(&node)
    .service(inventory::service(handler, allow).build())
    .service(telemetry::service(tele, allow).build())
    .spawn()?;
```

`ServerBuilder`, `ServiceBuilder`, `Service` and `serve` are untouched. A caller
who never stops a service reads no new documentation and pays no new cost.

### 2.3 The exact new signatures

> **Designed, not built (revision 4).** Nothing in §2.3 to §2.7 is implemented.
> §9.7 records why the justification went away and the one condition that would
> bring it back. The design is kept intact on purpose.

```rust
impl Server {
    /// Stop announcing `alpn`. New dials on it are refused at the handshake.
    ///
    /// In-flight connections are **not** touched. See RFC-02 §3.
    ///
    /// # Errors
    ///
    /// [`Error::UnknownAlpn`] when `alpn` was never registered on the builder.
    /// [`Error::EndpointClosed`] when the endpoint is already closed.
    pub fn stop_serving(&self, alpn: &[u8]) -> Result<()>;

    /// Announce `alpn` again, so its registered handler answers new dials.
    ///
    /// # Errors
    ///
    /// The same two errors as [`Self::stop_serving`].
    pub fn resume_serving(&self, alpn: &[u8]) -> Result<()>;

    /// Announce exactly this set, in one re-announcement.
    ///
    /// Required when protocols depend on each other. `iroh-docs` needs
    /// `iroh-blobs` and `iroh-gossip`, so announcing the three one at a time
    /// exposes a partial state in which a docs sync cannot fetch content. See
    /// RFC-02 §4.6.
    ///
    /// # Errors
    ///
    /// [`Error::UnknownAlpn`] naming the first ALPN that was never registered.
    /// [`Error::EndpointClosed`] when the endpoint is already closed. On either
    /// error the announced set is left unchanged.
    pub fn serve_only(&self, alpns: &[&'static [u8]]) -> Result<()>;

    /// The ALPNs this server announces right now, in sorted order.
    pub fn serving(&self) -> Vec<&'static [u8]>;
}
```

Four methods. No new public type. No async on the control path, because
`set_alpns` is synchronous. The blocking API in `blocking.rs` therefore needs no
mirror at all: `blocking::Server` can expose the same four methods by direct
delegation, with no `Runtime::run` call.

`serve_only` validates every ALPN against the catalogue **before** it mutates
anything, so a typo in one entry cannot leave the set half-applied.

The three single-ALPN methods are not sugar over `serve_only`. A caller who
writes `serve_only(&[a, c])` to drop `b` must first read `serving()`, and that
read-modify-write reintroduces at the call site exactly the race §4.2 removes.
Keep both shapes.

### 2.4 One breaking change, and why it earns its place

```rust
- pub async fn shutdown(self) -> Result<()>;
+ pub async fn shutdown(&self) -> Result<()>;
```

An operator who stops a service from a signal handler or an admin RPC needs
shared access, which means `Arc<Server>`. `shutdown(self)` cannot be called
through an `Arc`. Taking `&self` makes `Arc<Server>` work for every method, and
it removes the need for a second handle type.

This matches upstream. `iroh::protocol::Router::shutdown` already takes `&self`
(`protocol.rs:429`) and returns `Ok` when already shut down. keylink can adopt
the same rule: a second `shutdown` returns `Ok(())`.

`#[must_use = "the server stops when dropped; hold it for as long as you serve"]`
stays on the struct, so the "hold it" guidance is unaffected.

`blocking::Server::shutdown` currently holds `inner: Option<crate::Server>` only
so it can move out of `self`. That `Option` can go away.

### 2.5 Internal state

```rust
pub struct Server {
    router: Router,
    endpoint: Endpoint,
    /// Every ALPN registered on the builder. Never changes after `spawn`.
    catalogue: BTreeSet<&'static [u8]>,
    /// The subset currently announced. Guarded because control is shared.
    live: Mutex<BTreeSet<&'static [u8]>>,
}
```

`&'static [u8]` costs no allocation, because `Service::alpn` is already
`&'static [u8]`. The `Vec<Vec<u8>>` that `set_alpns` demands is built per call.
Stopping a service is an administrative action, not a hot path.

`std::sync::Mutex` is correct here, not `tokio::sync::Mutex`. The guard is never
held across an `.await`, because `set_alpns` is synchronous (chapter 9).

Poison is recovered rather than propagated, because the guarded value is a plain
`BTreeSet` with no invariant that a panic can break part-way:

```rust
let mut live = self.live.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
```

`unwrap_or_else` is not `unwrap`, so `clippy::unwrap_used` stays satisfied.

### 2.6 The critical ordering rule

**`set_alpns` must be called while the lock is held.** Section 4 explains the
race that this prevents. The rule is stated here because it is the one line of
this design that a future maintainer can "clean up" and silently break.

```rust
fn stop_serving(&self, alpn: &[u8]) -> Result<()> {
    let Some(known) = self.catalogue.get(alpn).copied() else {
        return Err(Error::UnknownAlpn { alpn: String::from_utf8_lossy(alpn).into_owned() });
    };
    if self.endpoint.is_closed() {
        return Err(Error::EndpointClosed);
    }
    let mut live = self.live.lock().unwrap_or_else(|p| p.into_inner());
    if !live.remove(known) {
        return Err(Error::UnknownAlpn { alpn: String::from_utf8_lossy(alpn).into_owned() });
    }
    // Inside the lock on purpose. See RFC-02 §4.2.
    self.endpoint.set_alpns(live.iter().map(|a| a.to_vec()).collect());
    Ok(())
}
```

`Endpoint::is_closed` is public at `endpoint.rs:1711`. The check exists because
`set_alpns` on a closed endpoint logs a warning and does nothing. A silent
no-op on a control operation is not acceptable.

### 2.7 New errors

`ConfigError` documents itself as "Every variant is raised at startup. None of
them can be reached by a running service." Both new errors are reachable by a
running service. They must therefore **not** go in `ConfigError`. Add them to
`Error`:

```rust
/// A control operation named an ALPN that was never registered.
#[error("no service is registered on ALPN {alpn}")]
UnknownAlpn {
    /// The ALPN that was asked for.
    alpn: String,
},

/// A control operation ran after the endpoint closed.
#[error("the endpoint is closed - it can no longer change what it serves")]
EndpointClosed,
```

`Error` is `#[non_exhaustive]`, so adding variants is not a breaking change.

---

## 3. Semantics of unmount

### 3.1 In-flight calls

**Untouched.** `set_alpns` states: "This will only affect new incoming
connections" (`endpoint.rs:962`). keylink never removes the handler from the
router's `ProtocolMap`, so no handler is dropped and no task is cancelled.

`RpcService::accept` (`rpc.rs:172`) keeps looping on `connection.accept_bi()`.
Calls already spawned into its `JoinSet` run to completion. A call that arrives
on an **existing** connection after `stop_serving` is still served, because the
connection already negotiated its ALPN.

That last point deserves emphasis. `stop_serving` is not a drain and not a
barrier. It closes the door to new connections. It does not empty the room.

### 3.2 A connection mid-body-transfer

keylink bodies can be gigabytes (`body.rs`). A transfer in progress when
`stop_serving` runs **completes normally**. There is no timeout, no
cancellation, and no truncation. `Error::BodyTruncated` cannot be caused by
`stop_serving`.

This is strictly better than the upstream fork. Upstream removes the handler from
the map and calls `ProtocolHandler::shutdown()` under a 30 second timeout
(`main.rs:526`). keylink does not implement `shutdown`, so keylink would inherit
the default no-op (`protocol.rs:284`) and survive anyway. But the fork also
carries the accept-queue window described in §4.3, and this design does not.

### 3.3 A peer that dials a just-removed ALPN

The peer receives TLS alert 120, `no_application_protocol`. rustls raises it at
`rustls-0.23.43/src/server/hs.rs:116` and again at `:134` for QUIC.

On the dialling side, `classify_connect` (`rpc.rs:137`) matches the rendered
error for `"support any known protocol"` or `"error 120"`, and returns:

```rust
Error::SchemaMismatch { ours: String }
```

**This is a diagnostic collision, and it is the real cost of this feature.**
Invariant 1 says a drifted peer is refused at the handshake, and
`Error::SchemaMismatch` says "the peer was built against a different contract".
After `stop_serving`, a correctly built peer receives that exact message. The
message is then false.

The two causes cannot be told apart from the dialling side. TLS sends the same
alert for both, and the dialler is a different process with no way to know.

**The collision is narrower than it first appears.** It arises only when the
stopped ALPN belongs to a **keylink** service, because `classify_connect` runs
only on `RpcClient::connect`. A peer dialling a stopped `iroh-blobs` or
`iroh-docs` ALPN never reaches keylink's error path at all, and §9 shows that
foreign protocols are the main reason to want this feature.

Revision 1 of this RFC proposed rewording `Error::SchemaMismatch` to cover both
causes. **That is withdrawn.** It would make a precise message vaguer for every
user in order to describe a case most of them never reach. Instead:

- Leave the `#[error(...)]` message exactly as it is.
- Add the second cause to the variant's **doc comment**, where a reader who is
  actually debugging will find it.
- State in `stop_serving`'s documentation that stopping a keylink service makes
  its dialers report `Error::SchemaMismatch`.

Documentation absorbs the ambiguity. The error message stays sharp for the
common case, which is genuine schema drift.

### 3.4 `Server::shutdown` keeps working

`shutdown` delegates to `Router::shutdown` and is not affected by the announced
set. The router owns every handler for the whole lifetime, so
`protocols.shutdown()` (`protocol.rs:598`) still reaches every handler, including
the ones that are not announced.

Stopping every service and then calling `shutdown` is well defined. Section 4.4
covers the empty set.

---

## 4. Interaction with `Endpoint::set_alpns` overriding

### 4.1 The announce lags the decision

`set_alpns` affects new incoming connections. A handshake that is already in
flight completes against the previous server configuration. `stop_serving`
therefore returns before the last connection on that ALPN is refused.

The window is one handshake. There is no way to close it, because
`Endpoint` exposes no method to drain accepted-but-unhandled connections.
Document the window. Do not pretend it is absent.

### 4.2 Two control calls racing

This is the race that §2.6 prevents. Consider `set_alpns` called **outside** the
lock, with services `A` and `B` announced:

| Time | Task 1: `stop_serving(A)` | Task 2: `stop_serving(B)` |
|---|---|---|
| t0 | locks, removes `A`, computes `[B]`, unlocks | |
| t1 | | locks, removes `B`, computes `[]`, unlocks |
| t2 | | calls `set_alpns([])` |
| t3 | calls `set_alpns([B])` | |

The endpoint ends announcing `[B]`. `live` holds `{}`. The two disagree
permanently, and `B` is served by a server that believes it stopped serving `B`.

Holding the lock across `set_alpns` makes the compute-and-apply step atomic and
removes the race. `set_alpns` clones a rustls configuration and calls
`set_server_config`. It does not block on I/O and it does not await.

### 4.3 The window that this design avoids

The upstream fork has a failure mode that this design does not. On
`stop_accepting`, the fork removes the handler from the map
(`main.rs:448`) and then re-announces. A connection that finished TLS just before
the re-announce is already queued inside `endpoint.accept()`. When
`handle_connection` runs for it, the lookup fails:

```rust
// custom-router/src/main.rs:555
let Some(handler) = protocols.get(&alpn) else {
    warn!("Ignoring connection: unsupported ALPN protocol");
    return;
};
```

The function returns and drops the connection. **No close code is sent.** The
dialling peer sees neither a clean close nor alert 120. It sees an opaque
transport failure, which violates invariant 4 — clean close and truncation must
stay distinct.

This design never removes a handler, so the lookup never fails. This is a
concrete correctness reason to prefer the no-fork design over vendoring, beyond
the line count.

### 4.4 Stopping the last service

`set_alpns(vec![])` is **safe**, and this is verified rather than assumed.
rustls 0.23.43, `src/server/hs.rs:130-137`:

```rust
if cx.common.alpn_protocol.is_none()
    && (!our_protocols.is_empty() || hello.protocols.is_some())
{
    return Err(cx.common.send_fatal_alert(
        AlertDescription::NoApplicationProtocol,
        Error::NoApplicationProtocol,
    ));
}
```

Under QUIC, an empty server list still rejects any client that **offers** an
ALPN. `Endpoint::connect(peer, alpn)` always offers one, so every iroh dialler is
refused. The behaviour fails closed, and invariant 2 is preserved.

A client that offers no ALPN at all reaches the router. `accepting.alpn()` then
fails, `handle_connection` warns, and the connection is dropped
(`protocol.rs:633`). Still closed. iroh cannot produce such a client. A
hand-rolled QUIC client could, and §8 records that this path is unverified.

### 4.5 Only one owner per endpoint

`Node::endpoint` is public, and `set_alpns` overrides. Two `Server` values on one
`Node` will clobber each other's ALPN list. That hazard exists today and caused
the original defect that
`two_services_share_one_node_and_both_answer` guards. Document the rule: one
`Server` owns the ALPN list of one `Node`.

### 4.6 Batch re-announcement is required, not a convenience

> **Premise removed in revision 4.** This section is the argument that earned
> `serve_only` its design, and it rests entirely on `iroh-docs`, which is no
> longer a target. No remaining protocol has the functional coupling described
> below: `iroh-blobs` and `iroh-gossip` each stand alone. The reasoning holds;
> the case it applies to is gone. See §9.7.

Every mount and unmount calls `set_alpns`, and `set_alpns` overrides the whole
list. Announcing three protocols one at a time therefore walks the set through
two intermediate states:

```text
{keylink}                          -> announce blobs
{keylink, blobs}                   -> announce gossip
{keylink, blobs, gossip}           -> announce docs
{keylink, blobs, gossip, docs}
```

For **independent** protocols an intermediate state is harmless. A peer that
dials too early is refused and retries.

For `iroh-docs` it is not harmless, because the protocols are **functionally
dependent**. §9.1 shows that `Docs` is constructed from a blobs store and a
gossip handle, and a docs sync fetches entry content over the blobs protocol.
Announce docs before blobs, and a peer can open a docs sync that cannot fetch
what the sync tells it to fetch. The failure is not a clean refusal. It is a sync
that starts and then cannot complete.

The order above is the safe one: dependencies first, dependents last. The reverse
order is the unsafe one. **Do not rely on callers getting that right.** A single
`serve_only` call computes the new set once and re-announces once, so the set
never has a partial state at all.

`serve_only` also makes unmount safe in the same way. Removing blobs while docs
is still announced leaves docs advertised and broken. One call removes all three
together.

---

## 5. Foreign protocols

### 5.1 A documentation defect — found here, now fixed

> **Status: resolved.** `Service::foreign` is shipped, and its documentation
> states plainly that `Allowlist` does not cover a foreign protocol. The
> paragraphs below record what the defect was.

`rpc.rs:328` said:

```rust
/// The underlying iroh router, so a keylink service can share a node with a
/// foreign iroh protocol such as `iroh-blobs` or `iroh-gossip`.
pub fn router(&self) -> &Router {
```

**This does not work.** `Server::router` returns `&Router` *after* `spawn`.
`iroh::protocol::Router` has exactly four methods: `builder`, `endpoint`,
`is_shutdown` and `shutdown` (`protocol.rs:404-444`). None of them registers a
protocol. `RouterBuilder::accept` is the only registration point, and the builder
is consumed by `spawn`.

So keylink today **cannot** put `iroh-blobs` on a node that serves a keylink
service. The doc comment promises a capability that the type cannot deliver.

### 5.2 The fix is five lines

```rust
impl Service {
    /// Register a foreign iroh protocol, such as `iroh-blobs` or `iroh-gossip`,
    /// on the same node as your keylink services.
    ///
    /// # Authorization
    ///
    /// [`Allowlist`] does **not** apply. keylink never sees these connections.
    /// The foreign protocol is responsible for its own authorization.
    pub fn foreign(alpn: &'static [u8], handler: impl ProtocolHandler) -> Self {
        Self { alpn, handler: Box::new(handler) }
    }
}
```

`ProtocolHandler` is already imported in `rpc.rs:32`. `Service.handler` is
already `Box<dyn DynProtocolHandler>`, and iroh provides
`impl<T: ProtocolHandler> From<T> for Box<dyn DynProtocolHandler>`
(`protocol.rs:317`), so the boxing is sound.

The call site then reads:

```rust
let server = Server::builder(&node)
    .service(inventory::service(handler, allow).build())
    .service(Service::foreign(iroh_blobs::ALPN, BlobsProtocol::new(&store, None)))
    .spawn()?;
```

`ServerBuilder::spawn` already rejects a duplicate ALPN, so a foreign protocol
that collides with a keylink service is caught at startup.

[Section 9](#9-the-four-protocol-case-iroh-docs) works this through for the
hardest real case: `iroh-docs`, which drags in `iroh-blobs` and `iroh-gossip`,
for four protocols on one endpoint.

### 5.3 Why `&'static [u8]` is the right parameter type

`Service.alpn` is `&'static [u8]` today. Changing it to `Vec<u8>` would allocate
for every service and force `Service::alpn()` to return a borrow. It is not
needed: `iroh-blobs` and `iroh-gossip` both publish their ALPN as a `const`, so
`&'static [u8]` accepts them unchanged. Keep the field as it is.

### 5.4 Foreign protocols would compose with capability A for free

Because `stop_serving` works on the announced ALPN set and not on handler types,
it would stop a foreign protocol exactly as it stops a keylink service. No extra
code would be required.

That property is real, and it is not enough on its own to justify building
capability A. §9.7 records why.

Capability B is only required when the *code* for a protocol is not known at
build time. That means a plugin system. keylink has no plugin system.

### 5.5 Version floor for `iroh-blobs` and `iroh-gossip`

Both crates reached iroh 1.x only in their 2026-06-15 release. Read from the
crates.io sparse index:

| Crate | Version | Requires |
|---|---|---|
| `iroh-blobs` | 0.101.0 | `iroh == 1.0.0-rc.0` |
| `iroh-blobs` | 0.102.0 | `iroh == 1.0.0-rc.1` |
| **`iroh-blobs`** | **0.103.0** | **`iroh = ^1.0.0`** |
| `iroh-gossip` | 0.99.0 | `iroh == 1.0.0-rc.0` |
| `iroh-gossip` | 0.100.0 | `iroh == 1.0.0-rc.1` |
| **`iroh-gossip`** | **0.101.0** | **`iroh = ^1`** |

The release-candidate pins are **exact**, so an older version cannot share an
iroh with keylink at all. Cargo would need two incompatible copies, and a
`ProtocolHandler` from one is a different trait from the other. The symptom is a
type error at the `.service(Service::foreign(..))` call, which is baffling until
you look at the version.

Require `iroh-blobs >= 0.103.0` and `iroh-gossip >= 0.101.0`.

---

## 6. Do we actually need this?

### 6.1 For a keylink service, the value really is narrow

Two facts from the task framing hold up under examination.

**A restart already covers deployment.** The operator deploys by restarting a
Kubernetes pod. Adding, removing or changing a service is a deploy. A restart
handles it, and a restart is the better tool, because it also picks up the new
binary.

**A `Middleware` layer already covers maintenance, and it is better at it.** A
layer sees the typed request and returns `Outcome::Err(RemoteError::Unauthorized(..))`.
`a_layer_can_refuse_one_method_and_pass_the_rest` proves this works today.
Against `stop_serving`, the layer wins on every axis that matters:

| | `Middleware` refusal | `stop_serving` |
|---|---|---|
| Error the caller sees | `RemoteError::Unauthorized("...")`, typed, with a message | TLS alert 120, indistinguishable from schema drift |
| Granularity | Per method | Whole service |
| Connection | Survives; the caller can retry on it | Refused before it exists |
| Reversible | Yes, immediately | Yes |
| New public surface | None | 3 methods, 2 error variants, 1 breaking change |

`stop_serving` is worse for the maintenance use case, because §3.3 shows it
degrades the drift diagnostic. Do not reach for it to gate a keylink service
that a layer can gate instead. §6.2 explains why that verdict does not carry
over to a foreign protocol.

### 6.2 But the comparison above only covers keylink services

§6.1 compares `stop_serving` against a `Middleware` layer. That comparison is
sound, and for a **keylink** service the layer wins.

It does not apply to a foreign protocol. keylink cannot put a layer on
`iroh-blobs`. `Middleware<Req, Resp>` wraps a typed keylink dispatch, and a
foreign handler has no keylink types to wrap. §9.3 shows the consequence:
`Service::foreign` admits a protocol that keylink cannot authorize, cannot
refuse, and cannot observe.

For a foreign protocol the comparison is therefore not "layer versus
`stop_serving`". It is "**`stop_serving` versus nothing**".

### 6.3 The narrow cases, named precisely

**Case 1 — a service that must stop while another keeps streaming.**

> One process serves service **A** and service **B**. Service **B** must stop
> being reachable **immediately**, and service **A** is carrying a transfer that
> must not be interrupted.

A restart is not available, because it kills A's transfer. A layer on B is not
enough, because the connection is still established, B's allowlist check still
runs, and B's handler is still resident. The concrete form is a vulnerability
found in B's handler while A streams a multi-hour body. Real, and rare.

**Case 2 — admission control over a foreign protocol.** This case is new in
revision 2, and it is not rare. Any node that runs `Service::foreign` has it. See
§9.3.

Both cases need **capability A only**. Neither needs the fork.

### 6.4 Recommendation

**Capability C is shipped.** `Service::foreign` is 5 lines and it corrected a
documentation defect that promised a capability keylink did not have.

**Capability A stays unbuilt.** Revision 2 recommended shipping it alongside C.
Revision 4 withdraws that. The two arguments revision 2 relied on have both
gone:

- The security argument was **wrong**. `iroh-blobs` has its own per-peer
  intercept, so `serve_only` was never the only control over a foreign protocol.
  It is a kill switch, and the problem called for authentication (§9.3).
- The coupling argument is **out of scope**. It rested on `iroh-docs`, which is
  not a target. No remaining protocol is functionally coupled to another (§9.7).

What is left is case 1 in §6.3 alone: one service must stop while another keeps
streaming. That is real and rare, and it does not on its own pay for the
`Error::SchemaMismatch` collision in §3.3.

Keep the design. Do not build it yet. §9.7 names the one condition that revives
it.

**Do not build capability B.** Two arguments have been made for it, and both
dissolve on inspection:

- The `iroh-docs` lazy-mount argument. §9.5 shows a 15-line `OnceLock` pattern
  that delivers the same deferral on top of capability A.
- The actor argument, which looked decisive: spawning an actor at runtime does
  sound like mounting a handler at runtime. §10.3 shows it is not. An actor is
  addressed inside one protocol, not by a protocol of its own, so its lifecycle
  never reaches the transport.

That leaves capability B buying only the case where the handler's **type** is
unknown at compile time, which is a plugin system. keylink has no plugin system.
If one is ever needed, first ask n0 to upstream `Router::accept` and
`Router::stop_accepting`.

**Build actors on one ALPN and a registry** (§10.3). This needs no router change
at all. The actor system registers on today's `ServerBuilder` as one ordinary
`Service`.

---

## 7. Test plan

> **Status (revision 4).** §7.1 describes capability C, which is shipped and
> tested. §7.2 to §7.6 describe capability A, which is not built, so those tests
> do not exist. They are the plan to use if §9.7's condition ever revives it.

Tests go in `keylink/tests/protocol.rs`, beside the existing end-to-end tests,
and follow that file's naming style. Every test runs against real QUIC on the
loopback interface, per `testing.rs`.

Following `feedback_break_the_code_to_validate_a_proof`: each test below names
the exact edit that must make it fail. A test that cannot be made to go red
proves nothing.

### 7.1 Capability C — `Service::foreign`

| # | Test | Made red by |
|---|---|---|
| 1 | `a_foreign_protocol_and_a_keylink_service_share_one_node` | Removing `Service::foreign` |
| 2 | `a_foreign_protocol_on_a_duplicate_alpn_is_refused_at_startup` | Deleting the `seen.insert` check in `spawn` |
| 3 | `a_foreign_protocol_is_not_subject_to_the_keylink_allowlist` | Wrapping the foreign handler in an allowlist check |

Test 1 uses a minimal local `ProtocolHandler` rather than a real `iroh-blobs`
dependency. The property under test is co-residency, not blob transfer.

### 7.2 Capability A — reachability

| # | Test | Made red by |
|---|---|---|
| 4 | `stop_serving_refuses_a_new_dial_on_that_alpn` | Making `stop_serving` a no-op |
| 5 | `stop_serving_leaves_the_other_services_reachable` | Passing `vec![]` to `set_alpns` instead of the remaining set |
| 6 | `resume_serving_makes_a_stopped_alpn_reachable_again` | Removing the ALPN from `catalogue` on stop |
| 7 | `stop_serving_an_unregistered_alpn_is_an_error` | Returning `Ok(())` for an unknown ALPN |
| 8 | `stop_serving_twice_is_an_error_not_a_silent_success` | Ignoring the `live.remove` return value |
| 9 | `stop_serving_the_last_service_refuses_every_dial` | Nothing in keylink — this pins rustls §4.4 behaviour |

Test 9 is a guard against an upstream change. It is the only evidence that
`set_alpns(vec![])` fails closed rather than open.

### 7.3 Capability A — in-flight survival

| # | Test | Made red by |
|---|---|---|
| 10 | `a_call_in_flight_survives_stop_serving` | Calling `Router::shutdown` inside `stop_serving` |
| 11 | `a_multi_megabyte_body_completes_across_stop_serving` | The same edit |
| 12 | `an_existing_connection_still_answers_after_stop_serving` | The same edit |

Test 11 uses the existing `Pattern` reader and `Fnv1a` checksum from
`tests/protocol.rs`. It starts a `download`, reads one chunk, calls
`stop_serving`, then drains and compares the checksum. Four MiB is enough to
prove the property. A gigabyte only makes the suite slow.

Test 12 pins the §3.1 semantic that `stop_serving` is not a drain. It exists so
that the behaviour is a decision on record, not an accident.

### 7.4 Capability A — the ordering rule

| # | Test | Made red by |
|---|---|---|
| 13 | `concurrent_stop_and_resume_leave_the_announced_set_consistent` | Moving `set_alpns` outside the lock (§4.2) |

This test drives many `stop_serving` and `resume_serving` calls from several
tasks, joins them, and then asserts that `serving()` agrees with what the
endpoint actually answers. It is the only test that can catch the §4.2 race, and
it is inherently probabilistic. Run it with a loop count high enough to fail
reliably against the broken ordering, and record that count in a comment.

### 7.5 Capability A — diagnostics and lifecycle

| # | Test | Made red by |
|---|---|---|
| 14 | `a_dial_to_a_stopped_service_reports_a_schema_mismatch` | Nothing — this pins the §3.3 collision |
| 15 | `shutdown_still_completes_after_stop_serving` | Skipping non-announced handlers in shutdown |
| 16 | `stop_serving_after_shutdown_is_an_error_not_a_silent_no_op` | Removing the `is_closed` check |
| 17 | `shutdown_twice_is_ok` | Returning an error on the second call |

Test 14 does not assert desirable behaviour. It asserts the behaviour this RFC
accepts, so that a future reader finds the collision documented in a test rather
than discovering it during an incident.

Test 17 guards the §2.4 signature change.

### 7.6 The batch path and the four-protocol case

| # | Test | Made red by |
|---|---|---|
| 18 | `serve_only_replaces_the_whole_set_in_one_announcement` | Making `serve_only` loop over `resume_serving` |
| 19 | `serve_only_never_exposes_a_partial_set` | The same edit (§4.6) |
| 20 | `serve_only_rejects_an_unregistered_alpn_without_changing_the_set` | Validating during the mutation instead of before it |
| 21 | `three_dependent_protocols_become_reachable_together` | The same edit as 18 |
| 22 | `a_lazy_handler_refuses_a_connection_before_it_is_filled` | Returning `Ok(())` instead of closing in `Lazy::accept` |

Test 19 is the one that pins §4.6, and it is the awkward one. Announcing a set is
not observable from outside except by dialling, so the test cannot see an
intermediate state directly. Drive it instead: run `serve_only` in a loop on one
task while a second task dials the *dependency* ALPN continuously, and assert
that no dial is ever refused while the *dependent* ALPN answers. Against a
looping implementation this fails; against one `set_alpns` it cannot.

Tests 18 to 21 use local stub `ProtocolHandler` implementations, not real
`iroh-docs`. Adding three heavyweight dependencies to the test suite to prove a
property about ALPN announcement would be a poor trade. The functional
dependency between docs, blobs and gossip is real, but the property under test
is keylink's, and a stub exercises it exactly as well.

Test 22 covers the §9.5 pattern. If the `Lazy` proxy stays a documented
caller-side pattern rather than library code, this test belongs in a doc test on
`Service::foreign` instead of in `tests/protocol.rs`.

---

## 8. Risks, and what could not be verified

### 8.1 Verified by reading source

Every claim about iroh, rustls and the upstream example was checked against
source on this machine, at these paths:

- `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/iroh-1.0.3/src/protocol.rs`
- `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/iroh-1.0.3/src/endpoint.rs`
- `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/iroh-1.0.3/src/socket.rs`
- `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/rustls-0.23.43/src/server/hs.rs`
- `custom-router/src/main.rs` and `Cargo.toml`, fetched from GitHub on 2026-08-05

### 8.2 Not verified

1. **Nothing was compiled or run.** This is a design document. No signature here
   has been type-checked. The `stop_serving` body in §2.6 is illustrative.
2. **`context7` iroh documentation is stale.** It returned
   `Endpoint::builder()` with no preset, `on_connecting` instead of
   `on_accepting`, and `NodeAddr` instead of `EndpointAddr`. Those match iroh
   0.9x, not 1.0.3. Every iroh claim in this RFC comes from the vendored 1.0.3
   source instead. Treat `context7` as unreliable for iroh 1.x.
3. **A QUIC client that offers no ALPN.** §4.4 argues it is dropped by
   `handle_connection`. That path was traced by reading, not exercised. iroh
   cannot produce such a client.
4. **Dropping an `Accepting` mid-handshake.** §4.3 claims the peer sees no close
   code. This follows from quinn's behaviour on a dropped `Connecting`, and was
   not confirmed against quinn's source or a live capture. It affects only the
   fork, which this RFC recommends against.
5. **The upstream commit SHA.** The licence header in §1.5 has a `<SHA>`
   placeholder. Pin the real commit before vendoring anything.
6. **Whether n0 will upstream runtime mount.** §1.6 recommends filing an issue
   first. The iroh issue tracker was not searched for an existing request.

### 8.3 Risks if capability A ships

1. **The diagnostic collision is permanent** (§3.3). Once
   `Error::SchemaMismatch`'s message is reworded to cover both causes, it is
   weaker for everybody. That cost cannot be recovered later.
2. **`stop_serving` reads like a drain and is not one** (§3.1). A caller who
   assumes it stops in-flight work will be wrong. The name and the documentation
   must both be explicit.
3. **The §4.2 race is invisible in review.** Moving one line out of a lock looks
   like a cleanup and silently desynchronises the announced set. Test 13 is the
   only defence, and it is probabilistic.
4. **One `Server` per `Node` is an unenforced rule** (§4.5). Two servers on one
   node clobber each other. keylink cannot detect this, because `Endpoint`
   offers no getter for the current ALPN list — only the builder's
   `alpns()` setter at `endpoint.rs:535`.

### 8.4 Risk if nothing ships

`Server::router`'s documentation keeps promising foreign-protocol co-residency
that keylink cannot deliver (§5.1). If capability C is rejected as well, then
correct that doc comment instead. Leaving it is the worst option.

### 8.5 Added in revision 2

7. **No four-protocol stack was compiled.** §9.1 is assembled from the
   `iroh-docs` 0.101.0 README and the crates' real signatures. It was not built.
   keylink's `Cargo.toml` was deliberately not modified.
8. **Version resolution was checked by requirement, not by lockfile.** §9.4
   reads the dependency requirements from the crates.io sparse index. No actual
   `cargo update` was run to confirm the four crates co-resolve on iroh 1.0.3.
9. **The `Lazy` proxy in §9.5 is untested.** In particular, whether
   `ProtocolHandler` can be implemented for it without a lifetime problem was
   reasoned about, not compiled.

### 8.6 Added in revision 3

10. **§10.2 was not benchmarked.** The `Arc::make_mut` deep clone, the datapath
    lock, and the linear ALPN scan are all read from source and are certain. The
    per-second figures are arithmetic on operation counts, not measurements. If
    anyone wants to defend a per-actor ALPN, measure it before arguing.
11. **`rustls` was read, `noq` was read, but the interaction was not traced end
    to end.** Specifically, whether the endpoint's live server config holds an
    additional `Arc` reference is not confirmed. It does not change the
    conclusion, because `create_server_config` already clones the template
    (`socket.rs:253`) and that alone forces the deep-copy branch.
12. **§10.4 assumes an envelope shape that rfc-04 has not defined.** The opaque
    payload requirement is firm. The concrete field layout is illustrative.
13. **`docs/rfc-04-actors.md` did not exist when §10 was written.** §10.7 states
    the contract in both directions. If rfc-04 contradicts §10.3, that
    disagreement must be resolved before either RFC is implemented.

### 8.7 One claim in this RFC was wrong

Recorded rather than quietly edited, because the failure mode is worth naming.

§9.3 claimed `serve_only` was "the only admission control keylink has over a
foreign protocol". It was false, and the evidence against it was **already in
the same section**: §9.3 itself wrote that "`iroh-blobs` does provide the hook —
`ConnectMode::Intercept`", then concluded two paragraphs later that no control
existed. Reading the source was not the failure. Following the consequence was.

The check that would have caught it: after writing "X is the only way to do Y",
grep your own text for a mechanism you already described that also does Y.

Both remaining foreign-protocol claims were re-verified for revision 4 against
`iroh-blobs-0.103.0`: `net_protocol.rs:72` (`events.unwrap_or(EventSender::DEFAULT)`),
`events.rs:191` (the `DEFAULT` mask), `events.rs:205` (`ALL_READONLY`),
`events.rs:85` (`AbortReason::Permission`), `events.rs:419` (`client_connected`),
and `events.rs:588` (`endpoint_id: Option<EndpointId>`).

---

## 9. The four-protocol case: `iroh-docs`

> **Out of scope as of revision 4.** The owner ruled `iroh-docs` out: it is
> replicated state, keylink is RPC, and it will not be taken as a dependency.
>
> The section is kept, not deleted, for one reason. It carried the argument that
> flipped revision 2 to "ship capability A", and a reader must be able to follow
> how that argument arrived and how it left. §9.7 closes the loop.
>
> Two parts survive the scope change and are still live:
> **§9.2** (the `Service::foreign` call shape, now shipped) and **§9.5** (why
> lazy construction does not justify the fork). §5.5 holds the version floor,
> which still applies to `iroh-blobs` and `iroh-gossip`.

This section answered the hardest concrete case: `iroh-docs` beside a keylink
service. It was the strongest test of the API, and it changed two of this RFC's
conclusions — one of which has since been reversed.

### 9.1 The stack, verified

Every fact below comes from the published crates, downloaded and read on
2026-08-05.

| Crate | Version | Released | ALPN constant | Value | Handler type |
|---|---|---|---|---|---|
| `iroh-docs` | 0.101.0 | 2026-06-15 | `iroh_docs::ALPN` | `/iroh-sync/1` | `Docs` |
| `iroh-blobs` | 0.103.0 | 2026-06-15 | `iroh_blobs::ALPN` | `/iroh-bytes/4` | `BlobsProtocol` |
| `iroh-gossip` | 0.101.0 | 2026-06-15 | `iroh_gossip::ALPN` | `/iroh-gossip/1` | `Gossip` |

Sources: `iroh-docs-0.101.0/src/net.rs:18`, `iroh-blobs-0.103.0/src/protocol.rs:406`,
`iroh-gossip-0.101.0/src/net.rs:45`. All three are `pub const ALPN: &[u8]`, so
each is `&'static [u8]` at the use site.

All three implement `ProtocolHandler` — `iroh-docs/src/protocol.rs:68`,
`iroh-blobs/src/net_protocol.rs:86`, `iroh-gossip/src/net.rs:129` — and all three
handler types derive `Clone`. `Clone` matters: the caller must keep a handle to
use the protocol's API while also giving one to the router.

The dependency really is transitive. `Docs::Builder::spawn` has this signature
(`iroh-docs/src/protocol.rs:101`):

```rust
pub async fn spawn(
    self,
    endpoint: Endpoint,
    blobs: BlobsStore,
    gossip: Gossip,
) -> anyhow::Result<Docs>
```

`Docs` cannot be constructed without a blobs store and a live gossip handle. The
three ALPNs are not merely co-resident. They are functionally coupled, which is
what makes §4.6 a requirement rather than a nicety.

### 9.2 What the user writes: `iroh-blobs` and `iroh-gossip`

> **In scope, unlike the rest of section 9.** This example showed
> `iroh-docs` until revision 4. It now shows `iroh-blobs` and `iroh-gossip`,
> which are real targets. A worked example is the part a reader copies, and a
> copied example beats any banner above it.

`Service::foreign` is confirmed as necessary. The foreign handlers are types
keylink does not generate, so the builder needs a constructor from a raw pair.
The signature from §5.2 is unchanged and it is sufficient:

```rust
pub fn foreign(alpn: &'static [u8], handler: impl ProtocolHandler) -> Service
```

No generic parameter on `Service`, no lifetime, no `Box` at the call site.
`impl Into<Box<dyn DynProtocolHandler>>` would also work and would match
`RouterBuilder::accept` exactly, but `impl ProtocolHandler` is the narrower bound
and reads better. Prefer it.

Two foreign protocols beside one keylink service:

```rust
use iroh_blobs::{store::mem::MemStore, BlobsProtocol, ALPN as BLOBS_ALPN};
use iroh_gossip::{net::Gossip, ALPN as GOSSIP_ALPN};
use keylink::{Allowlist, Node, Server, Service};

let node = Node::bind().await?;
let endpoint = node.endpoint().clone();

// Each of these spawns its own actors on construction, not on registration.
// That is why deferring a protocol means deferring this, not the mount (§9.5).
let store = MemStore::default();
let gossip = Gossip::builder().spawn(endpoint.clone());

// WARNING: `BlobsProtocol::new(&store, None)` serves any blob to any peer that
// asks. `Allowlist` does not reach a foreign protocol (§9.3). Supply the
// per-peer intercept instead; rfc-blobs owns it.
let blobs_events = /* EventSender with ConnectMode::Intercept - see rfc-blobs */;

let server = Server::builder(&node)
    .service(inventory::service(handler, Allowlist::only([peer])).build())
    .service(Service::foreign(BLOBS_ALPN, BlobsProtocol::new(&store, Some(blobs_events))))
    .service(Service::foreign(GOSSIP_ALPN, gossip.clone()))
    .spawn()?;

// `store` and `gossip` are still owned here, so their APIs stay usable.
```

Two details that are easy to get wrong:

- **Clone before you register.** `BlobsProtocol` and `Gossip` both derive
  `Clone`. Hand the router one clone and keep one, or the API becomes
  unreachable the moment you register it.
- **Do not write `None`.** The `Some(blobs_events)` above is not decoration. §9.3
  shows exactly what `None` configures.

Compare against the equivalent on a bare iroh router:

```rust
let _router = Router::builder(endpoint.clone())
    .accept(BLOBS_ALPN, BlobsProtocol::new(&store, Some(blobs_events)))
    .accept(GOSSIP_ALPN, gossip)
    .spawn();
```

`.service(Service::foreign(A, H))` against `.accept(A, H)` is the whole
difference. **The design passes this test.** Adding a keylink service to a
standard foreign-protocol setup costs one line and no restructuring, and it
scaled to four protocols when four were in scope.

### 9.3 Foreign protocols ignore `Allowlist` — but the fix is theirs, not ours

This is the finding that changed the recommendation.

`RpcService::accept` (`rpc.rs:174`) authorizes every connection:

```rust
let peer: EndpointId = connection.remote_id();
if !self.allow.permits(&peer) {
    connection.close(1u32.into(), b"unauthorized");
    return Ok(());
}
```

A foreign handler has no such check, and keylink cannot add one. `Middleware`
wraps a typed keylink dispatch, so it cannot reach `BlobsProtocol::accept`.

What `BlobsProtocol::new(&blobs, None)` actually configures is verified at
`iroh-blobs-0.103.0/src/provider/events.rs:191`. Passing `None` selects
`EventSender::DEFAULT`, whose mask is:

```rust
pub const DEFAULT: Self = Self {
    connected: ConnectMode::None,      // no connection-authorization hook runs
    get:       RequestMode::None,      // no read-request hook runs
    push:      RequestMode::Disabled,  // writes refused
    ...
};
```

State it accurately. Writes are **disabled** by default, which is a genuine
mitigation. Reads are **not** gated: with `ConnectMode::None` no authorization
callback runs at all, so any peer that reaches the endpoint can fetch any blob
in the store whose hash it knows. A docs sync distributes exactly those hashes.

`iroh-blobs` does provide the hook — `ConnectMode::Intercept`, reachable through
`EventMask::ALL_READONLY` and a custom `EventSender`
(`events.rs:205`, `events.rs:419`). The canonical README setup does not use it.

So a keylink node built from the §9.2 snippet has a strictly allowlisted
`inventory` service and, on the same endpoint and the same public key, an
unauthenticated read-only blob server.

That much is confirmed, and it is worth knowing. The conclusion revision 2 drew
from it was wrong.

#### Correction (revision 4)

Revision 2 wrote: "Capability A is the only control keylink has over that."
**That is false.** `iroh-blobs` ships its own per-peer admission control, and
this RFC even cited the hook two paragraphs earlier without following the
consequence through.

`BlobsProtocol::new(&store, Some(events))` with `ConnectMode::Intercept` calls
`client_connected` (`events.rs:419`) before serving, and `AbortReason::Permission`
(`events.rs:85`) is the refusal. `EventMask::ALL_READONLY` (`events.rs:205`)
already sets `connected: ConnectMode::Intercept`.

The two mechanisms are not substitutes:

| | `serve_only` | blobs intercept |
|---|---|---|
| Granularity | The whole ALPN | Per peer |
| Live connections | Unaffected, they continue | Refused at connect |
| Who is denied | Everyone, or nobody | The peers you choose |

`serve_only` is a **kill switch**. The intercept is **admission control**. The
answer to "an unauthenticated protocol is mounted" is to authenticate it, not to
add a switch that turns it off for everybody at once.

Owning the blobs allowlist therefore belongs to `rfc-blobs`, not here. One trap
for whoever writes it, which this RFC did not catch:

```rust
// iroh-blobs-0.103.0/src/provider/events.rs:588
pub struct ClientConnected {
    pub connection_id: u64,
    pub endpoint_id: Option<EndpointId>,
}
```

`endpoint_id` is an `Option`, so the check must **fail closed on `None`**. A
`match` that admits the `None` arm is an open door, and it is the exact shape of
mistake `Allowlist` was built to make impossible (`authz.rs:16`, "there is
deliberately no `Default`").

This correction removes the security justification for capability A. §9.7 states
what is left.

### 9.4 A version trap worth documenting

> **Moved to §5.5**, because it applies to `iroh-blobs` and `iroh-gossip`, which
> are still targets, and burying a live trap inside an out-of-scope section
> would waste it. The `iroh-docs` rows below are kept only for completeness.

`iroh-docs` reached iroh 1.x only at 0.101.0. From the crates.io sparse index:

| `iroh-docs` | requires |
|---|---|
| 0.99.x | `iroh == 1.0.0-rc.0` |
| 0.100.0 | `iroh == 1.0.0-rc.1` |
| **0.101.0** | **`iroh = ^1`** |

The release-candidate pins are **exact**. A project on iroh 1.0.3 cannot use
`iroh-docs` 0.100.0 at all: the two would need two incompatible copies of iroh,
and a `ProtocolHandler` from one is a different trait from the other. The failure
is a type error at the `.service(...)` call, which is confusing unless you know
to look at the version.

Require `iroh-docs >= 0.101.0`, `iroh-blobs >= 0.103.0`, `iroh-gossip >= 0.101.0`.
All three were released on 2026-06-15 as one train. Document the floor.

### 9.5 The lazy-mount argument, and why it dissolves

The argument for capability B is: start with a keylink service only, and mount
docs, blobs and gossip when a user first opens a document. Paying for three
protocols at boot is waste when the feature may never be used.

The premise is sound. The conclusion does not follow, because of where the cost
actually lands.

**Registration is not what costs anything.** `RouterBuilder::accept` inserts into
a `BTreeMap` (`protocol.rs:489`), and `set_alpns` rebuilds a rustls
configuration. Both are trivial.

**Construction is what costs.** Verified:

- `Gossip::builder().spawn(endpoint)` (`iroh-gossip/src/net.rs:184`) spawns the
  gossip actor and registers a `GossipAddressLookup` on the endpoint. It does
  this at construction, and it returns `Gossip`, not `Result<Gossip>`.
- `Docs::memory().spawn(..)` builds a replica store and an author store, calls
  `blobs.downloader(&endpoint)`, and `Engine::spawn` starts a `gc_protect_task`
  (`iroh-docs/src/engine.rs`). In `fs-store` mode both stores open `redb` files
  on disk.

So capability B does **not** save the boot cost either, unless the caller also
defers construction. And once construction is deferred, the type is still known
at compile time — only the value is missing. That is a `OnceLock`, not a router
fork:

```rust
/// Registered at build time, constructed later. Refuses until it is filled.
#[derive(Debug)]
struct Lazy<H>(std::sync::OnceLock<H>);

impl<H: ProtocolHandler> ProtocolHandler for Lazy<H> {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let Some(inner) = self.0.get() else {
            // Unreachable if you fill before announcing. Fails closed anyway.
            connection.close(1u32.into(), b"not ready");
            return Ok(());
        };
        inner.accept(connection).await
    }
}
```

The boot cost becomes three empty `OnceLock`s. When the user opens a document,
construct the real handlers, fill the cells, then announce all three at once:

```rust
// At boot: registered, constructed empty, and not announced.
server.serve_only(&[inventory::alpn()])?;

// On first document open: fill the cells FIRST, then announce.
blobs_cell.set(BlobsProtocol::new(&store, None)).ok();
gossip_cell.set(gossip).ok();
docs_cell.set(docs).ok();
server.serve_only(&[inventory::alpn(), BLOBS_ALPN, GOSSIP_ALPN, DOCS_ALPN])?;
```

Fill before announcing. That ordering is the whole discipline, and it is the same
dependency-before-dependent rule as §4.6.

This is roughly 15 lines, it is generic over `H`, and it needs no keylink change
beyond capability A. **Do not ship it in keylink.** It is a caller-side pattern
with one correct usage; putting it in the library would add a public type to
serve a single shape. Document it in the `Service::foreign` rustdoc instead.

What remains for capability B after this is only the case where the handler's
**type** is unknown at compile time. That is a plugin system, and keylink has
none.

### 9.6 Verdict on the four-protocol case

1. The API passes. Four protocols cost one `.service(Service::foreign(..))` line
   each, and the keylink call site is unchanged (§9.2).
2. `Service::foreign` is confirmed necessary, with the §9.2 signature.
3. A batch re-announcement is **required**, not optional, because `Docs` is
   functionally dependent on blobs and gossip (§4.6). `serve_only` covers it.
4. Lazy mounting does **not** justify the fork. The cost is in construction, and
   a `OnceLock` defers construction on top of capability A (§9.5).
5. ~~The case strengthens capability A for a reason nobody had raised: a foreign
   protocol ignores `Allowlist`, so `serve_only` is keylink's only admission
   control over it.~~ **Withdrawn in revision 4.** The premise is true and the
   conclusion is false. See the correction in §9.3.

### 9.7 Why capability A is designed and not built

This is the record the team lead asked for. Capability A had two justifications.
Both are gone, and neither failed because the design was wrong.

**Justification 1 — admission control over a foreign protocol. Withdrawn.**
`iroh-blobs` has its own per-peer intercept, so `serve_only` was never the only
control. It is a kill switch where the problem needed authentication. §9.3
carries the correction and the comparison table.

**Justification 2 — atomic mounting of coupled protocols. Removed by scope.**
`serve_only` earned its validate-all-before-mutate design from one fact:
`Docs::Builder::spawn(endpoint, blobs, gossip)` proves docs cannot work without
blobs and gossip, so announcing them one at a time exposes a state where a docs
sync starts and cannot fetch (§4.6). With `iroh-docs` out of scope, **no
remaining target has that coupling.** `iroh-blobs` and `iroh-gossip` each stand
alone, and each is a single independent mount.

So the batch primitive is now a solution without a problem. Say it plainly: the
reasoning was sound and the case it solved no longer exists.

**What survives.** The design in §2.3 through §2.7, the race analysis in §4.2,
and the in-flight semantics in §3.1 through §3.3 are unaffected by either
reversal. They describe a mechanism that works. Nothing here says it is wrong,
only that nothing currently needs it.

**When it comes back.** One condition, named precisely: if `rfc-actor` lands on
**one ALPN per actor** rather than the §10.3 registry, then actor mount becomes
ALPN mount, capability A becomes load-bearing, and the batch commit becomes
mandatory rather than optional (§10.6). §10.2 argues hard against that outcome.
If it happens anyway, this design is ready and must not have been deleted.

---

## 10. Actors: the answer changes the design

Revision 3. The owner states the use case: "call a function on another machine
and get an answer", and "be able to be an actor-like framework". Item 6 is
answered YES, so this section stops arguing about justification and designs the
mechanism.

The conclusion is short: **an actor is not a protocol, and actor lifecycle must
not touch the transport.** One ALPN carries the whole actor system, and actor
identity travels inside the request. Mount and unmount become a map write.

That answer removes capability B from the actor path completely. It also removes
capability A from the actor path. Sections 1 through 9 still stand, but they now
describe a **foreign-protocol** feature, not the actor feature.

### 10.1 The two addressing options

**Option 1 — one ALPN per actor.** The ALPN carries the actor id, for example
`myapp/Session/9f2c…/<actor-id>`. Mounting an actor announces a new ALPN.

**Option 2 — one ALPN per actor system.** The ALPN is derived exactly as today,
from the schema of the actor envelope. The actor id is a field in the request.
Mounting an actor inserts into a registry behind that one protocol.

### 10.2 Option 1 does not survive the churn question

The team lead asked what 10 mounts per second costs. The answer is worse than a
rate. The cost is **O(N) per mount**, where N is the number of live actors, so
starting N actors costs O(N²). Three verified reasons.

**Every `set_alpns` deep-clones the whole TLS server configuration.**
`QuicServerConfig` holds `inner: Arc<rustls::ServerConfig>`
(`noq-proto-1.1.1/src/crypto/rustls.rs:454`). `set_alpn_protocols` mutates it
through `Arc::make_mut`:

```rust
// noq-proto-1.1.1/src/crypto/rustls.rs:488
pub fn set_alpn_protocols(&mut self, alpn_protocols: Vec<Vec<u8>>) {
    let config = Arc::make_mut(&mut self.inner);
    config.alpn_protocols = alpn_protocols;
}
```

The clone is **not avoidable**. `create_server_config` starts with
`self.server_config.clone()` (`iroh-1.0.3/src/socket.rs:253`), which bumps the
`Arc` to at least two. `Arc::make_mut` therefore always takes the deep-copy
branch. Each call copies a whole `rustls::ServerConfig`, including an O(N) deep
copy of the previous ALPN vector that is discarded on the very next line.

**Every `set_alpns` takes the endpoint-wide datapath lock.**

```rust
// noq-1.1.1/src/endpoint.rs:327
pub fn set_server_config(&self, server_config: Option<ServerConfig>) {
    self.inner.state.lock().unwrap().inner.set_server_config(...)
}
```

`self.inner.state` is the same mutex that packet handling and connection accept
take (`endpoint.rs:382`, `:413`, `:241`). Actor churn therefore contends with
the datapath.

**Every handshake scans the whole ALPN list.** rustls does a linear search
(`rustls-0.23.43/src/server/hs.rs:101-108`):

```rust
cx.common.alpn_protocol = our_protocols
    .iter()
    .find(|ours| their_protocols.iter().any(|theirs| theirs.as_ref() == ours.as_slice()))
```

With N actors, each incoming handshake is O(N).

So per mount, Option 1 costs one full TLS config clone, two O(N) vector
operations, and one acquisition of the datapath lock. At 10 mounts per second
with 1,000 live actors that is roughly 20,000 small allocations per second plus
10 datapath lock acquisitions. At 10,000 actors it is ten times that, and every
handshake also walks 10,000 entries.

**These are complexity classes and operation counts, not measurements. Nothing
here was benchmarked.** The shape is what decides it: a per-actor ALPN makes the
cost of one actor proportional to the number of other actors.

Option 1 has four further defects:

- **It needs capability B.** Actor ids are not known at build time, so this is
  the plugin case, and it requires the 350-line fork this RFC rejects.
- **It costs one connection per actor.** ALPN is negotiated once per connection.
  A client talking to five actors on one node opens five connections, five TLS
  handshakes, and five independent congestion-control states. keylink's whole
  design is one connection carrying many concurrent streams (`rpc.rs` module
  documentation). Per-actor ALPN discards that.
- **It caps the actor id length.** A protocol name in the TLS ALPN extension has
  a one-byte length prefix, so 255 bytes is a hard ceiling for the whole string,
  prefix and schema hash included.
- **It gives a probe oracle.** More precisely than the team lead put it: a
  server does **not** send its ALPN list. rustls returns a single
  `SingleProtocolName` (`rustls-0.23.43/src/msgs/handshake.rs:422`), so the set
  cannot be enumerated. But a peer can test any one id by dialling it and seeing
  whether it gets alert 120. That is an existence oracle, not a directory. It
  matters only if actor ids are guessable.

### 10.3 Option 2 is the recommendation

Mount and unmount become this:

```rust
registry.write().insert(id, actor);   // mount
registry.write().remove(&id);         // unmount
```

O(1). No TLS work, no `set_alpns`, no endpoint lock, no re-announcement, no
handshake cost that grows with the actor count. The 10-per-second question does
not arise, and **no debounce and no batch commit are needed**. That is the direct
answer to churn: the throughput problem is not solved, it is removed.

Routing is read-heavy — every message reads the registry, and only lifecycle
writes it. A `std::sync::RwLock<HashMap<ActorId, Arc<dyn Actor>>>` is the right
starting point. Reach for sharding only if a profile demands it.

One connection then serves every actor on a node, and keylink's existing
one-stream-per-call design already gives per-message concurrency with no
head-of-line blocking. The actor system needs no new transport concurrency
mechanism.

### 10.4 What Option 2 costs, and how to pay it

**Invariant 1 weakens, and it must be rebuilt one layer up.** Today the ALPN
carries a hash of the service schema, so a drifted peer is refused at the
handshake. Under Option 2 the ALPN covers the **actor envelope**, not each
actor's message types. A peer built against a drifted actor message type would
complete the handshake.

Restore the property inside the envelope:

1. The request envelope carries the actor id **and** a `u64` schema hash for
   that actor's message type, produced by the existing `derive_alpn` machinery
   in `node.rs:104`.
2. The envelope's message field is **opaque bytes**, not a typed field. keylink
   decodes the envelope, compares the hash, and only then decodes the payload as
   the actor's type.
3. A mismatch returns a typed refusal before the payload is ever interpreted.

Step 2 is a hard ordering requirement, not a stylistic choice. postcard is not
self-describing, so decoding the payload before checking the hash is exactly the
silent-misparse failure the ALPN scheme exists to prevent
(`node.rs:185`, `same_shape_variant_swap_decodes_as_the_wrong_variant`).

What this trade actually costs and buys:

| | Handshake ALPN (today) | In-envelope hash (actors) |
|---|---|---|
| Drift refused | Yes | Yes |
| Silent misparse possible | No | No, if the ordering above holds |
| When it fires | Before any bytes | After one header frame |
| Bytes exposed first | None | One frame, capped by `MAX_HEADER` |
| Error the caller sees | TLS alert 120 | A typed `RemoteError` |

The last row is an improvement, not a regression. It also dissolves the §3.3
diagnostic collision for actors: a stopped actor and a drifted actor produce
two different typed errors, where two stopped ALPNs produce the same TLS alert.

The exposure in row four is bounded. Invariant 3 already checks the declared
frame length against `MAX_HEADER` before allocating (`wire.rs:128`).

### 10.5 Unmount with in-flight work

Under Option 2 this is well defined, and it is no longer an edge case.

**Routing stops at once.** Removal from the registry is atomic. Any message that
arrives afterwards for that id gets a typed refusal on a live connection. The
connection is not closed and other actors on it are unaffected. This needs one
new `RemoteError` variant, which rfc-04 should name.

**The message already being processed runs to completion.** Dispatch clones an
`Arc` to the actor before it awaits, so removing the map entry drops a reference
and never cancels a running future. This is the same reason the upstream fork
had to change `Box<dyn DynProtocolHandler>` to `Arc` (§4.3), and keylink gets it
for free by holding `Arc<dyn Actor>`.

**A body transfer in progress survives.** An actor message can carry gigabytes.
The rule from §3.1 and §3.2 carries over unchanged: nothing is cancelled and
nothing is truncated.

**Stopping is therefore two operations, not one.** They must be separate,
because a caller that closed a document wants to know the state is flushed, and
a caller responding to a fault does not want to wait.

```rust
/// Stop routing to `id`, then wait for the message in flight to finish.
/// Wrap in `tokio::time::timeout` to bound the wait.
pub async fn stop(&self, id: &ActorId) -> Result<()>;

/// Stop routing to `id` and cancel the message in flight where it stands.
pub async fn abort(&self, id: &ActorId) -> Result<()>;
```

Two named methods, not one method with a boolean or an `Option<Duration>`. A
flag argument here would hide the difference between "flush my document" and
"kill this now" at the call site.

`stop` deliberately takes no deadline. `tokio::time::timeout` composes, and
adding a parameter would duplicate it.

### 10.6 What this means for sections 1 to 9

Nothing in sections 1 to 9 is withdrawn, but its scope narrows.

- **Capability B stays rejected**, and the actor case no longer argues for it.
  The strongest argument for the fork was runtime mounting, and §10.3 shows
  actors do not mount at the transport layer at all.
- **Capability C is shipped**, and capability A is **designed and unbuilt**
  (§9.7). Revision 3 said both were recommended for foreign protocols, citing
  §9.3. That citation was withdrawn in revision 4: `iroh-blobs` has its own
  per-peer intercept. Actors need neither in any case.
- **The actor system is one ordinary keylink `Service`.** It registers on the
  existing frozen-router `ServerBuilder` before `spawn`, exactly like
  `inventory` in §9.2. No router change is required to ship actors.

If Option 1 were chosen against this recommendation, then yes, a debounce and a
batch commit would be mandatory rather than optional, and `serve_only` (§2.3) is
already the batch primitive they would build on. Coalescing every mount inside a
short window into one `set_alpns` would reduce the per-mount TLS clone to a
per-window one. It would not fix the O(N) handshake scan, and it would add
latency between "the actor exists" and "the actor is reachable".

### 10.7 Interface with rfc-04

`docs/rfc-04-actors.md` did not exist when this section was written. The
boundary is: **rfc-04 owns actor semantics, this RFC owns whether mounting
touches the transport.** §10.3 answers that it must not.

**What this RFC needs from rfc-04:**

1. The `ActorId` type and its wire encoding. §10.2 shows the oracle question
   depends on whether ids are guessable, and that analysis cannot be finished
   without knowing.
2. Whether a message schema is per actor **type** or per actor **instance**.
   §10.4 assumes per type. Per instance would need a hash in every request
   rather than a lookup.
3. Whether an actor can be addressed **through** a node that does not host it.
   Forwarding would put a routing table in the request path and would reopen
   every question in §10.5.
4. The name of the `RemoteError` variant for an unknown or stopped actor.

**What rfc-04 needs from this RFC — one thing:**

> Actor lifecycle must never call `Endpoint::set_alpns`. Mounting is a registry
> write behind a single ALPN. Do not design an actor address that appears in the
> ALPN.

Plus one constraint that is easy to miss: the actor system's ALPN is derived
from the **envelope** schema. Any change to that envelope moves the ALPN for
every actor at once, and every peer must be rebuilt. rfc-04 should freeze the
envelope shape early, and should put per-actor variation inside the opaque
payload rather than in the envelope.

### 10.8 Tests this adds

| # | Test | Made red by |
|---|---|---|
| 23 | `mounting_an_actor_does_not_change_the_announced_alpn_set` | Routing actor mount through `set_alpns` |
| 24 | `a_message_to_an_unmounted_actor_is_a_typed_refusal` | Closing the connection instead of replying |
| 25 | `the_connection_survives_a_message_to_a_stopped_actor` | The same edit |
| 26 | `stop_waits_for_the_message_in_flight` | Removing the drain from `stop` |
| 27 | `abort_cancels_the_message_in_flight` | Making `abort` an alias of `stop` |
| 28 | `a_body_transfer_survives_the_actor_being_stopped` | The same edit as 26 |
| 29 | `an_actor_message_on_a_drifted_schema_is_refused_before_it_is_decoded` | Decoding the payload before comparing the hash (§10.4) |
| 30 | `two_actors_share_one_connection_and_do_not_block_each_other` | Serialising dispatch behind one lock |

Test 23 is the one that pins §10.3. It is cheap and it guards the whole
recommendation: if a future change makes actor mount touch the ALPN set, the
O(N²) behaviour of §10.2 returns silently.

Test 29 must be written so that it fails when the ordering is wrong, not merely
when the hash differs. Use two actor message types with **identical postcard
shapes and different meanings**, the same construction as
`same_shape_variant_swap_decodes_as_the_wrong_variant` in `node.rs:185`. A test
with differently shaped types would pass even against the broken ordering,
because the decode would fail on its own.
