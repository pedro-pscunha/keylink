# RFC-02 — Runtime mount and unmount of protocols

**Status:** proposal. Nothing here is implemented.
**Target:** `keylink` 0.1.0, iroh 1.0.3, edition 2021, rustc 1.97.
**Author's recommendation in one line:** ship 5 lines now, hold ~40 lines in reserve, and do not vendor the 350-line router.

---

## 0. Summary

The request is "mount and unmount protocols at runtime". Research shows that this
request contains two capabilities with very different costs.

| | Capability | Cost | Recommendation |
|---|---|---|---|
| **A** | Serve, or stop serving, an ALPN whose handler was registered before `spawn()` | ~40 lines, 0 new dependencies | Design ready. Ship when a caller asks. |
| **B** | Mount an ALPN that no code registered before `spawn()` | ~350 vendored lines, 4 new direct dependencies | Do not build. |
| **C** | Put a foreign protocol (`iroh-blobs`, `iroh-gossip`) on a keylink node | 5 lines, 0 new dependencies | **Ship now.** It corrects a documentation defect. |

Capability C is separate from the runtime question. Section 5 shows that keylink
today **cannot** do what `Server::router` promises. That is a live defect, and it
is cheap to correct.

The honest answer to "do we need this?" is in [section 6](#6-do-we-actually-need-this).
The value of capability A is narrow. The value of capability B is zero until
somebody names a use.

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

    /// The ALPNs this server announces right now, in sorted order.
    pub fn serving(&self) -> Vec<&'static [u8]>;
}
```

Three methods. No new public type. No async on the control path, because
`set_alpns` is synchronous. The blocking API in `blocking.rs` therefore needs no
mirror at all: `blocking::Server` can expose the same three methods by direct
delegation, with no `Runtime::run` call.

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
alert for both. Section 6 treats this as the deciding argument.

If capability A ships, the message must change to cover both causes:

```rust
#[error("the peer does not serve this contract (ours is {ours}) - it was built \
         against different types, or it is not serving this service now")]
```

That weakens a good error message for every user, including the users who never
call `stop_serving`. Count it as a cost, not a mitigation.

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

---

## 5. Foreign protocols

### 5.1 A documentation defect exists today

`rpc.rs:328` says:

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
    .service(Service::foreign(iroh_blobs::ALPN, blobs))
    .spawn()?;
```

`ServerBuilder::spawn` already rejects a duplicate ALPN, so a foreign protocol
that collides with a keylink service is caught at startup.

### 5.3 Why `&'static [u8]` is the right parameter type

`Service.alpn` is `&'static [u8]` today. Changing it to `Vec<u8>` would allocate
for every service and force `Service::alpn()` to return a borrow. It is not
needed: `iroh-blobs` and `iroh-gossip` both publish their ALPN as a `const`, so
`&'static [u8]` accepts them unchanged. Keep the field as it is.

### 5.4 Foreign protocols compose with capability A for free

Because `stop_serving` works on the announced ALPN set and not on handler types,
it stops a foreign protocol exactly as it stops a keylink service. No extra code
is required.

This changes the answer to the original request. "Mount `iroh-blobs` at runtime"
becomes "register `iroh-blobs` at build, and announce it at runtime". For an
operator that is the same outcome, and it costs 5 lines instead of 350.

Capability B is only required when the *code* for a protocol is not known at
build time. That means a plugin system. keylink has no plugin system.

---

## 6. Do we actually need this?

### 6.1 The honest answer: the value is narrow

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
destroys the diagnostic. Do not ship it for maintenance.

### 6.2 The narrow case, named precisely

There is one situation where a layer cannot substitute:

> A single process serves service **A** and service **B**. Service **B** must
> stop being reachable **immediately**, and service **A** is carrying a transfer
> that must not be interrupted.

A restart is not available, because it kills A's transfer. A `Middleware` layer
on B is not enough, because the connection is still established, B's allowlist
check still runs, and B's handler is still resident and reachable. Only removing
B from the ALPN set stops B's code from being entered at all.

The concrete form of this is a vulnerability discovered in B's handler or in a
type B deserializes, while A is streaming a multi-hour body. That is real. It is
also rare.

Note what this case requires: it needs **capability A only**. It does not need
the fork. Service B was registered at build time.

### 6.3 Recommendation

**Ship capability C now.** `Service::foreign` is 5 lines and corrects a
documentation defect that promises a capability keylink does not have. That is
worth doing regardless of everything else in this RFC.

**Hold capability A.** The design is complete and costs ~40 lines with no new
dependencies. Do not ship it until somebody presents the §6.2 scenario. The
diagnostic collision in §3.3 is a real, permanent cost paid by every user,
including the ones who never call it. Do not pay it speculatively.

**Do not build capability B.** No named use case, ~350 vendored lines, 4 new
direct dependencies, two lost iroh features, and a fork of an upstream internal
file. If it is ever needed, first ask n0 to upstream `Router::accept` and
`Router::stop_accepting`. They already wrote the code.

---

## 7. Test plan

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
