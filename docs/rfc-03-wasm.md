# RFC-03: keylink in the browser

**Status:** proposal
**Target:** `wasm32-unknown-unknown`
**Verified against:** iroh 1.0.3, n0-future 0.3.2, rustc 1.97.1, cargo 1.97.1

## Summary

keylink can run in a browser. The work is a dependency diet, a swap of four
tokio runtime items for their `n0_future` equivalents, and three cargo features
that fence off the parts a browser does not have.

The five invariants survive. None of them touches a thread, a file, or a clock.

This proposal is not a sketch. Every claim below was compiled. The method is in
[Appendix A](#appendix-a-how-this-was-verified).

## 1. What breaks, file by file

The list below is the work plan. The column "verdict" says what the fix is.

### 1.1 `keylink/Cargo.toml` and the workspace manifest

| Item | Why it breaks | Verdict |
|---|---|---|
| `tokio = { features = ["full"] }` | `full` implies `net`, which pulls `mio`. `mio` does not compile for `wasm32-unknown-unknown`. | Split by target. See §2. |
| `iroh = "1.0.3"` (default features) | Defaults are `metrics`, `fast-apple-datapath`, `portmapper`, `tls-ring`. `portmapper` is a UPnP client with no meaning in a tab. | `default-features = false, features = ["tls-ring"]` for wasm. Restore the other three for native. |
| No `n0-future` | The replacement runtime items live there. | Add it. It is already in `Cargo.lock` at 0.3.2, pulled in by iroh. |

The `mio` failure arrives before any keylink code. A plain
`cargo check -p keylink --target wasm32-unknown-unknown` on the tree as it
stands today produces 48 errors, all of them inside `mio`.

### 1.2 `keylink/src/blocking.rs` — the whole file

| Line | Item | Why it breaks |
|---|---|---|
| 61 | `tokio::runtime::Builder::new_multi_thread()` | The browser gives one thread, and it paints the page. |
| 65 | `.build()` | `rt-multi-thread` does not exist for this target. |
| 82 | `inner.block_on(fut)` | Blocking the only thread stops the event loop. Nothing can ever wake it. |
| 99 | `tokio::runtime::Handle::try_current()` | Needs `rt`. |
| 192 | `tokio::signal::ctrl_c()` | `signal` pulls `mio`. There is no `SIGINT` in a tab. |
| 93 | `inner.shutdown_background()` | Needs `rt`. |

**Verdict:** feature `blocking`, on by default, plus a `compile_error!`. See §6.

### 1.3 `keylink/src/body.rs`

| Line | Item | Why it breaks | Verdict |
|---|---|---|---|
| 13 | `use std::path::Path` | Compiles, but becomes an unused import once `from_file` is gated. | Gate the import. |
| 71-78 | `Body::from_file` | `tokio::fs` needs the `fs` feature, which needs a filesystem. | Feature `fs`. |
| 18, 106-107 | `tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, copy_buf}` | **Does not break.** These are pure future combinators. | Keep. Declare `tokio/io-util` directly. |
| 41 | `Source::Reader(Pin<Box<dyn AsyncRead + Send>>)` | **Does not break**, and the `Send` bound must stay. See §5. | Keep. |

The rest of the file is portable as written. `BodyReader`, the truncation
check, the overrun check, and the cap all compile for wasm unchanged.

### 1.4 `keylink/src/rpc.rs`

| Line | Item | Why it breaks | Verdict |
|---|---|---|---|
| 36 | `use tokio::task::JoinSet` | `JoinSet` needs `tokio/rt`. | `n0_future::task::JoinSet`. |
| 183 | `calls.try_join_next()` | **`n0_future`'s browser shim does not have this method.** | Rewrite. See §4. |
| 187 | `calls.spawn(...)` | Present on both. | Keep. |
| 196 | `calls.shutdown().await` | Present on both. | Keep. |
| 45 | `BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>` | **Does not break.** iroh requires `Send` here on every target. | Keep. |

`iroh::protocol::ProtocolHandler` is declared `Send + Sync + 'static`, and its
`accept` returns `impl Future<Output = ...> + Send`. Those bounds carry no
`cfg`. Read `iroh-1.0.3/src/protocol.rs:228` and `:276`. As a result keylink
must **not** relax its own `Send` bounds for wasm, and `n0_future::boxed::BoxFuture`
is the wrong type here: it drops `Send` in a browser.

### 1.5 `keylink/src/middleware.rs`

| Line | Item | Why it breaks | Verdict |
|---|---|---|---|
| 123 | `tokio::time::timeout` | `tokio/time` reads `std::time::Instant`, which panics on this target. | `n0_future::time::timeout`. |
| 15, 104 | `std::time::Duration` | **Does not break.** `Duration` is arithmetic. | Keep. |

The panic is the point. `tokio/time` may compile and will then fail at run
time, which is the worst of the two outcomes.

### 1.6 `keylink/src/node.rs`

| Line | Item | Why it breaks | Verdict |
|---|---|---|---|
| 50 | `std::env::var(var)` | Compiles. Always answers "unset". | Feature `env`, plus a `compile_error!`. See §10. |
| 3 | `use std::str::FromStr` | Unused once `bind_from_env` is gated. | Gate the import. |
| 9 | `use crate::ConfigError` | Same. | Gate the import. |
| 26 | `presets::N0` | **Does not break.** The preset drops `DnsAddressLookup` for browsers itself. | Keep. |
| 79 | `endpoint.online()` | **Does not break**, and becomes mandatory. See §10.3. | Keep. |
| 104 | `derive_alpn` / `hash_ty_path` | **Does not break.** `postcard-rpc` default features are `[]`. | Keep. |

### 1.7 `keylink/src/authz.rs`

| Line | Item | Why it breaks | Verdict |
|---|---|---|---|
| 73 | `std::env::var(var)` in `from_env` | Compiles. Always answers "unset", so the allowlist is always `MissingEnvVar`. | Feature `env`. |
| 127 | test `unset_env_fails_closed` | Calls a gated function. | Gate the test. |
| everything else | `BTreeSet<EndpointId>`, `parse`, `permits` | **Does not break.** | Keep. |

### 1.8 `keylink/src/macros.rs` — the trap

`service!` unconditionally generates a `pub mod blocking` that names
`$crate::blocking::Runtime`. A browser build of a crate that calls `service!`
therefore fails with `could not find blocking in $crate`, even though keylink
itself compiled.

Writing `#[cfg(feature = "blocking")]` inside the macro body **does not fix
this**. That `cfg` is expanded in the calling crate and reads the *caller's*
features. A caller with an unrelated feature named `blocking` would flip the
wrong switch.

**Verdict:** a helper macro in keylink, defined twice under keylink's own
`cfg`. See §6.2. This defect is invisible to `cargo check -p keylink`, and it
only appears when a downstream crate expands the macro.

### 1.9 `keylink/src/lib.rs`

| Line | Item | Verdict |
|---|---|---|
| 41 | `pub mod blocking;` | `#[cfg(feature = "blocking")]`. |
| — | (new) three `compile_error!` guards | Add. See §6.1. |

### 1.10 `keylink/src/wire.rs`, `reply.rs`, `error.rs`, `testing.rs`

`wire.rs`, `reply.rs` and `error.rs` are portable as written. Nothing in them
names a thread, a file, or a clock. The six iroh error types in `error.rs`
carry no `cfg` and exist on both targets.

`testing.rs` is `#[cfg(test)]` and native-only, so it does not block anything.
Note one thing about it for later: `presets::Minimal` compiles for wasm but
configures no relay, and a browser has no UDP. A `Minimal` endpoint in a tab
can reach nobody. Browser tests need `presets::N0` and a real relay, which
makes them integration tests rather than unit tests. See §8.3.

### 1.11 `demo/`

`demo` is native-only and stays native-only. It uses `std::fs::read_to_string`,
`std::env::args`, `tokio::fs`, `tokio::signal` and a hand-built runtime. No
change is required, and none is proposed. Its `tokio = { features = ["full"] }`
is harmless because `demo` never enters a wasm build graph.

One consequence: CI must scope wasm builds with `-p keylink`, never
`--workspace`.

### 1.12 `keylink/tests/protocol.rs`

Native-only. No change required. It exercises `Body::from_file`,
`keylink::blocking` and `tokio::time::sleep`, all of which stay behind default
features.

---

## 2. The split, and the exact `Cargo.toml`

Three axes decide where a thing goes.

1. **Portable.** No thread, no file, no clock, no environment. This is most of
   the crate.
2. **Cargo feature.** A capability a caller can decline: `blocking`, `fs`,
   `env`, `web`.
3. **Target `cfg`.** A dependency that cannot exist on a target at all.

The rule that makes the two last axes work together: **a cargo feature never
enables a dependency feature that fails to build on another target.** Features
are unified across a build graph, and a failure inside `mio` names `mio`, not
the mistake. Target scoping keeps the failure inside keylink, where the error
message can be useful.

### 2.1 `keylink/Cargo.toml`

```toml
[package]
name = "keylink"
version = "0.1.0"
edition = "2021"
description = "Typed peer-to-peer RPC over iroh, with a schema-derived ALPN and fail-closed authorization."

[features]
default = ["blocking", "fs", "env"]
# The synchronous API. Needs OS threads, so it cannot exist in a browser.
blocking = []
# `Body::from_file`. Needs a filesystem.
fs = []
# `Node::bind_from_env` and `Allowlist::from_env`. Needs a process environment.
env = []
# `Body::from_web_file` and `Body::from_web_stream`: the browser's answer to
# `fs`. Off by default, because it is meaningful on exactly one target.
web = ["dep:wasm-streams", "dep:web-sys", "dep:tokio-util"]

[dependencies]
iroh = { workspace = true }
n0-future = { workspace = true }
serde = { workspace = true }
postcard = { workspace = true }
postcard-schema = { workspace = true }
postcard-rpc = { workspace = true }
bytes = { workspace = true }
thiserror = { workspace = true }
pastey = { workspace = true }
# Only the io adapters, which are pure future combinators and compile
# everywhere. Everything with a syscall behind it is in the target block below.
tokio = { version = "1", default-features = false, features = ["io-util"] }

# Scoped by target rather than by cargo feature on purpose. A feature that
# enabled `tokio/fs` would fail inside mio, several crates away from the
# mistake. Scoped this way, `--features blocking` on wasm fails in keylink's own
# `compile_error!` instead, which says what to do about it.
[target.'cfg(not(all(target_family = "wasm", target_os = "unknown")))'.dependencies]
iroh = { version = "1.0.3", default-features = false, features = [
    "tls-ring",
    "metrics",
    "portmapper",
    "fast-apple-datapath",
] }
tokio = { version = "1", default-features = false, features = [
    "io-util",
    "sync",
    "rt",
    "rt-multi-thread",
    "macros",
    "fs",
    "time",
    "signal",
] }

# The browser's body sources. Scoped to the browser target so that `web` on a
# native build is inert rather than a resolution failure.
[target.'cfg(all(target_family = "wasm", target_os = "unknown"))'.dependencies]
wasm-streams = { version = "0.5", optional = true }
web-sys = { version = "0.3", optional = true, features = ["Blob", "File", "ReadableStream"] }
tokio-util = { version = "0.7", optional = true, default-features = false, features = ["compat"] }

[dev-dependencies]
tokio = { version = "1", features = ["full"] }

[lints]
workspace = true
```

### 2.2 Workspace `Cargo.toml`

```toml
[workspace.dependencies]
iroh = { version = "1.0.3", default-features = false, features = ["tls-ring"] }
n0-future = "0.3"
serde = { version = "1", features = ["derive"] }
postcard = { version = "1", features = ["use-std"] }
postcard-schema = { version = "0.2", features = ["derive", "use-std"] }
postcard-rpc = "0.12"
tokio = { version = "1", features = ["full"] }   # demo only
bytes = "1"
thiserror = "2"
pastey = "0.2"
```

The workspace entry for `iroh` carries the browser-safe feature set, which is
the smaller one. `keylink` adds `metrics`, `portmapper` and
`fast-apple-datapath` back for native in its own target block. Cargo unions the
two sets per target.

This matters, and it is easy to get wrong. Writing `default-features = false`
once, in the workspace, would silently remove NAT port mapping from every
native build and degrade connectivity on real networks. Verified with
`cargo tree -e features`:

```
native: iroh feature "tls-ring" "metrics" "portmapper" "fast-apple-datapath"
wasm:   iroh feature "tls-ring"
```

The workspace `tokio` entry stays as it is, for `demo`. `keylink` stops using
it and declares `tokio` directly.

### 2.3 What is NOT in the manifest, and why

The prior research proposed two things this design drops.

**`getrandom = { version = "0.3", features = ["wasm_js"] }` and the
`--cfg getrandom_backend="wasm_js"` rustflag: not needed.** That rule belongs to
getrandom 0.3. iroh 1.0.3 depends on getrandom **0.4** with the `wasm_js`
feature already, scoped to wasm. In 0.4 the cargo feature alone selects the
backend. Read `getrandom-0.4.3/src/backends.rs:170-184`: the branch keys on
`#[cfg(feature = "wasm_js")]`, and only the `else` arm raises `compile_error!`.

This was verified both ways. The build is green with the rustflag and green
without it, and no `.cargo/config.toml` is required.

**`crate-type = ["cdylib", "rlib"]`: belongs to the application, not to
keylink.** keylink is a library. The consuming browser crate declares the
`cdylib`, exactly as `browser-echo` does. Adding `cdylib` to keylink would make
cargo link keylink for wasm on every build, for nobody's benefit.

### 2.4 A correction to the prior research

The quoted `browser-echo` manifest is incomplete. The real one also carries
`n0-future = "0.3"`, `wasm-streams = "0.5"`, `async-channel`,
`wasm-bindgen-futures` and `tokio-stream`. The `n0-future` omission matters,
because `n0-future` is the load-bearing dependency of the whole approach.

The note about `io-util` is correct, and the mechanism is worth naming: iroh
depends on `tokio` with `["io-util", "macros", "sync", "rt"]` **unconditionally**,
for every target. `browser-echo` calls `tokio::io::copy` and compiles only
because cargo unifies iroh's `io-util` into it. This proposal does not rely on
that. It declares `tokio/io-util` directly.

---

## 3. The `n0_future` migration

`n0-future` **0.3.2**, already in `Cargo.lock`. Its `build.rs` defines the same
`wasm_browser` alias iroh uses, so each item below is `tokio` on native and a
`wasm_bindgen_futures` shim in a browser.

| keylink uses today | Replacement | Available in 0.3.2? |
|---|---|---|
| `tokio::task::JoinSet` | `n0_future::task::JoinSet` | Yes. `task.rs:7` native, `task.rs:44` shim. |
| `JoinSet::new` | same | Yes. `task.rs:64`. |
| `JoinSet::spawn` | same | Yes. `task.rs:72`. |
| `JoinSet::shutdown` | same | Yes. `task.rs:234`. |
| **`JoinSet::try_join_next`** | **absent from the shim** | **No.** See §4. |
| `tokio::time::timeout` | `n0_future::time::timeout` | Yes. `time.rs:7` native, `time.rs:201` shim. |
| `tokio::spawn` | `n0_future::task::spawn` | Yes. `task.rs:5` and `task.rs:606`. Not used by keylink today. |

Two further notes on the shim, both taken from its source rather than from its
README.

`JoinSet::poll_join_next` exists on **both** sides, with the same signature
(`task.rs:163` in the shim, `tokio-1.53.1/src/task/join_set.rs:500` native).
That is the seam §4 uses.

tokio's `poll_join_next` requires `T: 'static`; the shim's does not. Any
portable helper over it must therefore write the `'static` bound explicitly, or
it compiles for wasm and fails for native. This proposal hit exactly that.

**Do not use `n0_future::boxed::BoxFuture` or `BoxStream` in keylink.** They
drop `Send` in a browser by design (`lib.rs:64-72`), and iroh's
`ProtocolHandler` requires `Send` there. keylink's own `BoxFut` stays as it is.

---

## 4. Concurrency: one stream per call, without `try_join_next`

This is the hardest item, and the answer is not a swapped import.

### 4.1 The problem

`rpc.rs:180-197` runs an accept loop:

```rust
let mut calls = JoinSet::new();
loop {
    // Reap finished calls so the set tracks in-flight work, not history.
    while calls.try_join_next().is_some() {}

    match connection.accept_bi().await {
        Ok((send, recv)) => { calls.spawn(serve_one(...)); }
        Err(error) if is_clean_close(&error) => break,
        Err(error) => return Err(AcceptError::from_err(error)),
    }
}
calls.shutdown().await;
```

`try_join_next` is the only method here the browser shim lacks. It is also the
only one that must not suspend: the loop reaps what has finished and then waits
for the *next stream*, not for a call to end.

### 4.2 The answer

Both `JoinSet` types expose `poll_join_next`. One poll with a no-op waker is
`try_join_next`, spelled portably.

```rust
/// Drop the calls that have already finished, without waiting for any that have
/// not.
///
/// tokio's `JoinSet` has `try_join_next` for this and the browser shim does
/// not, but both expose `poll_join_next`. One poll with a no-op waker is the
/// same operation spelled portably: it never suspends, so the waker it
/// registers is never needed. Nothing awaits this set between accepts.
fn reap_finished<T: 'static>(calls: &mut JoinSet<T>) {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    while let std::task::Poll::Ready(Some(_)) = calls.poll_join_next(&mut cx) {}
}
```

The call site becomes one line:

```rust
reap_finished(&mut calls);
```

`Waker::noop()` is stable since Rust 1.85. iroh 1.0.3 already requires 1.91.

**Why discarding the waker is safe.** `poll_join_next` stores the waker so it
can notify a task that is *waiting* on the set. Nothing waits on this set
between accepts, so the stored waker is never consulted. The one place that
does wait is `calls.shutdown().await` at the end of the loop, and `shutdown`
calls `join_next`, which registers a real waker of its own. No wake-up is lost.

**Why the semantics are identical.** `try_join_next` is documented as "polls
once, returns `Some` if a task has completed". `poll_join_next` with a no-op
waker is that definition without the wrapper.

**The `'static` bound is load-bearing.** Without it the helper compiles for
wasm and fails for native with `E0310`. This was observed, not predicted.

### 4.3 An alternative, considered and rejected

The loop could race `accept_bi()` against `join_next()` with
`n0_future::future::or`, which would reap continuously instead of at each
accept. `AcceptBi` is cancel-safe, so dropping the losing branch is sound —
verified by reading `noq-1.1.1/src/connection.rs:1075-1098`, where the stream is
taken from the connection and returned inside a single `poll`.

It is rejected because it buys nothing. Under the current design each accept
reaps every finished call, so the set holds at most the in-flight calls plus
those that finished since the last accept. QUIC's `max_concurrent_bidi_streams`
bounds both at 100. There is no growth to fix, and a race is a worse thing to
own than a helper with a comment.

### 4.4 What concurrency means in a tab

The property in `rpc.rs`'s module doc survives: each call takes its own QUIC
stream, `call` takes `&self`, there is no client mutex, and a slow handler
blocks nobody. The integration test
`concurrent_calls_do_not_queue_behind_each_other` passes unchanged after the
migration.

One thing changes, and the documentation should say it. In a browser the
`JoinSet` shim spawns through `wasm_bindgen_futures::spawn_local`. Calls
interleave on the single JS event loop at each `await`. They do not run in
parallel. A handler that computes for 200 ms without awaiting freezes the page
for 200 ms. On native that same handler occupies one worker thread out of many.

That is a property of the platform, not of this design. It deserves a sentence
in `rpc.rs`'s module doc, next to the paragraph on stream costs.

---

## 5. Bodies without a filesystem

### 5.1 The portable core is already right

`Body` has two sources: `Bytes` and `Pin<Box<dyn AsyncRead + Send>>`. Neither
names a file. `from_file` is a *constructor*, not the abstraction, so gating it
removes nothing structural.

`BodyReader` is portable without a single change. Invariant 4 lives entirely in
`next_chunk`: it compares `received` against `declared_len` and raises
`BodyTruncated` or `BodyOverrun`. That comparison is source-independent, so it
holds for a browser body for exactly the reason it holds for a file.

The three portable constructors — `empty`, `from_bytes`, `from_reader` — are
what a browser needs. `from_bytes` already covers a payload the page holds in
memory.

### 5.2 What the browser gets instead of `from_file`

A JS `ReadableStream`, behind an optional `web` feature that mirrors `fs`:

```rust
/// Send a browser [`File`](web_sys::File) — what a browser has where a
/// native program has a path.
///
/// The length is read from the file's own metadata, so the receiver knows
/// the size before the first byte arrives and can refuse it. That is the
/// same contract as [`Self::from_file`], which is what keeps a short
/// transfer distinguishable from a complete one on either platform.
#[cfg(all(feature = "web", target_family = "wasm", target_os = "unknown"))]
#[must_use]
pub fn from_web_file(file: &web_sys::File) -> Self {
    // `Blob::size` is `f64` because JS has no integer type wide enough.
    // Every file size below 2^53 bytes (8 PiB) is exact in an `f64`, so the
    // cast loses nothing a browser can actually hand us.
    let len = file.size() as u64;
    Self::from_web_stream(file.stream(), Some(len))
}

/// Send whatever a JS `ReadableStream` produces.
///
/// The browser counterpart of [`Self::from_reader`]. Pass `len` when it is
/// known; `None` is an open-ended stream, ended by finishing it.
#[cfg(all(feature = "web", target_family = "wasm", target_os = "unknown"))]
#[must_use]
pub fn from_web_stream(stream: web_sys::ReadableStream, len: Option<u64>) -> Self {
    use tokio_util::compat::FuturesAsyncReadCompatExt;

    let reader = wasm_streams::ReadableStream::from_raw(stream)
        .into_async_read()
        .compat();
    Self {
        declared_len: len,
        source: Source::Reader(Box::pin(OnOneThread::new(reader))),
    }
}
```

`web_sys::Blob::size()` returns `f64` (`web-sys-0.3.103/src/features/gen_Blob.rs:25`)
and `Blob::stream()` returns a `ReadableStream` (`:254`). `File` derefs to
`Blob`. `wasm_streams::ReadableStream::into_async_read` is
`wasm-streams-0.5.0/src/readable/mod.rs:356`.

**Invariant 4 is preserved for the same reason it holds natively.** A browser
`File` knows its size, so `declared_len` is `Some`. A stream that ends early
raises `BodyTruncated` at the receiver. `from_web_stream` with `len = None` is
the open-ended case, ended by finishing the QUIC stream — the same contract as
`from_reader(r, None)`.

### 5.3 The `Send` problem, and why it is not the one you expect

`wasm_bindgen::JsValue` **is** `Send` on this target. wasm-bindgen writes
`unsafe impl Send for JsValue` under `#[cfg(not(target_feature = "atomics"))]`
(`wasm-bindgen-0.2.126/src/lib.rs:173-176`). So `web_sys::File` is not the
obstacle.

`js_sys::JsFuture` is. It holds an `Rc<RefCell<Inner>>`, so it is `!Send`, and
`wasm_streams::IntoAsyncRead` holds one. Meanwhile iroh requires the handler
future to be `Send` **in a browser too**. The compiler states it plainly:

```
the trait `Send` is not implemented for `Rc<RefCell<js_sys::futures::Inner>>`
note: required because it appears within the type `js_sys::JsFuture`
note: required because it appears within the type `wasm_streams::readable::IntoAsyncRead<'_>`
= note: required for the cast from `Pin<Box<Compat<IntoAsyncRead<'_>>>>`
        to `Pin<Box<dyn AsyncRead + Send + 'static>>`
```

Relaxing `Source::Reader` to drop `Send` on wasm does not work. `Body` crosses
`Next` and `Dispatch`, both of which end inside
`ProtocolHandler::accept() -> impl Future + Send`. A `!Send` `Body` breaks the
whole chain, and that bound belongs to iroh.

The fix is a newtype that asserts what the platform already guarantees:

```rust
/// Asserts that a reader is only ever polled on the thread that built it.
///
/// A JS-backed reader is `!Send`, because [`js_sys::JsFuture`] holds an `Rc`.
/// iroh nevertheless requires a protocol handler's future to be `Send` on every
/// target, browsers included, so a `!Send` body cannot travel the handler chain
/// as it stands.
///
/// `wasm32-unknown-unknown` has exactly one thread, so "same thread" is not a
/// precondition here, it is a tautology. iroh and `n0-future` make the same
/// assertion for their own task handles. It stops being true under
/// `-Ctarget-feature=+atomics` with a shared memory, which keylink does not
/// support.
#[cfg(all(feature = "web", target_family = "wasm", target_os = "unknown"))]
struct OnOneThread<R>(R);

// SAFETY: `wasm32-unknown-unknown` without the `atomics` target feature is
// single threaded, so no value of this type can reach a second thread.
#[cfg(all(feature = "web", target_family = "wasm", target_os = "unknown"))]
unsafe impl<R> Send for OnOneThread<R> {}

#[cfg(all(feature = "web", target_family = "wasm", target_os = "unknown"))]
impl<R: AsyncRead + Unpin> AsyncRead for OnOneThread<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}
```

This is the same assertion iroh makes internally. `n0-future`'s browser task
handle wraps its state in `send_wrapper::SendWrapper` with the comment "safe as
long as you keep all of your work on the main UI worker in the browser"
(`n0-future-0.3.2/src/task.rs:252`).

This is the only `unsafe` the proposal introduces, it is confined to the
optional `web` feature, and it is the only place the design leans on a platform
guarantee rather than on the type system. §10 lists it as a risk.

**An alternative if the `unsafe` is unacceptable:** pump the JS stream into a
bounded `tokio::sync::mpsc` channel of `Bytes` from a `spawn_local` task, and
give `Body` a third source that reads the receiver. No `unsafe`, and
backpressure survives with a capacity of one chunk. It costs a new `Source`
variant and starts draining the JS stream at construction time rather than at
send time. That behavior change is why it is the second choice.

---

## 6. The blocking module

### 6.1 A compile error, not a runtime panic

`blocking` becomes a default-on cargo feature. `lib.rs` gains:

```rust
// A blocking API needs a thread to block. `wasm32-unknown-unknown` has one
// thread and it is the one painting the page, so the honest answer is a
// compile error naming the fix, not a runtime deadlock.
#[cfg(all(feature = "blocking", target_family = "wasm", target_os = "unknown"))]
compile_error!(
    "the `blocking` feature needs OS threads, which wasm32-unknown-unknown does not have. \
     Build the browser target with --no-default-features, and re-enable `fs` and `env` only \
     for native builds."
);

#[cfg(feature = "blocking")]
pub mod blocking;
```

Verified. The message appears **first**, above the `tokio::signal` and
`new_multi_thread` errors that follow:

```
$ cargo check -p keylink --target wasm32-unknown-unknown --no-default-features --features blocking
error: the `blocking` feature needs OS threads, which wasm32-unknown-unknown does not have.
       Build the browser target with --no-default-features, and re-enable `fs` and `env`
       only for native builds.
  --> keylink/src/lib.rs:45:1
```

This ordering is why §2's rule matters. Had `blocking = ["tokio/rt-multi-thread",
"tokio/signal"]` been written instead, cargo would build `mio` first, and the
user would read 48 errors about `IoSource<UdpSocket>` before reaching anything
keylink wrote.

The same guard is proposed for `fs` and `env`. Both are verified to fire.

### 6.2 The macro half

`service!` wraps its generated `blocking` module:

```rust
$crate::__keylink_if_blocking! {
/// The same service, for programs that are not async.
pub mod blocking {
    // ... unchanged ...
}
}
```

and keylink defines the helper twice, under its own features:

```rust
/// Emit the tokens only when keylink itself was built with `blocking`.
///
/// A `#[cfg(feature = "blocking")]` written inside `service!` would read the
/// *calling* crate's features, not keylink's, and a caller that happens to have
/// a feature of the same name would flip the wrong switch. Branching here
/// instead resolves against keylink's own cargo features, where the decision
/// belongs.
#[cfg(feature = "blocking")]
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_if_blocking {
    ($($body:tt)*) => { $($body)* };
}

/// The `blocking` half of a generated service, dropped: keylink was built
/// without the `blocking` feature, so there is no runtime to drive it.
#[cfg(not(feature = "blocking"))]
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_if_blocking {
    ($($body:tt)*) => {};
}
```

Without this, a downstream browser crate fails at `service!` expansion even
though keylink compiled. `cargo check -p keylink` cannot catch it. §8 puts a
downstream `cdylib` in CI for this reason.

---

## 7. Timeouts

One line, in `middleware.rs`:

```rust
-  match tokio::time::timeout(budget, next(cx, req, body)).await {
+  match n0_future::time::timeout(budget, next(cx, req, body)).await {
```

`n0_future::time::timeout` re-exports `tokio::time::timeout` on native
(`time.rs:7`) and provides a `gloo-timers` shim in a browser (`time.rs:201`).
The signature and the `Result` shape match, and `Timeout`'s arm matches
`Err(_)`, so the error type name is irrelevant.

`Timeout::budget` stays `std::time::Duration`. `Duration` is arithmetic and
portable. Only `Instant::now()` breaks on this target, and keylink never calls
it.

The cancellation contract in `Timeout`'s doc comment is unchanged: the handler's
future is dropped where it stands.

Verified by `a_timeout_layer_refuses_a_slow_handler`, which passes unchanged.

---

## 8. Verification

### 8.1 The commands

```sh
# native, unchanged behavior
cargo test -p keylink
cargo clippy -p keylink --all-targets -- -D warnings
cargo clippy -p keylink --all-targets --all-features -- -D warnings
cargo clippy -p keylink --lib --no-default-features -- -D warnings

# the browser
cargo clippy -p keylink --target wasm32-unknown-unknown --no-default-features -- -D warnings
cargo clippy -p keylink --target wasm32-unknown-unknown --no-default-features --features web -- -D warnings

# the guards must fail
! cargo check -p keylink --target wasm32-unknown-unknown --no-default-features --features blocking
! cargo check -p keylink --target wasm32-unknown-unknown --no-default-features --features fs
! cargo check -p keylink --target wasm32-unknown-unknown --no-default-features --features env

# the one that actually proves it: a downstream cdylib, LINKED
cargo build -p keylink-browser-example --target wasm32-unknown-unknown --lib
```

### 8.2 `cargo check` is not enough. Use `cargo build`.

This is the single most important line in this section.

`cargo check -p keylink --target wasm32-unknown-unknown --no-default-features`
passed while `cargo build` of the same tree failed at link with:

```
rust-lld: error: undefined symbol: ring_core_0_17_14__limbs_mul_add_limb
rust-lld: error: undefined symbol: ring_core_0_17_14__bn_from_montgomery_in_place
```

`cargo check` never links, so it cannot see this class of failure. The macro
defect in §6.2 is similarly invisible to `cargo check -p keylink`, because it
lives in a downstream expansion. A wasm CI job that only runs `check` proves
much less than it appears to.

### 8.3 What cannot be verified this way

There is no `cargo test --target wasm32-unknown-unknown`. Browser tests need
`wasm-bindgen-test` and a headless browser, and keylink's tests need two
endpoints that can reach each other. `presets::Minimal` gives no transport in a
tab, so a browser test needs `presets::N0` and a live relay. That makes it a
networked integration test, not a unit test.

Recommendation: keep the 43 native tests as the behavioral gate, and add the
browser as a **build-and-link** gate plus one manual smoke page. Promote to
`wasm-bindgen-test` against a relay only if the browser path grows logic of its
own.

### 8.4 The CI job

```yaml
  wasm:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          targets: wasm32-unknown-unknown
          components: clippy
      - run: cargo clippy -p keylink --target wasm32-unknown-unknown --no-default-features -- -D warnings
      - run: cargo clippy -p keylink --target wasm32-unknown-unknown --no-default-features --features web -- -D warnings
      # Links. `check` alone would not have caught the ring archive failure.
      - run: cargo build -p keylink-browser-example --target wasm32-unknown-unknown --lib
      - name: the guards must fire
        run: |
          for f in blocking fs env; do
            if cargo check -p keylink --target wasm32-unknown-unknown --no-default-features --features "$f"; then
              echo "::error::--features $f must not compile for wasm"; exit 1
            fi
          done
```

Two rules for this job.

Scope every command with `-p`. A `--workspace` wasm build pulls in `demo`,
which is native by design and will fail.

Never add `--all-features` to the wasm line. It would turn on `blocking` and
trip the guard.

### 8.5 macOS developers need `llvm-ar`

The link failure in §8.2 has a specific cause, and it is a toolchain problem
rather than a code problem.

ring compiles its C sources to valid wasm objects with Apple clang. It then
archives them with Apple's `ar`, which writes a Mach-O style symbol index.
`rust-lld -flavor wasm` cannot read that index, sees an archive with no symbols,
and reports every one of them undefined.

```
$ file .../out/*montgomery.o
WebAssembly (wasm) binary module version 0x1 (MVP)     # the objects are fine
$ file .../out/libring_core_0_17_14_.a
current ar archive random library                       # the index is not
```

The fix, verified end to end:

```sh
rustup component add llvm-tools
export AR_wasm32_unknown_unknown="$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | sed -n 's/host: //p')/bin/llvm-ar"
cargo build -p keylink-browser-example --target wasm32-unknown-unknown --lib
```

`brew install llvm` and pointing at `/opt/homebrew/opt/llvm/bin/llvm-ar` works
equally well. This belongs in the repository README, next to
`rustup target add wasm32-unknown-unknown`.

GNU `ar` on Linux writes an index `lld` reads, which is why this does not appear
in CI. That last claim is inferred from `browser-echo` shipping a working
deployed build from Linux CI. It was not tested on Linux here.

---

## 9. Sequencing

Seven steps. Each one lands on its own, and each one leaves `cargo test` and
`cargo clippy --all-targets -- -D warnings` green on native.

**Step 1 — Dependency diet.** Manifests only. Replace
`tokio = { features = ["full"] }` in `keylink` with the explicit list and the
target block. Add `n0-future`. Split the iroh feature sets by target.
*Gate:* native tests and clippy. Nothing about wasm yet, and the largest single
reduction in blast radius.

**Step 2 — Runtime items.** `tokio::task::JoinSet` becomes
`n0_future::task::JoinSet`; `try_join_next` becomes `reap_finished`;
`tokio::time::timeout` becomes `n0_future::time::timeout`. Still native-only.
*Gate:* all 43 tests, and specifically
`concurrent_calls_do_not_queue_behind_each_other` and
`a_timeout_layer_refuses_a_slow_handler`. If those two pass, the concurrency and
timeout semantics did not move.

**Step 3 — Features, no target yet.** Add `blocking`, `fs`, `env`, default-on.
Gate `blocking.rs`, `Body::from_file`, both `from_env` functions, their imports,
and the test. Add `__keylink_if_blocking!`.
*Gate:* clippy under default, `--all-features`, and `--no-default-features`,
plus the full test run. Still no mention of wasm, so a reviewer can judge the
feature split on its own merits.

**Step 4 — Target scoping and the guards.** Add the three `compile_error!`
blocks and the target-scoped dependency blocks. This is the first step that
names wasm.
*Gate:* `cargo check -p keylink --target wasm32-unknown-unknown --no-default-features`
goes green for the first time, and all three guards fire.

**Step 5 — CI and the toolchain note.** Add the job from §8.4 and the `llvm-ar`
paragraph to the README. Add a minimal `keylink-browser-example` crate with
`crate-type = ["cdylib", "rlib"]` so the job can link something.
*Gate:* the job is green, and it catches the §6.2 macro defect if it is
reintroduced.

**Step 6 — The `web` feature.** `Body::from_web_file`, `Body::from_web_stream`,
`OnOneThread`, and the three optional wasm-only dependencies. Purely additive
and off by default.
*Gate:* `--features web` clippy for wasm; native untouched.

**Step 7 — Optional, later.** A real browser demo page, and
`wasm-bindgen-test` against a relay if the browser path grows its own logic.

Steps 1 to 4 are the requirement. Steps 5 and 6 are what make it stay true.

---

## 10. Risks, and what could not be verified

### 10.1 Does anything fundamentally NOT work in a browser?

No, with three consequences the documentation must state.

**Every browser connection goes through a relay.** A tab cannot open a UDP
socket, so there is no hole punching and no direct path. iroh's own
`Endpoint::watch_addr` says so structurally: the wasm branch waits on the home
relay and builds an `EndpointAddr` from relay transports only
(`iroh-1.0.3/src/endpoint.rs:1299`). Latency and throughput are the relay's, and
availability depends on it. A browser peer with `RelayMode::Disabled` can reach
nobody.

**`node.online().await` stops being optional.** Natively an endpoint has a
direct address immediately. In a browser it has nothing to publish until the
relay path exists, so an id printed before `online()` may be undialable.

**Concurrency is cooperative.** §4.4. Calls interleave on the JS event loop.
They do not run in parallel.

### 10.2 The five invariants

| Invariant | Status | Why |
|---|---|---|
| 1. Schema-derived ALPN refuses a drifted peer | **Preserved, untouched.** | `derive_alpn` is `postcard-rpc`'s `hash_ty_path` over the type graph. No platform dependency. `postcard-rpc` default features are `[]`. |
| 2. Fail-closed `Allowlist`, no `Default` | **Preserved, and strengthened.** | `Allowlist` is portable. Gating `from_env` on wasm removes a path that would always have reported "unset". |
| 3. Frame cap checked before allocation | **Preserved, untouched.** | `wire.rs` compiles for wasm with no change. |
| 4. Clean close and truncation are distinct | **Preserved.** | The check is in `BodyReader::next_chunk`, independent of the source. §5.2 gives the browser a `declared_len` from `File::size()`. |
| 5. The call site names no transport concept | **Preserved.** | `service!` generates the same client. §6.2 removes only the blocking twin, which is not a transport concept. |

### 10.3 Risks

**The `unsafe impl Send` in §5.3.** Confined to the optional `web` feature. It
is sound on `wasm32-unknown-unknown` without the `atomics` target feature, and
unsound with `-Ctarget-feature=+atomics` and a shared memory. keylink should
state that it does not support threaded wasm. A `compile_error!` on
`all(target_feature = "atomics", feature = "web")` would enforce it; that guard
is proposed but was not tested, because building threaded wasm needs a nightly
`build-std`.

**Bundle size.** The release `.wasm` for a small consumer of keylink is
**5.8 MiB** (6,057,896 bytes), measured with the default release profile. That
is a QUIC stack, a TLS stack and a relay client. `browser-echo`'s profile
(`opt-level = "z"`, `lto = true`, `strip = "symbols"`, `panic = "abort"`) plus
`wasm-opt` will cut it substantially. The reduced figure was not measured. Any
consumer should expect single-digit megabytes and plan for compression.

**`getrandom` is settled today, not forever.** The rustflag is unnecessary
because iroh 1.0.3 pins getrandom 0.4 with `wasm_js`. An iroh bump that moves
that dependency could change the answer. The wasm CI job is what catches it.

**`ring` on wasm.** It compiles and links. The one thing not proven here is
that ring's RNG path is never reached at run time: keylink does not enable
`ring/wasm32_unknown_unknown_js`, and without it `ring::rand::SystemRandom` has
no `SecureRandom` impl on this target. That is a compile-time absence rather
than a runtime panic, and the build is green, so no reached path needs it.
Randomness comes through `rand` 0.10 and getrandom 0.4 instead.

### 10.4 What could not be verified

**Anything at run time in a real browser.** Every claim here is compile-time or
link-time. No `.wasm` was loaded into a browser, no relay was dialed, no call
was made from a tab. The strongest available evidence for the runtime is that
`browser-echo` ships a working deployed build on the same iroh 1.0.x and
`tls-ring` stack.

**Linux and Windows toolchains.** §8.5 was diagnosed and fixed on
macOS/aarch64. The claim that GNU `ar` needs no override is inferred from
`browser-echo`'s Linux CI, not measured.

**The reduced bundle size.** Measured at `opt-level = 3` without lto or strip.

**Firefox, Safari and mobile browsers.** iroh's browser transport uses
WebSockets to a relay. Behavior across engines was not investigated.

**`wasm-bindgen` version coupling.** `browser-echo` pins `wasm-bindgen = "=0.2.122"`
exactly, because the `wasm-bindgen-cli` binary must match the crate. This
proposal's example resolved 0.2.126 and built. Any consumer must pin the crate
to its installed CLI. keylink itself does not depend on `wasm-bindgen`, so this
is a consumer concern.

---

## Appendix A: how this was verified

The design was not reasoned about and written down. It was built and compiled,
outside this repository, at `/tmp/klwasm`:

- `keylink/` — a copy of `keylink/src` plus `keylink/tests`, with the changes in
  this document applied.
- `browser/` — a `crate-type = ["cdylib", "rlib"]` consumer that calls
  `service!`, serves, dials, and streams a body. This is the real browser shape,
  and it is what catches the §6.2 macro defect and the §8.2 link failure.

Nothing under `/Users/pedrocunha/repos/keylink` was modified.

Final state of the matrix:

```
native  default          clippy --all-targets             PASS
native  --all-features   clippy --all-targets             PASS
native  --no-default     clippy --lib                     PASS
native  default          test                       21+16 PASS
wasm    --no-default     clippy                           PASS
wasm    --features web   clippy                           PASS
wasm    cdylib consumer  build (links)                    PASS
wasm    --features blocking     compile_error             FIRES
wasm    --features fs           compile_error             FIRES
wasm    --features env          compile_error             FIRES
```

The 37 tests are keylink's own (21 unit, 16 integration). `demo`'s 6 were not
copied, and `demo` is unchanged by this proposal.

Four findings came from the compiler and would not have come from reading:

1. `n0_future`'s browser `JoinSet` has no `try_join_next` (§4).
2. tokio's `poll_join_next` requires `T: 'static` and the shim's does not (§4.2).
3. `service!` generates a `blocking` module that a browser build cannot resolve,
   and a `cfg` inside the macro reads the wrong crate's features (§6.2).
4. `js_sys::JsFuture` is `!Send` while `JsValue` is `Send`, which decides the
   whole shape of the browser body (§5.3).

The two link-level findings — the ring archive (§8.5) and the size (§10.3) —
came from `cargo build`, and `cargo check` was green for both.
