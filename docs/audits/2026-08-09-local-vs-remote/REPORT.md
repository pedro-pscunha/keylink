# Local vs remote: a symmetry audit of keylink's caller surfaces

Written 2026-08-09 against `main` at `0119504`.

Synthesised from three read-only passes in this directory. **Every claim below is
traceable to one of them; they carry the file:line evidence.**

| file | lines | what it holds |
|---|---|---|
| `local.md` | 1057 | the local surface: `service!` grammar, the `Actor` trait, `Handle<A>`, `LocalActor`, every spawn door, the local error types |
| `remote.md` | 1929 | the remote surface: six client types, four dial paths, five serving doors, `System`/`SystemBuilder`, the wire, the ALPN rule |
| `seam.md` | 469 | the divergence: 31 asymmetries classified ESSENTIAL / INCIDENTAL / DELIBERATE, plus the error mapping both directions |

## Corrections this audit made to its own first draft

Recorded because two of them were mine and both reached the user before being
caught.

1. **"A peer cannot ask for liveness, busy-ness, queue depth" was wrong.** All
   three cross the wire as `ActorStatus` over keylink's own `Inspect` service.
   The tell: `ActorStatus` derives `postcard_schema::Schema` and its `buffered`
   field is `u64` rather than `usize`, because `usize` has no wire schema. A
   purely local status type needs neither. See `remote.md` §3.
2. **"One of three passes never delivered" was wrong.** The file was still being
   written — 3m34s newer than the file it was compared against. Absence at one
   instant is not failure.
3. **The seam is five caller surfaces, not two** (six generated client types, if
   the blocking module's own `IdempotentClient` is counted separately).

---

## 1. The surfaces

| surface | type | reached by |
|---|---|---|
| local | `Handle<A>` | `actor`, `actor_with`, `actor_wired`, `actor_deep`, `supervised`, `supervised_deep` |
| remote, unkeyed | `Client` | `Client::connect(node, peer)` |
| remote, unkeyed + ledger | `IdempotentClient` | `Client::with_idempotency` |
| remote, keyed | `KeyedClient` → `ActorRef<'a>` | `connect` then the free fn `at(client, id)` |
| remote, blocking | `blocking::Client` (+ its own `IdempotentClient`) | `connect` |

Two things that do **not** diverge, contrary to expectation: every caller method
on every surface takes `&self` — `&mut self` appears only on the serving `Actor`
trait — and a local call **moves** its arguments into a mail variant rather than
borrowing them.

## 2. Method parity, per declared kind

| kind | `Handle<A>` | `Client` | `ActorRef<'_>` | blocking |
|---|---|---|---|---|
| `rpc` | `Result<T, Undelivered>` | `Result<T>` | `Result<T>` | `Result<T>` |
| `tell` | `Result<(), NotQueued>` | `Result<()>` | **absent** | `Result<()>` |
| `send` (upload) | **absent** | `body: Body` | `body: Body` | **absent** |
| `recv` (download) | **absent** | `BodyReader` | `BodyReader` | **absent** |
| `blob` | `(T, BlobId)` | `(T, BlobHandle)` | `(T, BlobHandle)` | **absent** |

Coverage: `Client` 5/5 · `ActorRef` 4/5 · `Handle` **3/5** · blocking 2/5.

The two absences carry written arguments. `send`/`recv` locally: *"Streaming is a
transport capability, and this handle names no transport."* Keyed `tell`: four
refusals a keyed message can hit are decided **after** the bytes reach the wire
and are reported on the reply, so a keyed `tell` would be fire-and-forget into a
registry that can refuse four ways the caller can never learn about.

## 3. The same condition, both sides

| condition | local caller gets | peer gets |
|---|---|---|
| handler returned | `Ok(T)` | `Ok(T)` |
| handler panicked | **the panic, resumed** | `Failed("panicked: {msg}")` |
| observer refused | `Undelivered::Refused { name }` | `Failed("observer refused: {name}")` |
| observer panicked | `Undelivered::ObserverPanicked { name }` | `Failed("observer panicked: {name}")` |
| live actor dropped the reply | `Undelivered::Dropped` | `Failed("dropped: …")` |
| restart coming | `Undelivered::Restarting` — **fact only** | `Failed("restarting: {reason}")` — fact **and** reason |
| actor gone | `Undelivered::Gone` | `NoSuchActor { id }` |
| queue budget expired | `Undelivered::Expired` — **unreachable locally** | `TimedOut { millis }` |

Shapes: `Undelivered` 7 variants **exhaustive** · `NotQueued` 2 **exhaustive** ·
`Error` 17 `#[non_exhaustive]` · `RemoteError` 5 `#[non_exhaustive]` · plus **5
`Failed` prefix strings** carrying discriminations that are variants on the local
side.

## 4. What each side can learn about the actor

| question | local | peer |
|---|---|---|
| read the state (`with_state`) | yes | **no** — essential, there is no `A` there |
| alive / busy / queue depth | yes | **yes, via `Inspect`** — second ALPN, second connection, opt-in wiring |
| why it ended (`Ended`, 6 variants) | yes | **no** — only through a separate `Inspect` event stream |
| the restart *reason* | **no** | yes |
| mount / shutdown / abort / evict | yes | **no** — the hard gap |

## 5. The asymmetry ledger

**31 total: 16 ESSENTIAL, 8 INCIDENTAL, 7 DELIBERATE.** Full table in `seam.md`.

- **ESSENTIAL** — no `&A` on another machine, no `Observer<A>` trait object across
  a wire, no local `Stream` as a feed, no unwinding stack across a process, no
  identity to authorise in-process, no transport to stream over.
- **DELIBERATE, with the argument on file** — RFC-04 §5.5 (two error types),
  RFC-06 §6 (no new `RemoteError` variants), RFC-07 §9.8 (no supervisor trees),
  the keyed-`tell` refusal, the local `send`/`recv` refusal.
- **INCIDENTAL** — the ones worth closing. See §7.

## 6. The ALPN rule, which decides what "equal" can cost

| change | moves |
|---|---|
| a method, a parameter, a return type, any field at any depth | **that service's** two ALPNs |
| **reordering two variants** | same — postcard encodes by declaration index, so identical shapes decode cleanly as the **wrong** variant |
| a `RemoteError` variant, an `Envelope` field, a `WireKinds` field | **every service in the fleet** |
| a `RemoteError` associated **const** — all five failure prefixes | **nothing** |

That two-tier rule is why the wire uses prefixes instead of variants, and it
names the cost: **a prefix is content inside `Failed(String)`, so the handshake
cannot protect it.** An old peer meets a moved prefix as a `Failed` it silently
stops recognising, which the crate itself calls the more dangerous kind of break.
The variant route has been paid for once already — removing `ActorSchemaMismatch`
renumbered `Failed` from 5 to 4, wire-visible for every service.

## 7. Evaluation — what to do

### Close now: cheap, uncontested, no argument against

1. **Expose the restart reason locally.** A peer gets `restarting: {reason}`; a
   local caller gets a unit variant with the reason discarded, because
   `Undelivered` is `Copy + Eq` and `Arc<Ended>` is neither. `ask_catching`
   already answers the richer type and the generated `Handle` does not expose it.
   The local caller is currently worse-informed than a peer about its own actor.
2. **`ServableAsActor::<A>::CHECK` fires at three of four doors.** Instantiated at
   `Handle::service`, `spawn` and `spawn_wired` — **not** at
   `svc::service(handler, allow)`. So one service declaring a `send` method is
   refused at compile time as an actor and accepted as a handler. Same shape for
   `SpawnableUnfed`, which `spawn` instantiates and `spawn_wired` does not.
3. **`keyed_service` has no `serve` twin**, while the other two serving doors are
   pairs. A registry always spells four steps out.

### Close as ergonomics, not capability

4. **`Inspect` is built and undiscoverable.** Liveness, busy-ness and queue depth
   already cross the wire. What is missing is a pointer from the surface a user
   holds. Do **not** rebuild it.

### Re-argue before relying on it

5. **RFC-04 §5.5** is the permanent argument for two error types and it reasons
   from *"A local mailbox has exactly one failure: the task is gone."* That
   mailbox now has **seven** variants, and the type the argument names
   (`MailboxClosed`) no longer exists. The conclusion may well survive; the
   reasoning has been overtaken by the crate's own evolution and should be
   rebuilt from the current tree.

### Leave

6. Everything the wire physically cannot carry, and the prefix-versus-variant
   trade, which is bought deliberately and priced in §6.

## 8. The structural observation

The defended asymmetry runs one way and the undefended one runs the other.
§5.5 argues a remote caller has failures a local caller cannot have — true, and
essential. It says nothing about the reverse, and the reverse is larger: a local
caller learns six `Ended` variants, the observer's own error, liveness,
busy-ness and queue depth; a peer learns `NoSuchActor` and five prefixed strings.
The only thing on record for that direction is an observation rather than an
argument: *"A peer has no `ended` to fall back on, so this prefix is the whole of
what it learns."*
