# Goal 3 of 3: What a peer may learn and may do

**Blocked on goals 1 and 2**, and on a security design that does not exist yet.
This charter names the decision; it does not pre-empt it.

## Intent

A local `Handle` can ask whether an actor is alive, busy, how deep its queue is
and why it ended. A peer needs a **second service on a second ALPN with opt-in
wiring** for the first three, and cannot ask the fourth at all. Separately,
`shutdown` / `abort` / `evict_idle` are reachable from no peer.

Close the read gap. Rule on the write gap **before** building it.

## The reads

`__status()` and `__ended()` should be answerable without a second connection.
One review makes a stronger case for putting them in **`WireRequest`** rather than
on each service's contract: `WireRequest::KeyState` is already answered *"without
entering the handler and without running a layer"*, so the two doors join a
category that is ungoverned on **both** sides — symmetric. Put them on the service
contract and they are governed for a peer and ungoverned for a `Handle`, which is
goal 2's hazard reappearing on keylink's own bookkeeping.

Two corrections the naive shape needs:

- **`__ended()` returns `EndReason`, not `Ended`.** `Ended::Observer` carries
  `Arc<dyn Error>`, which has no schema. `EndReason` exists for exactly this and
  holds one `From<&Ended>` with no wildcard arm, so a new ending is a compile
  error on one line. Reuse it. If the trait method's return differs by surface it
  is not one trait — so the *inherent* `Handle::ended()` keeps the richer `Ended`
  and the *trait* method returns `EndReason` everywhere.
- **`status()` must not return `ActorStatus`.** Two of its four fields (`id`,
  `idle_for`) do not exist for a singleton `Handle`. And **drop `is_running`
  entirely**: remotely a stopped actor answers `NoSuchActor`, so `running: false`
  can never arrive. **The `Result` is the liveness answer.**

There is no reservation mechanism today — `rg 'reserved|RESERVED'
keylink/src/macros.rs` is empty and `$rpc:ident` accepts anything, so a user
declaring `rpc Status()` gets a duplicate-definition error pointing inside
`macros.rs`. `macro_rules!` cannot compare idents; enforcement needs a per-method
`const _: () = assert!(...)`. **Budget it — it is not free.**

## The writes — the decision this goal exists to force

`shutdown` / `abort` / `evict_idle` are id-plus-verb and cross the wire fine. The
problem is authorisation. `Allowlist` is `Only(BTreeSet<EndpointId>) | Anyone`,
**per service, with no per-method dimension**. Put a destructive verb on the
service's own contract and *one allowlist admits both*: any peer permitted to call
`place_order` becomes permitted to destroy the actor and evict the registry — **on
upgrade, with nobody editing an allowlist.** The only in-tree separator is a
`Middleware`, which is the caller's own code, so safety would depend on every
downstream user remembering to mount one. That is fail-open, in a crate whose
`Allowlist` deliberately has no `Default` *"so it can never be what you get by
forgetting"*.

**Recommended: a separate admin service, its own ALPN, its own `Allowlist`.** It
matches the existing grain, and it dissolves `evict_idle`'s scoping problem
because an admin service is naturally system-scoped.

State the tension rather than hiding it: **the reads lose a second ALPN and the
destructive writes gain one**, because they differ in blast radius.

## What must be settled before this goal is executable

1. Reads on `WireRequest` or on the service contract — the symmetry argument
   against the discoverability one.
2. Whether destructive verbs ship at all in this wave, or only the reads.
3. The reservation mechanism's cost, measured rather than assumed.

## Two things a bold sweep will propose, and the answers

- **A peer door for `System::contains`.** `reply.rs:79-81` refuses the
  distinction for a **privacy** reason unrelated to the hash. Record why, or the
  next sweep re-proposes it.
- **"A peer cannot cause an actor to exist."** False — `SystemBuilder::spawn_with`
  already creates by id lazily over the wire. The residual note about `mount` must
  say so; only handing over a *pre-built* `Mounted<M>` is impossible.

Invariants, process, interaction policy and definition of done: **as goal 1**.
