# Brooks-Lint Review

**Mode:** PR Review (adapted: charter review of a GOAL document)
**Scope:** `docs/goals/2026-08-09-one-service-one-trait.md`, graded against `main` at `0119504` and the evidence in `.tmp/surf/`
**Health Score:** 0/100 (8 Critical × −15 floors the scale)

The charter is well built and its direction is correct, but eight of its statements are refuted by the tree it is written against — and a charter whose executor may not ask questions must be true, not merely persuasive.

**Read the score as a gate, not as a grade.** Every Critical below is a place where an executor forbidden to ask a question must guess, and where two reasonable guesses give different code.

---

## What I checked, and how

Every claim below cites a file and a line I read. Where I state a count, the command that produced it is named.

| claim in the goal | what the tree says |
|---|---|
| "22 steps" (three times) | **18** — `mise.toml:141`-`207`, counted `grep -c '{ task ='` on the `gate` run list |
| `type Error: Into<Failure>` | `Failure` is not a type. The only one is `use crate::RemoteError as Failure;` at `keylink/src/turns.rs:4500`, inside `mod tests` |
| `pub enum Withheld` (new) | `enum Withheld` **already exists** at `keylink/src/turns.rs:866` |
| "Public error enums stay exhaustive — no `#[non_exhaustive]`" | `RemoteError` (`reply.rs:57`), `Error` (`error.rs:17`), `MountError` are all `#[non_exhaustive]`, on a written rule at `console.rs:398`-`404` |
| "Client types 6 → 3" | 6 − 2 = **4** |
| "The hash moved exactly once" | Two pinned literals, `protocol.rs:1201` and `protocol.rs:1305`, and a procedure in `CONTRIBUTING.md:196`-`210` — none named by the goal |
| `Handle` / blocking to 5/5 | `macros.rs:3095`-`3100` refuses both on a physical argument the goal does not answer |
| item 8 "gated by the existing `Allowlist`" | `Allowlist` is `Only(BTreeSet<EndpointId>) \| Anyone` (`authz.rs:133`-`138`) — no per-method granularity exists |

---

## Findings

### 🔴 Critical

**Domain Model Distortion — `Withheld` is already a type in this crate, and a break record quotes it**

Symptom: The goal introduces `pub enum Withheld` with seven variants including `Panicked { message: String }`. `keylink/src/turns.rs:866` already declares `enum Withheld` with two variants — `Vetoed { name, fatal }` and `Panicked { name }` — meaning *an observer withheld this turn's value*. In the existing type, `Withheld::Panicked` is **the observer**; in the proposed type, `Withheld::Panicked` is **the handler**, and the observer moves to `ObserverPanicked`. The same path in the same file would carry both. Worse, `keylink/tests/observe.rs:446`-`448` is a break record whose watched sentence is the literal `(Ok(_), Some(Withheld::Panicked { name })) => Answered::ObserverPanicked { name },`, and `docs/rfc-06-observers.md:1243` describes the two-armed `Withheld` by name in an append-only file.
Source: The Pragmatic Programmer — DRY as duplicated *decision*; Evans, *Domain-Driven Design* — Ubiquitous Language, one word one meaning.
Consequence: Two enums named `Withheld`, both with a `Panicked` variant carrying different payloads and opposite meanings, in the module a reader goes to first. The mechanical failure is sharper than the naming one: the Invariants require every break record's search literal to resolve **exactly once**, and `RECORD_BLOCKS = 177` (`.mise/tasks/anchors:3239`) is a hard tripwire. Introducing a public `Withheld::Panicked` makes that literal resolve twice, so `mise run anchors` reddens for a reason no one predicted, in the middle of a 13-item execution.
Remedy: Do not take the name. Two options, both better than the goal's: (a) rename the private observer enum to `Veto` — it is private, two-armed, and `Vetoed`/`Panicked` are already the veto vocabulary — and register the rename against the `observe.rs` record; or (b) **use the name the crate already has for this concept**: `TurnLost`. See the Major finding on `TurnLost` below — (b) also removes a third spelling and is the design I would argue for.

---

**Accidental Complexity — one trait cannot serve `blocking::Client`, and the crate's own guard proves it**

Symptom: The goal's trait methods return `impl Future<Output = …> + Send`. `blocking::Client`'s generated methods are `pub fn name(&self, …) -> Result<T>` and their whole body is `self.runtime.run(self.inner.name(…))?` (`macros.rs:3084`-`3094`). `Runtime::run` begins with `guard()?` (`blocking.rs:77`-`83`), and `guard()` is:

```rust
fn guard() -> Result<()> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(Error::BlockingInAsync);
    }
    Ok(())
}
```
(`blocking.rs:115`-`120`)

Source: Ousterhout, *A Philosophy of Software Design* — an interface that is not deeper than its implementations; Winters et al. — a de-facto API created by observable behaviour.
Consequence: **The parent's belief is confirmed.** A trait impl for `blocking::Client` can only be written as `std::future::ready(self.inner_sync(…))`. Awaiting it requires an async context; an async context makes `Handle::try_current()` succeed; `run` therefore returns `Err(Error::BlockingInAsync)` — whose own message is *"use the async API here"* (`error.rs:89`). So `settle(&blocking_client).await` **compiles and always fails** inside any `#[tokio::test]`, which is how every served test in `keylink/tests/` is written. Success criterion 1 says "compiles and passes"; an executor will get it to compile, watch it fail, and then either (i) drive it under a non-tokio executor at the top level — passing the criterion while proving nothing a user will ever do — or (ii) make the impl delegate to the inner **async** client, at which point `blocking::Client` is the async client wearing a blocking name and the feature is destroyed. Both are green transcripts and both miss the point. This is the clearest instance of the parent's question 5.
Remedy: State the honest shape in the goal. The trait covers the three **async** surfaces — `Handle`, `Client`, `ActorRef`. `blocking::Client` gets a **second generated trait** with synchronous methods (`orders::Blocking`), one contract, two traits, because sync and async are not one interface and this crate already refuses to pretend otherwise at `blocking.rs:114`. Then rewrite success criterion 1 as two criteria: one generic async function over three surfaces, one generic sync function over the blocking surface. Do not `#[cfg]` and do not drop the blocking client — the `#[cfg]` route reintroduces exactly the failure `__keylink_if_blocking!` was added to fix (`macros.rs:4038`-`4042`).

---

**Domain Model Distortion — `Failure` does not exist, and the type that would fill the hole reopens the defect the goal forbids**

Symptom: The public-interface block declares `type Error: Into<Failure>;` and the worked example returns `Result<u64, Failure>`. `rg '\bFailure\b'` over the workspace returns five hits, all in `keylink/src/turns.rs:4500`-`4505`, all inside `mod tests`, and all from one line: `use crate::RemoteError as Failure;`. The section is headed *"Designed here so review can check nothing else escaped"*, and the central type of the design escaped.
Source: Brooks, *The Mythical Man-Month* — Conceptual Integrity; McConnell, *Code Complete* — a name that requires reading the implementation.
Consequence: An executor forbidden to ask must invent a public type or repurpose `RemoteError`. If it picks `RemoteError`, then `Undelivered: Into<RemoteError>` must map `Undelivered::Reentrant`, which has no remote counterpart and must not acquire one — so the conversion is lossy or `RemoteError` grows a variant a peer can never see. That is **representable-but-impossible**, the exact defect the goal's own Context-dependent invariant forbids two sections earlier: *"A unified enum would force a local caller to write `SchemaMismatch` and `Transport` arms that can never fire."* The goal moved that problem from `Self::Error` to `Into<Failure>` and did not notice it arrive.
Remedy: Decide it in the goal. Either (a) delete the `Into<Failure>` bound entirely — `type Error: std::error::Error + Send + Sync + 'static` is enough for `settle` to write `Box<dyn Error>` and keeps every surface's error precise, which is the whole reason for an associated type; or (b) define `Failure` in the public-interface block, name its variants, and state what `Undelivered::Reentrant` and `Error::BlockingInAsync` become inside it. (a) is smaller and is what `ponytail` would pick.

---

**Knowledge Duplication — the "no `#[non_exhaustive]`" invariant inverts a rule this crate argues in writing**

Symptom: Under `## Invariants → Universal (this repo)`: *"**Public error enums stay exhaustive** — no `#[non_exhaustive]`, no wildcard arm inside the crate."* `keylink/src/console.rs:398`-`404` states the crate's actual rule, and it is two rules pointing opposite ways:

> **Error enums are `#[non_exhaustive]`** in this crate — [`RemoteError`], [`MountError`], [`Error`] — and this is one. **Enums a caller matches exhaustively are not**, which is why [`EndReason`] and [`LifecycleEvent`] are open […] That is the difference, and it is why the rules disagree without either being wrong.

`RemoteError` carries `#[non_exhaustive]` at `reply.rs:57`; the goal's proposed `RemoteError` block omits it silently.
Source: The Pragmatic Programmer — DRY as duplicated decision; Winters et al. — backward compatibility and coordinated upgrades.
Consequence: An executor grading itself against `## Invariants` — and the goal mandates a **dedicated invariant-compliance wave** that treats anything it cannot evidence as violated — will strip `#[non_exhaustive]` from `RemoteError`, `Error` and `MountError`. That is a semver-breaking change to three public types, outside every scope item, contradicting a written argument in the tree, delivered by an agent that believes it is obeying the goal. The invariant wave will then certify it as compliant.
Remedy: Replace the bullet with the crate's real two-part rule, quoting `console.rs:398`-`404`, and add `#[non_exhaustive]` to the `RemoteError` snippet in the public-interface block. State explicitly that `Withheld` is in the second class (matched exhaustively, therefore open-free) and that `RemoteError` stays in the first.

---

**Change Propagation — renaming `lost` is forbidden by this goal's own append-only invariant**

Symptom: Scope item 11 renames `NodeStatus::lost` / `EventPage::lost`. Both identifiers are named in `docs/rfc-07-the-turn-model.md` at lines **3238-3239** and **3410-3411**. The Invariants say `docs/rfc-*.md` is append-only with one carve-out: *"an identifier **a break record names** may be renamed in place."* `rg 'NodeStatus|EventPage' .mise/tasks/anchors` returns **nothing** — no break record names either identifier, so the carve-out does not reach them.
Source: Fowler, *Refactoring* — Shotgun Surgery; Feathers — a change with no safe seam.
Consequence: The executor hits a fork with no correct branch. Edit RFC-07 and the append-only invariant is violated and success criterion 13 (`git diff main...HEAD -- docs/ | rg '^-'` prints only carve-out renames) goes red. Leave RFC-07 alone and two RFC paragraphs name a field that no longer exists — which is the doc rot the append-only rule exists to prevent. Either way the invariant wave reports a violation and the loop reopens, on the cheapest item in the goal.
Remedy: Decide it in the goal, in one sentence: *"RFC-07 §10.8 and its later entry are corrected by **appending** a note recording the rename; the old names stay in place as history."* And widen the carve-out sentence, or say plainly that it does not apply here.

---

**Dependency Disorder — item 8 hands every allowed peer the power to destroy the actor**

Symptom: Item 8 makes `shutdown` / `abort` / `evict_idle` peer-reachable, *"gated by the existing `Allowlist`"*. `Allowlist` is `enum Allowlist { Only(BTreeSet<EndpointId>), Anyone }` (`authz.rs:133`-`138`), passed once to `ServiceBuilder::new(alpn(), allow, dispatch)` (`macros.rs:732`). There is no per-method dimension: `rg 'allow|Allowlist' keylink/src/macros.rs` returns no method-scoped use.
Source: Martin, *Clean Architecture* — Interface Segregation, and policy/detail boundaries; McConnell — defensive programming at a trust boundary.
Consequence: The grant that today means *"you may call `place()`"* becomes *"you may call `place()` **and destroy this actor and evict the registry**"*. Every existing deployment silently widens on upgrade — nobody edits an allowlist and everybody's blast radius grows. `Allowlist::Anyone`, whose own doc calls it *"catastrophic by accident"* (`authz.rs:137`), becomes catastrophic on purpose. The per-method mechanism that does exist — `Middleware` via `ServiceBuilder::layer` (`rpc.rs:1390`) — is caller-supplied and optional, so gating on it is a policy the serving side can always be built without.
Remedy: The destructive doors need a **second, default-deny** gate that is not the call allowlist — an explicit `ServiceBuilder::admin(Allowlist)` defaulting to `Only(empty)`, so an existing service gains nothing until its author opts in. State it in the goal's public interface. If that is too much for this goal, cut item 8's `abort`/`evict_idle` and keep only what is observational; do not ship the widening behind a sentence that reads like a gate.

---

**Cognitive Overload — the reserved doors are named two ways, and nothing can enforce them today**

Symptom: Scope item 7 says *"Reserved `__status()` and `__ended()` doors"*. The public-interface block, thirty lines later, declares `fn status(&self)` and `fn ended(&self)` — no underscores. Success criterion 7 says `__status()` / `__ended()`. Separately, `macros.rs:405` and `485` parse a method as `$kind:ident $rpc:ident ( … ) -> $ret:ty`, and `rg 'reserved|RESERVED' keylink/src/macros.rs` returns **nothing**: there is no name reservation in this macro today, and no precedent to copy.
Source: McConnell, *Code Complete* — naming; Ousterhout — information leakage through an interface a caller must guess at.
Consequence: Two failures, one per branch. Named `status`/`ended`, a user writing `rpc Status() -> u8;` gets a duplicate-definition error pointing **inside `macros.rs`**, not at their declaration — the worst diagnostic a macro can emit, and `Handle` already has an inherent `ended()` (`macros.rs:2378`) that this would shadow-collide with. Named `__status`/`__ended`, the collision is unlikely but the goal still names no enforcement, so the reservation is a comment. Either way the executor picks one, and the reviewer at the merge gate reads a different one in the same document.
Remedy: Pick `__status` / `__ended` — it matches the crate's existing `__keylink_*` convention for generated-and-not-yours — and fix the public-interface block and the example to match. Then state the enforcement, because `macro_rules!` cannot compare two idents: emit, per declared method, a `const _: () = assert!(!matches!(stringify!($rpc).as_bytes(), b"Status" | b"Ended"), "…");` so the failure is a const-eval message the user can read. Budget it as work; it is not free.

---

**Accidental Complexity — items 3 and 5 buy "5/5" with a physical cost the goal forbids discussing**

Symptom: Item 3 gives `Handle` local `send`/`recv` *"(generalise `FrameReader` over `AsyncRead`)"* — a transport-layer refactor stated as a parenthesis. Item 5 takes the blocking client 2/5 → 5/5. `macros.rs:3095`-`3100` refuses all of these on one argument, and the audit quotes its full form at `.tmp/surf/seam.md:405`-`421`: a `BodyReader` reads a QUIC `RecvStream` held by value, a `Body` drains into a `SendStream`, *"neither half exists in this process"*, and the two exits are **buffer a 4 GiB upload** or **generalise `FrameReader` over `AsyncRead`**. The blob refusal at `macros.rs:3098`-`3099` is the same fact: *"a synchronous wrapper would either buffer the whole blob or hand back a handle that only works inside a runtime."* Meanwhile `## Out of scope` says *"Any performance work. If a change costs an allocation on a failure path, say so; do not optimise it here."*
Source: Brooks — the Second-System Effect; Ousterhout — strategic versus tactical, and paying for generality with no current requirement.
Consequence: A blocking `send` at 5/5 is an unbounded in-memory buffer on the **happy** path, not an allocation on a failure path, so the out-of-scope sentence does not cover it and gives the executor cover to ship it without saying so. `FrameReader` is the subject of gate step 1 (`framing`) and is built two ways for wasm (`wasm-streams`); generalising it over `AsyncRead` is the largest single piece of engineering in this goal and it is one parenthesis inside a bullet whose headline is "method parity". The 13-item plan will be sized as if item 3 were macro work.
Remedy: Split item 3. `blob` unification and `ask_catching` exposure stay. Local `send`/`recv` and blocking `send`/`recv`/`blob` become their own goal, or are moved to `## Out of scope` beside `with_state` and `mount` with the audit's argument quoted — which is what the evidence actually supports: `.tmp/surf/seam.md:405` calls them *"the cleanest ESSENTIAL pair in the crate"*. If they stay in, the goal must say **which** exit is bought, and price it.

Also unresolved and blocking on its own: item 3 says *"`blob` returning one type on both sides"* and never says **which** type. Local returns `BlobId`, remote returns `BlobHandle`, and `macros.rs:3911`-`3915` says the difference is that a handle names a holder. An executor that cannot ask will pick one, and the two picks are different public APIs.

---

### 🟡 Major

**Knowledge Duplication — `TurnLost` is the third spelling of the same seven facts, and the goal does not touch it**

Symptom: After this goal, `Withheld` names 7 conditions. `TurnLost` (`observe.rs:647`) names 8 — `Gone`, `Dropped`, `Restarting(Arc<Ended>)`, `Refused{}`, `ObserverPanicked{}`, `Faulted(Panic)`, `Expired`, `Reentrant` — which is `Withheld` ∪ `{Reentrant}` with `Faulted` in place of `Panicked`. The goal deletes the fourth spelling (the five prefix strings) and leaves three. It names the twin guard `every_turn_lost_except_faulted_has_one_undelivered_twin_with_the_same_words` under "reuse rather than duplicate" and never says what becomes of it — that guard's premise is variant-per-variant correspondence, and `Undelivered` is about to go from 7 flat variants to 2 nested ones.
Source: The Pragmatic Programmer — DRY; `console.rs:172` states the crate's own position: *"This crate refuses parallel types for one concept."*
Consequence: Success criterion 2 requires `Withheld` to be equal on both sides *"for all seven conditions"*. `Panicked` is not reachable through the local `Handle` at all — `ask` resumes the panic (`turns.rs:3905`) and only `ask_catching` yields a value, as `TurnLost::Faulted(Panic)` (`turns.rs:3941`). So the criterion **cannot be met without changing `TurnLost`**, and the goal never authorises it. The executor discovers this mid-run, cannot ask, and either widens scope silently or writes a test that asserts six conditions and calls it seven.
Remedy: Make it explicit and make it the better design: `TurnLost` becomes `{ Faulted(Panic), Reentrant, Turn(Withheld) }`, the same shape as `Undelivered`. Then one enum names the shared middle on all three surfaces, `Faulted` keeps the payload that lets `ask` resume the unwind (which is genuine physics), and the twin guard becomes a **stronger** statement: two wrappers, one payload, checked by the type system instead of by matching words. This also supplies the honest answer to the Critical naming collision above.

---

**Domain Model Distortion — `Withheld::Restarting { reason: String }` throws away structure the crate already carries on the wire**

Symptom: The goal chooses `String` and defends it: *"`Ended` derives `Clone` but not `PartialEq`, which is why the reason crosses as rendered text and not as `Arc<Ended>`."* True about `Ended`. But `EndReason` (`console.rs:184`) derives `Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema` — exactly the set required — is already on the `Inspect` wire, mirrors `Ended`'s six variants one-for-one, and exists precisely because `Ended::Observer` carries an unencodable `Arc<dyn Error>`. The goal itself names `EndReason` in scope item 7 for this exact job.
Source: Fowler — Primitive Obsession; Evans — Value Object.
Consequence: The one place where a peer is currently **better** informed than a local caller (`.tmp/surf/seam.md:388`) gets levelled down instead of up: both sides end up with a rendered sentence where a six-way typed discrimination was available for free. Every consumer of a restart reason then parses text, and the crate has just spent a week deleting the last thing that parsed text.
Remedy: `Restarting(EndReason)`. It satisfies `Clone + PartialEq + Eq + Schema`, it is already hashed into a contract so it costs nothing new on the wire, and it is the type the goal already reaches for one item earlier.

---

**Change Propagation — restructuring `Undelivered` is stated as if only `assert_eq!` sites were affected**

Symptom: Success criterion 8 reads: *"`Undelivered` keeps `Clone + PartialEq + Eq` and every existing `assert_eq!` on it still compiles."* The change turns 7 flat variants into `{ Turn(Withheld), Reentrant }`. Counts from the tree: `rg -o 'Undelivered::[A-Za-z]+'` returns **142 variant-path uses** (Gone 42, Refused 27, Reentrant 24, Restarting 15, Dropped 15, ObserverPanicked 11, Expired 10), across 207 total `Undelivered` references, with 24 `match`/`Err(Undelivered::…)` sites.
Source: Fowler — Shotgun Surgery; Feathers — change without a seam.
Consequence: 118 of those paths (everything except `Reentrant`) stop compiling. The criterion names the one category that mostly survives and is silent on the category that does not, so a planner sizing item 1 from the criteria will under-size it by an order of magnitude — and a reviewer checking the criterion will find it satisfied while the real work is unmeasured.
Remedy: Restate criterion 8 against the real quantity: *"`rg -c 'Undelivered::' ` is N before and N after, every site rewritten, none deleted"*, with N read off the run. And add a `From<Withheld> for Undelivered` plus per-variant associated constructors if the goal wants call sites to stay short.

---

**Cognitive Overload — dropping `Copy` was measured against one syntax only**

Symptom: The Context-dependent invariant reads: *"**Measured: zero** `@`-binding sites depend on `Copy` for `Undelivered`."* `@`-bindings are one of several `Copy`-dependent forms. Dereference of a borrowed value (`*e`), `Option::copied`, `Result<T, Undelivered>: Copy` propagating to a containing type, and any `Cell<Undelivered>` are others, and none was measured.
Source: my own standing note — *measure the quantity the mechanism uses, not the one that is easy to read off*.
Consequence: The invariant reads as a completed measurement and is a partial one. If a single `*err` exists behind a `&Undelivered`, the executor meets a compile error the goal told it could not happen, and the goal's authority on the next invariant is weaker for it.
Remedy: Either re-measure with the full set (`rg -n '\*[a-z_]+' ` over the `Undelivered` sites plus `.copied()`), or downgrade the sentence to what was actually checked: *"zero `@`-binding sites; other `Copy`-dependent forms not surveyed — expect breakage and fix it."* The second is honest and costs nothing.

---

**Domain Model Distortion — splitting `Gone` from `NoSuchActor` overturns a written privacy argument, and the goal does not record the cost**

Symptom: The user's decision to keep them distinct is settled and I am not reopening it. But `reply.rs:79`-`81` argues the current conflation on purpose: *"Deliberately does NOT distinguish 'never existed' from 'was stopped'. The caller's next action is the same either way, and **telling them apart would leak that an id was once real**."* The goal calls the conflation *"the defect"* and says nothing about the argument it overturns.
Source: Winters et al. — Hyrum's Law and observable behaviour as a de-facto API.
Consequence: After the change a peer holding a list of candidate `ActorId`s can distinguish *this id ran and stopped* from *this id never existed* — an enumeration oracle over the registry, delivered as a side effect of an error-typing change. That may be an acceptable trade; it is not a trade anyone will notice being made, because the only place it is written down is a rustdoc paragraph the diff will delete.
Remedy: Add a scope line: *"`reply.rs:79`-`81`'s non-disclosure argument is overturned; append the counter-argument beside it and state that actor-id existence is now observable to an allowed peer."* One paragraph, and the decision survives review a year from now.

---

**Knowledge Duplication — "the hash moves exactly once" names no instrument, and the two that exist are outside the blast radius**

Symptom: The Definition of done says *"**The hash moved exactly once.** Two moves is a failure."* There is no single hash: each service has `alpn()` and `keyed_alpn()`, and keylink's own `inspect` has a third. The tree holds exactly two pinned literals — `const PINNED: &str = "keylink/Probe/d75913b8a4742547";` (`tests/protocol.rs:1201`, test `the_contract_hash_has_not_moved`) and `const PINNED: &str = "keylink/Inspect/0572763878b7fb9d";` (`tests/protocol.rs:1305`, test `the_inspect_contract_hash_has_not_moved`). `CONTRIBUTING.md:196`-`210` governs them: *"1. Decide whether this change **should** be visible on the wire. 2. Only then update the literal, **and say so in the release notes**. Never step 2 without step 1."* The goal names neither test, neither literal, nor `CONTRIBUTING.md` — which appears in **neither** the may-modify nor the must-not-touch list.
Source: Feathers — Characterization Tests; Winters et al. — coordinated upgrade.
Consequence: Both pins will go red — `RemoteError` is in the shared hashed graph so it moves every service, and items 9 and 11 move `Inspect` on their own. An executor with no pointer to `CONTRIBUTING.md`'s procedure does the natural thing: updates a literal to turn a red test green, which is the one act that file names as *"how a deployed fleet silently splits in half"*. And the Definition-of-done item cannot be evidenced either way, so the invariant wave — which treats unevidenced as violated — fails the whole run on a criterion with no proof attached.
Remedy: Rewrite the item as something checkable: *"Both `PINNED` literals in `keylink/tests/protocol.rs` take exactly one new value, in one commit, each read off its own failing run; `CONTRIBUTING.md:196`-`210`'s step 1 is answered in the commit body."* Add `CONTRIBUTING.md` to the may-modify list. Also note that `tests/protocol.rs:1296`-`1298` currently claims *"`Probe`'s hash did not move in the same run"* as evidence the two pins discriminate — this change moves both, so that paragraph needs an appended sentence or it becomes a false claim about the tree.

---

**Cognitive Overload — "Client types 6 → 3" does not arithmetic, and its proof passes at 4**

Symptom: Context counts six: `Handle<A>`, `Client`, `IdempotentClient`, `KeyedClient` + `ActorRef`, blocking `Client`, blocking `IdempotentClient`. Item 6 deletes the two `IdempotentClient`s. 6 − 2 = 4. Success criterion 6 says *"Exactly three generated client types remain (`Client`, `KeyedClient`, `blocking::Client`)"* — omitting both `Handle` (which criterion 5 requires to survive at 5/5) and `ActorRef` (which criterion 1 and the worked example both require). The attached proof is `rg 'pub struct IdempotentClient' keylink/src/macros.rs` is empty.
Source: McConnell — magic numbers; my standing note — *a count is wrong only against the unit it names*.
Consequence: The headline number is unreachable and the proof is satisfied at the reachable number, so the criterion passes with its own claim false. This is the second clean instance of the parent's question 5, and it is the kind of figure that gets quoted into a PR body and then into a README.
Remedy: *"Six generated caller types become four: `Handle`, `Client`, `KeyedClient`/`ActorRef`, `blocking::Client`. Proof: `rg 'pub struct IdempotentClient' keylink/src/macros.rs` is empty, and `rg -c 'pub struct [A-Z]' keylink/src/macros.rs` reads N."*

---

**Accidental Complexity — the gate is 18 steps, not 22**

Symptom: "22 steps" appears in success criterion 9, in the Definition of done, and in the `/goal` completion condition. The `gate` run list at `mise.toml:141`-`207` holds 18 entries: `framing`, `anchors`, `anchors-selftest`, `task-lists`, `fmt-check`, `deny`, `replay-anchors --selftest`, `lint`, `features`, `test`, `examples`, `docs`, `wasm`, `wasm-guards`, `wasm-web`, `wasm-browser`, `package`, `msrv`.
Source: The Pragmatic Programmer — assertion as executable documentation, and its inverse.
Consequence: Three copies of one wrong number, one of them inside the completion condition an autonomous run is graded on. The executor prints 18 green steps and cannot satisfy a criterion demanding 22 — so it either reports failure on a clean gate, or invents four steps, or edits `mise.toml`'s step list, which the blast radius explicitly forbids. All three are worse than the one-character fix.
Remedy: Write 18, and cite where it was counted. Better: drop the number from the criterion and say *"`mise run gate` exits 0 with every step in `mise.toml`'s `[tasks.gate]` list printed"* — a count embedded in a document is a value the document does not control, and this one already rotted once.

---

**Cognitive Overload — three success criteria are not binary**

Symptom: (a) Criterion 3: *"Zero `starts_with` on a served failure anywhere — `rg 'starts_with' keylink/ demo/ keylink-persist/` shows no hit on a `RemoteError`/`Failed` value."* That command returns **26** hits today across 12 files, most unrelated; "no hit on a `RemoteError`/`Failed` value" is a human reading, not an exit code. (b) Criterion 2 says *"byte-identical in type and value"* and then asks for value equality — two different assertions in one sentence. (c) Criterion 13's `git diff … | rg '^-'` *"prints **only** identifier renames under the carve-out"* is a judgement over a diff, and the Critical on `lost` above shows the carve-out will not cover what the goal needs it to.
Source: Meszaros — Assertion Roulette; the goal's own rule, *"Each is binary and names its proof."*
Consequence: A criterion that needs a human to read output is not a criterion an unattended executor can discharge, and the invariant wave will grade it by whether the command ran, not by what it showed.
Remedy: (a) `rg -n 'starts_with\(RemoteError::|starts_with\(Failure::' keylink/ demo/ keylink-persist/` is empty — an exact, exit-code-checkable predicate. (b) Drop "byte-identical"; say `assert_eq!(local, remote)` on the two `Withheld` values. (c) Keep the command but state the expected line count, read off the run.

---

**Accidental Complexity — item 12 asks a question in a goal that forbids questions**

Symptom: *"Rule on `Undelivered::Expired`, which is public and unreachable locally."* "Rule on" is a decision request, and `## Execution interaction policy` says *"Questions during execution: not allowed."* The goal has in fact already ruled — `Expired` is in the proposed `Withheld` — so two sections disagree about whether the question is open.
Source: Brooks — Conceptual Integrity.
Consequence: The executor must decide a public-API question with no criterion, then discover the interface block decided the opposite, then pick. `.tmp/surf/seam.md:175`-`180` holds the material for a real decision and the goal cites none of it.
Remedy: Rule on it here: keep `Expired` in `Withheld` **and** close the gap that makes it unreachable — `.tmp/surf/seam.md:447`-`449` notes `QueueTimeout` is a `Dispatch` property, not a remote one, so a local `Handle` on a served actor could honour it. Or delete the variant. Either is a decision; "rule on" is not.

---

**Change Propagation — item 13 pre-announces the conclusion of the argument it commissions, and the conclusion has changed**

Symptom: *"Re-argue RFC-04 §5.5 by appending. Its conclusion (two wrapper types) survives."*
Source: Brooks — Conceptual Integrity; the audit's own framing at `.tmp/surf/REPORT.md:145`-`151`, which says the conclusion *"may well survive"*.
Consequence: An argument whose verdict is given in the brief is ceremony. Worse, the verdict is no longer accurate: §5.5 concluded *two error types that share nothing*, and after this goal they share `Withheld` as a payload and differ only in their private arms. That is a materially different structure, and an appended section restating the old conclusion documents the tree incorrectly on the day it is written.
Remedy: *"Append a section that re-derives the split from the current tree — 7-variant `Undelivered`, no `MailboxClosed`, a shared `Withheld` payload. State the conclusion you reach; do not assume it."*

---

**Brooks's Law / Second-System Effect — this is three charters wearing one hat**

Symptom: 13 in-scope items, spanning the wire format, the macro's generated surface, the transport layer (`FrameReader`), the registry's administrative doors, a documentation re-argument and an evidence copy. Four of the eight Criticals above come from items that share nothing with the goal's stated intent.
Source: Brooks — the Second-System Effect; Ousterhout — strategic programming has a scope, too.
Consequence: One review gate over a diff this shaped cannot be a single pass, and a Critical in item 8 (registry doors, security) blocks items 1-2 (the actual thesis) from landing.
Remedy: Split at two seams that the evidence already draws:
1. **The shared failure vocabulary** — items 1, 9, 10, 13, plus the `TurnLost` unification. One wire move, one thesis: *one enum names what happened to a turn, on both sides.* This is the goal that is ready.
2. **One trait, both sides** — items 2, 6, 12, plus the `blob` unification and `ask_catching`. Depends on (1) for its error type, and it is where the blocking-trait question is answered.
3. **Peer-reachable actor administration** — items 7, 8, and the `Inspect` folding. This one needs a security design (default-deny admin gate) and should not ride on the other two.
Item 3's `FrameReader` generalisation and item 5's blocking streaming belong in none of the three; see the Critical above. Item 11 (`lost`) is a five-minute rename that can go on its own today.

---

### 🟢 Minor

**Cognitive Overload — the generated trait is named three ways.** `pub trait Orders` in the interface block, `impl orders::Orders` in the example, `impl <svc>::Trait` in criterion 1, and *"a generated trait per service"* in item 2. Pick one; `orders::Orders` reads best and collides with nothing the macro emits (`Request`, `Response`, `Mail`, `Feed`, `Feeds`, `Actors`, `Handle`, `KeyedClient`, `ActorRef`, `Client`, `IdempotentClient`, `Handler`, `Actor`, `blocking`).

**Domain Model Distortion — `EventPage` already holds three loss counters, not two.** `console.rs:344`-`367`: `lost` (ring eviction, per console) and `dropped` (produced-and-never-sequenced, the same reading as `NodeStatus::lost`). Item 11 describes a two-way clash; it is three-way, and `NodeStatus::lost` should almost certainly become `NodeStatus::dropped` to match the name `EventPage` already gives that quantity — which makes the rename smaller than the goal implies, and points at the field that keeps its name.

**Knowledge Duplication — the reserved doors' wire position is undecided.** `__Status`/`__Ended` become variants of the generated `Request`/`Response` (`macros.rs:509`, `514`), and postcard encodes by declaration index (`node.rs:157`). Whether they are emitted before or after the user's methods is a wire decision the goal leaves to whoever writes the macro arm. Say "appended last" and the reason.

**Accidental Complexity — the trait impls need feature and target gating.** `blocking` is a default feature (`keylink/Cargo.toml:17`) and does not exist on wasm; `features` and the four `wasm*` steps are gate steps 9 and 13-16. Whatever shape the blocking side takes, the goal should say the impl lives inside `__keylink_if_blocking!` — `macros.rs:4038`-`4042` records what happens when that guard is missing.

**Cognitive Overload — the evidence-copy destination is unnamed.** Criterion 14 says `.tmp/surf/*.md` copied *"into `docs/`"*. `docs/` holds `rfc-*.md` and `goals/`; four audit files landing loose beside eight RFCs will read as RFCs. Name `docs/audits/2026-08-09-local-vs-remote/`.

**Cognitive Overload — `RECORD_BLOCKS` is 177 today** (`.mise/tasks/anchors:3239`). The goal handles it correctly ("read off the failing run"); recording the current value here saves the executor a lookup and makes a wrong new value visible.

---

**Recommended fix order:** the two naming Criticals first (`Withheld` collision, `Failure` undefined) because they change the public interface block everything else is written against; then the blocking-trait shape, because it decides whether criterion 1 is one criterion or two; then the three invariant conflicts (`#[non_exhaustive]`, append-only vs `lost`, the `Allowlist` gate); then the scope split; then the counts.

---

## What else should close, and is not in the goal

The instruction was to minimise every difference that can and should be eliminated. These are the ones the goal leaves on the table. Ordered by what I would actually do.

### 1. The axis is wrong: the real divide is *served* versus *not served*, not local versus remote

`.tmp/surf/seam.md:427`-`449` establishes this and the goal does not act on it. A `Middleware` layer wraps a `Dispatch` (`rpc.rs:1390`); a `Dispatch` exists only when `Handle::service` builds one (`macros.rs:2147`); a local method calls `ask` and never constructs one (`macros.rs:3853`). So on **one served actor, sharing one mailbox** (`macros.rs:2035`-`2036`), a peer passes through every layer, the authorization check and the idempotency ledger, and the local `Handle` passes through none of them.

**Why it should close, and why this goal makes it urgent:** the goal's entire product is `fn settle(o: &impl Orders)` — a function that does not know which surface it holds. Today that ignorance is safe because the two surfaces are different types with different methods. After the goal it is unsafe: the same generic function, handed a `Handle`, silently bypasses the rate limiter, the audit log and the idempotency ledger that the same function gets when handed a `Client`. That is a policy difference invisible at the call site, in a crate whose thesis is *fail closed*. It is the most consequential thing the goal creates and it is not mentioned once.

**Cost to close:** low, if closed as documentation and a type-level marker rather than as behaviour. Add an associated `const SERVED: bool` (or a `Caller`-shaped associated type) to the generated trait so generic code can refuse or branch, and state the bypass in the trait's own rustdoc under *mechanism and reason*. **Do not** route local calls through a `Dispatch` — `Caller::Local`'s doc (`middleware.rs:391`-`394`) argues correctly that there is nothing to authorize in-process, and building a fake one is the failure that variant exists to prevent.

### 2. `TurnLost` collapses into the same shared payload — take it in this goal, not the next one

Covered as a Major above; repeating it here because it is the largest **closable** difference the goal misses rather than mis-states. Three enums naming seven-to-eight identical facts is the defect the goal exists to fix, and it deletes the fourth spelling while leaving three. `TurnLost { Faulted(Panic), Reentrant, Turn(Withheld) }` mirrors `Undelivered { Reentrant, Turn(Withheld) }`, keeps the one payload that is genuine physics (a `Panic` you can resume), and converts the hand-maintained twin guard into a type-checked one. **Cost:** it is the same edit as item 1, on one more enum — cheaper now than as a follow-up, because it shares the wire move.

### 3. `Undelivered::Gone` / `NotQueued::Closed` / `TurnLost::Gone` — one sentence, three enums (seam N3)

`turns.rs:3366`-`3372` says these carry one sentence — *the actor is no longer running* — and `turns.rs:3373`-`3374` records that a test holds **two** of the three together and the third is kept **by hand**. A hand-kept correspondence is exactly what item 1 abolishes elsewhere. **Cost:** near zero once `Withheld` exists — `NotQueued::Closed` becomes `NotQueued::Turn(Withheld::Gone)` or the guard is widened to all three. Do the guard at minimum.

### 4. The restart reason, upgraded rather than levelled (seam §7.2)

`.tmp/surf/REPORT.md:123`-`128` puts this first on its own "close now" list and calls it *the local caller is currently worse-informed than a peer about its own actor*. The goal closes it by giving both sides a `String`. Closing it with `EndReason` (Major finding above) is the same edit and keeps six-way discrimination on both sides. **Cost:** one type substitution, and it is already hashed into the `Inspect` contract, so the wire move is already being paid.

### 5. `ServableAsActor::CHECK` at the fourth door, and `SpawnableUnfed` (REPORT §7.2)

In scope as item 12 ("adjacent, cheap, already measured") — flagging only that it is currently bundled with four unrelated things behind one bullet. It is a **soundness hole**, not an ergonomics item: `.tmp/surf/REPORT.md:130`-`134` records that a service declaring a `send` method is refused at compile time as an actor and **accepted** as a handler, because `svc::service(handler, allow)` never instantiates the check. That deserves its own line and its own success criterion — a compile-fail test at all four doors — not a share of one bullet with `ask_catching` and a naming question.

### 6. Departure and stopping vocabulary (seam N6, N7, N8)

Four words for departure — `Undelivered::Gone` (actor ended), `RemoteError::NoSuchActor` (id not mounted), `Departure` (connection ended), `Ended` (why the actor stopped) — and three verbs for stopping split by side: `close` (connection), `stop` (actor, graceful), `abort` (actor, destructive), plus `System::shutdown`/`System::abort`. Plus N7: `Handle::ended()` asks *has my actor ended*, `Client::watch()` asks *has my peer left*, and a reader meeting both on one page cannot tell them apart from the names.

**Why it should close:** the goal is about a caller writing one function against both sides. Item 8 is about to add `shutdown`/`abort` to the wire, which puts `Client::close`, `Client::watch`, peer `shutdown` and peer `abort` on **one type**, where today they are spread across two and the ambiguity is survivable. **Cost:** rustdoc plus at most one rename. If item 8 ships, this is not optional — it is part of item 8.

### 7. `deliver_to_local` is the served path (seam N5)

`turns.rs:4680` answers **a peer's** request; its twin `deliver_to_actor` (`turns.rs:4651`) is the local one. The name says the opposite of what the function does, in the file this goal edits most, and the goal's own §7 evidence flags it. **Cost:** a private rename, one commit, no wire effect, no public API. Take it — it is the cheapest correctness-of-reading fix in the audit and it is not in the goal.

### 8. Items I checked and would leave

- **`with_state`, `mount`** — physics, correctly out of scope, correctly required to be documented.
- **`send`/`recv` locally** — `.tmp/surf/seam.md:405` calls them the cleanest ESSENTIAL pair, on a mechanism argument (a `BodyReader` holds a QUIC `RecvStream` by value). The goal reopens them; I would not, in this goal. See the Critical.
- **`R3`/`R4` idempotency locally** — `macros.rs:2640`: *"The key is a transport concern rather than a domain one."* That argument still holds; folding `IdempotentClient` into `Client` (item 6) does not disturb it.
- **`L11` mailbox depth remotely** — RFC-04 §6.2 (`docs/rfc-04-actors.md:826`) fixes capacity 1 and argues the transport already bounds it. Holds.
- **`R9` per-method `Middleware` locally** — holds as *served versus not served*, which is item 1 above, not a capability gap.
- **`RFC-07 §9.8` supervision** — correctly out of scope and correctly untouched.

### 9. The one DELIBERATE classification whose argument no longer holds — and the goal is right to reopen it

**`R16`, the keyed `tell`** (item 4). `macros.rs:3705`-`3716` refuses it because *"four refusals are decided AFTER the bytes reach the wire and are reported on the reply"*. The goal closes it by returning the routing acknowledgement, which answers the refusal directly rather than overriding it — the four refusals become four things the acknowledgement can say. That is the correct move. **What is missing is the append:** that comment is a written refusal and it must gain a paragraph recording why it no longer applies, exactly as item 13 does for RFC-04 §5.5. Add it to the success criteria, or the strongest documented refusal in the crate is deleted with no record. The same obligation attaches to `macros.rs:2311` and `2326` (the `stop`/`abort` local-only refusals) if item 8 ships.

---

## Summary

The thesis is right and the evidence behind it is unusually good — the audit in `.tmp/surf/` is better than most designs it could support. The charter fails on execution readiness, not on direction: eight statements in it are refuted by the tree (`Withheld` is taken, `Failure` does not exist, the gate is 18 steps, `6 − 2 ≠ 3`, `#[non_exhaustive]` is the crate's rule and not its violation), and three more are decisions deferred to an executor that is explicitly forbidden to ask (`__status` or `status`, `BlobId` or `BlobHandle`, "rule on `Expired`").

The single most important change is not on the fix list: **the goal is organised around local-versus-remote, and the crate's real boundary is served-versus-not-served.** A generic `settle(&impl Orders)` that silently skips authorization, middleware and the idempotency ledger when handed a `Handle` is a security-shaped consequence of this goal's central feature, and it appears nowhere in the document. Fix the eight refuted statements, split the charter at the two seams named above, and add the served-versus-not-served axis to the trait's design — then this is a strong goal.
