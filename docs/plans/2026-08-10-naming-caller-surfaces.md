# Keylink caller-surface naming implementation plan

**Goal:** Execute `docs/goals/2026-08-10-naming.md` against the post-PR-#47 tree without compatibility aliases or capability drift.

**Scope owner:** One implementer owns `keylink/src/macros.rs`, the generated interface, and all coupled callers. Read-only inventories and required reviews may run independently.

## Constraints

- Work only in the existing worktrunk checkout `/Users/pedrocunha/repos/keylink.naming-caller-surfaces` on `naming-caller-surfaces`. Verify the worktree, branch, clean baseline, hooks, and ancestry before editing. Do not create a second worktree and never use raw `git worktree`.
- Preserve `LocalRef` and `at_local`, `RegistryClient::at`, and `Registry::ended` with their current lifecycle and error behavior.
- Preserve every generated `rpc`/`tell`/`send`/`recv`/`blob`, blocking, idempotency, governance, and transport capability. `Client::connect` stays inherent.
- Keep `Handle`, `Client`, `IdempotentClient`, `LocalRef`, and `ActorRef`. Delete legacy paths instead of adding aliases, deprecated wrappers, or duplicate forwarders.
- Change only the goal blast radius. Add no dependency or speculative seam.
- Repair each source-literal anchor, record, and replay row in the same atomic change. Run `mise run anchors` after formatting for every literal-moving commit.
- During iteration, run narrow checks only. Let commit and push hooks own full gates. Capture long commands in `/tmp` with an `EXIT=` terminator.

## Task 1: Record and gate this plan

1. Verify the existing worktrunk checkout, branch, baseline SHA, hook installation, and clean tracked tree. Confirm that it descends from the intended `main` baseline.
2. Add the goal, reviewed proposal, and this plan to the branch.
3. Run one independent adversarial plan review with `openai-codex/gpt-5.6-luna`, `max` reasoning, `brooks-review`, `requesting-code-review`, and `brooks-audit` for the extension-trait seams.
4. Apply review feedback under `receiving-code-review`. Record the initial severity counts and dispositions. Re-run only the Critical/structural corrections and their blast radius.
5. Commit the planning documents as `docs: plan caller-surface naming change`.

## Task 2: Free the public `Registry` name

1. In `keylink/src/actor/registry.rs` only, rename private `Registry<M>` to `Shared<M>` with an exact scripted substitution.
2. Inspect the complete diff. Verify that no public symbol changed.
3. Repair affected anchors in this commit, format, run `mise run anchors`, and run narrow registry library tests.
4. Commit as `refactor(actor): free the Registry name`.

## Task 3: Establish isolated RED guards and inventories

Keep each RED independently attributable. Do not combine collision, trait, or builder failures in one fixture. RED edits remain uncommitted scratch changes until each expected first failure is recorded; restore unrelated RED edits before observing the next target.

1. Inventory every `compile_fail` fence and classify it as name-only, semantic trait membership, illegal state, or wrong program. Record per-file name-only deletion counts before moving any method. Name-only fences leave in the same commit as the names they pin.
2. Add a collision-only `service!` fixture containing exactly these 18 reclaimed RPC words: `Service`, `Serve`, `WithState`, `Stop`, `Abort`, `IsRunning`, `IsBusy`, `Buffered`, `Ended`, `Id`, `WithIdempotency`, `KeyState`, `Watch`, `Close`, `Status`, `Reserve`, `TryReserve`, and `ReserveOwned`. Exclude `Connect`, which stays reserved. First observe the current generated-name collision. After the trait move, this same fixture becomes the GREEN regression and calls both the user's inherent RPC and the shadowed keylink capability through UFCS.
3. Add a builder-only fixture. Observe missing `setup`/`restarting`, then separately prove state setup with and without feeds, observers, backpressure, both terminals, restart, give-up, queued messages, observer refusal, feed end, and drop/ending order.
4. Add a trait-only fixture with no colliding RPC names. Observe the missing six traits. Positive assertions and semantic negative `compile_fail` witnesses must cover every row in the exact matrix below under default and `--no-default-features` configurations as applicable.

| Trait | Positive implementors only | Required shape |
|---|---|---|
| `Wire` | async `Client`, `RegistryClient`, `IdempotentClient` | existing async `key_state`, `watch`, `close` bodies and outputs |
| `BlockingWire` | `blocking::Client`, `blocking::IdempotentClient` | existing synchronous `key_state`, `close` bodies and outputs; blocking feature only |
| `WithIdempotency` | async `Client` → `IdempotentClient`, `RegistryClient` → itself, blocking `Client` → blocking `IdempotentClient` | associated `Output`; no impl on either idempotent client, `Handle`, `LocalRef`, or `ActorRef` |
| `Status` | async `Client`, `IdempotentClient`, `ActorRef` | existing async status result and errors; no `RegistryClient`, blocking, local-ref, or handle impl |
| `Own` | `Handle<A>` | `type State = A`; `type Permit<'a> = Permit<'a, A> where Self: 'a`; `type OwnedPermit = OwnedPermit<A>`; exact contracts below |
| `Servable` | `Handle<A>`, `Registry<M>` where its mail implements the generated dispatch seam | associated request/response types; existing service builder and serve results |

5. Prove `keylink::prelude::*` supplies ordinary method syntax. For a service declaring a colliding RPC, document and test that the user's inherent method wins and the keylink operation remains available through `Wire::...`, `Own::...`, `Status::...`, or `Servable::...` UFCS.
6. Confirm every RED is the expected compiler error or behavior assertion, never a syntax, feature, earlier collision, or unrelated doctest failure.

## Task 4: Rename the registry family and delivery helpers

1. Apply exact substitutions and inspect each diff immediately: `System`→`Registry`, `SystemBuilder`→`RegistryBuilder`, `Mounted`→`Mountable`, `KeyedClient`→`RegistryClient`, `keyed_alpn`→`registry_alpn`, `deliver_to_singleton`→`deliver_unkeyed`, `deliver_to_actor`→`deliver_keyed`, `serving_of_singleton`→`serving_unkeyed`, and `serving_of_actor`→`serving_keyed`.
2. Manually repair rustdoc links, diagnostics, generated aliases, `keylink::inspect` exports, and semantic prose. Keep `RegistryClient::at`, `LocalRef`, `at_local`, and `Registry::ended` unchanged except receiver spelling.
3. Rename `Registry::shutdown(&id, budget)` to `Registry::stop(&id, budget)` without changing budget, return, teardown, or ending semantics.
4. Re-register every moved anchor after each logical literal group. Format and run `mise run anchors` before committing.
5. Run focused registry, interface, actor, protocol-property, local-ref, and keyed-ending tests.
6. Commit as `refactor(api): rename registry caller surfaces`.

## Task 5: Replace generated free serving doors with `Servable`

Use a legal crate-owned seam; do not emit an orphan impl for the generated registry alias.

1. Add associated `Request` and `Response` types to root `Servable`. Its `service` method returns `ServiceBuilder<Self::Request, Self::Response>` and `serve` uses that one builder path.
2. Add one hidden root registry-dispatch trait implemented by each generated local `Mail` type. That implementation owns the existing addressed dispatch closure, status interception, allowlist, blob declaration, middleware, and diagnostics.
3. Add one root blanket `Servable` implementation for `Registry<M>` where `M` implements the registry-dispatch trait. It delegates to the generated `Mail` implementation; it does not duplicate dispatch and is legal because both `Servable` and `Registry` are crate-owned.
4. Implement `Servable` directly for generated local `Handle<A>` and move the existing singleton dispatch body into it once. Qualify internal calls as `<Self as Servable>::service(...)` so a user RPC cannot shadow the trait inside the implementation.
5. Delete `keyed_service` and `keyed_serve`. Convert callers to `registry.service(allow)` and `registry.serve(&node, allow)` with the prelude in scope.
6. Run consumer-crate method-resolution checks plus focused interface, actor, blob, middleware, and inspect tests. Repair anchors, format, and run anchors.
7. Commit as `refactor(api): serve registries through Servable`.

## Task 6: Collapse nine actor doors to four

Keep setup statically typed. Do not box the factory future and do not reuse one incarnation's observers or feeds after restart.

1. Generate `Setup<A, Mode, F = ()>` with public `#[doc(hidden)]` mode types whose fields remain private, so every reachable public return type passes `private_interfaces` and downstream method resolution. A state mode owns state, observer vector, feed state, and backpressure. `.feeds(Feeds)` changes the feed type; the `F = ()` terminals retain the existing `SpawnableUnfed` compile-time refusal for services that declare feeds.
2. Generate a restarting mode parameterized by the concrete factory, future, and feed types. The factory continues to return `Incarnation<A, F>`, so every incarnation gets fresh state, observers, and feeds after the prior values drop. Restarting setup exposes only options that preserve that rule; it always exposes `.backpressure()`.
3. Add `setup(state)` and `restarting(factory, limit)` with explicit `#[must_use]` reasons. Add `#[must_use]` to `Setup` itself and every applicable self-returning `observe`, `feeds`, and `backpressure` method.
4. Add `.actor()` and `.mountable()` terminals. They delegate to the current `LocalActor`, `mount`, `run_keyed`, and supervision implementations; they do not copy loop or teardown behavior.
5. Extend `Mountable` to carry an optional selected mailbox policy into the registry entry. An unconfigured `spawn(state)` or other existing `Mountable` continues to use `RegistryBuilder::mailbox_depth`. An opted-in setup policy reaches the `Route` used by both local keyed delivery and `deliver_keyed`. Preserve current `Park(depth)` and all default paths. Define and test explicit `Shed(depth)` behavior rather than ignoring it: `try_send` refuses immediately, remote delivery uses the existing `RemoteError::MailboxFull`, and local keyed delivery gains the corresponding named full-mailbox error only for this new opted-in path. Never translate Full into Closed/AtCapacity or turn default Park into shedding.
6. Delete `actor_with`, `actor_wired`, `actor_deep`, `spawn_wired`, `spawn_supervised`, `supervised`, and `supervised_deep`. Keep `actor` and `spawn` unchanged.
7. Convert callers semantically. Run the isolated builder tests and focused actor, feed, supervision, compose, persistence, local-keyed, and keyed-ending tests to GREEN.
8. Repair anchors, format, run anchors, and commit as `refactor(api): collapse actor setup doors`.

## Task 7: Move keylink verbs onto six extension traits

1. Add root `Wire`, `BlockingWire`, `WithIdempotency`, `Status`, `Own`, and `Servable` with static dispatch and return-position futures. `Own` declares `type State`, `type Permit<'a> where Self: 'a`, and `type OwnedPermit`. Its contracts preserve: `with_state<T, F>(&self, F) -> impl Future<Output = Result<T, Unanswered>> + Send` with the current `T`/`F` bounds; `stop(&self) -> impl Future<Output = Result<(), NotQueued>> + Send`; synchronous `abort`, `is_running`, `status`, and `try_reserve`; `ended(&self) -> impl Future<Output = Ended> + Send + 'static`; `reserve(&self) -> impl Future<Output = Result<Self::Permit<'_>, NotQueued>> + Send`; and consuming `reserve_owned(self) -> impl Future<Output = Result<Self::OwnedPermit, NotQueued>> + Send`. `try_reserve` returns `Result<Option<Self::Permit<'_>>, NotQueued>`. Add compile-time associated-type and lifetime assertions. Add `keylink::prelude` with exactly those six public re-exports.
2. Move each existing body to only the Task 3 matrix surfaces. Keep one implementation per capability. Do not add a method to make a trait fit and do not box futures.
3. Move status/reservation methods to ordinary trait names. Qualify generated internal calls as `<Handle<A> as Own>::...` so user RPCs cannot shadow them. Update generated `compose::Reserving` to delegate through `Own` or the same non-public single-source helper while preserving its permit type and errors. Remove all `fn __status`, `fn __reserve`, `fn __try_reserve`, and `fn __reserve_owned` definitions.
4. Delete generated `Handle::is_busy` and `Handle::buffered`, `LocalRef::id`, and `ActorRef::id`. To satisfy the literal acceptance sweep without losing status data, rename remaining internal/public status vocabulary from `buffered` to `queued` and private local readings from `is_busy` to `busy`; update schemas, docs, and tests semantically.
5. Remove obsolete generated conflict and `Calls` conflict arms for reclaimed words. Keep real wire-enum, feed-field, Rust-keyword, and inherent `connect` checks.
6. Delete the pre-classified name-only compile-fail fences in this same commit. Keep semantic negative witnesses for trait membership, illegal states, and unsupported programs. Print per-file deletion counts.
7. Run collision, exact matrix, compose, async, blocking, idempotency, governance, PR-#47 local-reference, and keyed-ending tests to GREEN under both feature configurations.
8. Repair anchors, format, run anchors, and commit as `refactor(api): reclaim caller method names with traits`.

## Task 8: Repair break records and documentation semantically

1. Maintain a per-hit RFC ledger: pointer-only, historical/semantic claim, or unrelated prose. Re-quote pointer-only break records; preserve historical assertions and append corrections where the argument changed.
2. Update only necessary rows in `.mise/tasks/anchors` and `.mise/tasks/replay-anchors`. Never change task bodies. If the pinned count fails, read the required value from the failure; do not compute it. Run anchors again after formatting.
3. Update `README.md`, `CONTRIBUTING.md`, generated rustdoc, diagnostics, examples, and current-capability docs. Re-audit the proposal's two stale per-surface sentences against PR #47.
4. Update the naming proposal with the shipped/not-shipped execution record.
5. Run focused doctests and `mise run docs`, capturing full output.
6. Commit as `docs: adopt caller-surface vocabulary`.

## Task 9: Verify, review, fix, and deliver

1. Run all goal sweeps and focused proof tests. Audit `git diff --name-only` against the goal allowlist and explicitly verify no dependency version, hook, forbidden crate, or mise task body changed.
2. Run one independent merge gate with `openai-codex/gpt-5.6-luna` at max reasoning using `brooks-review`, `requesting-code-review`, and `brooks-audit` for trait seams. In parallel, run an invariant-compliance review with one evidenced verdict per invariant.
3. Count all Critical/Major/Minor findings. A separate implementer applies every valuable finding under `receiving-code-review`. Targetedly re-review only Critical/structural fixes and their blast radius.
4. Re-read the goal and this plan line by line. Commit review fixes atomically. Do not manually run a full suite immediately before commit or push.
5. Push reviewed `HEAD`. Let the pre-push hook run the sole full gate and capture complete output with an `EXIT=` terminator. Never kill or bypass it outside documented exceptions.
6. Compare local `HEAD` with `git ls-remote --heads origin naming-caller-surfaces` and require exact equality.
7. Open an unmerged PR to `main`. Put the root Prime Agent session id `019fee28-fdeb-70ee-b5b8-5ed06d08fc90` in the body, then read the submitted body back and verify it.
8. Print the consolidated proof block: gate results, sweeps, collision and exact matrix guards, PR-#47 regressions, deleted compile-fail counts, review counts, invariant verdicts, exact pushed SHA proof, and PR URL.

## Initial plan-gate disposition

The independent gate returned 2 Critical, 11 Major, and 4 Minor findings: FAIL. This revision accepts the implementability, setup representation, keyed policy, literal sweep, isolated RED, compile-fail timing, anchor timing, exact matrix, compose forwarding, caller shadowing, must-use, RFC ledger, RED boundary, and blast-radius findings. It corrects the worktree finding by verifying the already-required existing worktrunk checkout instead of creating a duplicate. It rejects the session-id finding because the deliverable requires the root execution session, not the reviewer's child session. The first targeted re-review returned 1 Critical and 2 Major remaining blockers. Those corrections were applied, and the final targeted verdict was 0 Critical, 0 Major, 0 Minor: PASS. Source work begins only after that PASS.
