# Phased Implementation Plan (Context-Window Sized)

## Summary
Implement Omens in small vertical slices where each phase fits in 1-2 coding context windows and ends with `code + tests + at least one runnable CLI path`.
Sequence prioritizes early reliability risks first: runtime isolation, auth/session, extraction durability, then analysis/reporting.

## Phase Sizing Contract
1. Each phase targets 3-6 source files changed and roughly 200-600 LOC net.
2. Each phase has one explicit CLI demo path.
3. Each phase has unit/integration tests added in the same phase.
4. No phase starts until previous phase acceptance checks pass.
5. If a phase exceeds the size contract, split it into `a/b` subphases before coding.

## Public Interfaces and Types to Introduce (in order)
1. `src/cli.rs`: command tree for `auth`, `explore`, `collect`, `report`, `config`, `browser`.
2. `src/config.rs`: `OmensConfig`, derived-path resolution from `runtime.root_dir`, override semantics.
3. `src/runtime/browser_manager.rs`: `BrowserMode`, `BrowserInstallState`, `BrowserManager` APIs (`status/install/upgrade/rollback/reset_profile`).
4. `src/browser/harness.rs`: `BrowserHarness` trait and `ChromiumoxideHarness` implementation.
5. `src/auth.rs`: `validate_session()` + `wait_for_login()` flow.
6. `src/store/mod.rs`: SQLite repository traits and migrations for `runs/items/item_versions/signals/recipes/item_key_aliases`.
7. `src/explore/*.rs`: recipe capture/review/promote structures with statuses.
8. `src/collector/*.rs`: section collectors and normalization contracts.
9. `src/analysis/*.rs`: rules engine + LM Studio client contract.
10. `src/report/*.rs`: terminal + JSON/Markdown rendering and ordering.

## Phase Breakdown

### Phase 1: CLI + Config + Root Layout
1. Implement CLI skeleton with no-op handlers for all planned commands.
2. Implement config loading from `~/.omens/config/omens.toml`.
3. Implement derived defaults from `runtime.root_dir` for profile/db/reports/lock paths.
4. Implement directory bootstrap helper for required `~/.omens/*` paths.
5. Acceptance: `omens config doctor` runs and prints resolved paths.
6. Tests: config parsing, derived path precedence, invalid config error cases.

### Phase 2a: Browser Runtime Manager Foundation
1. Implement `BrowserManager` core structures and `omens browser status`.
2. Add compile-time `PINNED_CHROMIUM_BUILD` resolution when `browser.bundled_build=0`.
3. Implement Chrome for Testing URL resolution for `linux64`, `mac-arm64`, `mac-x64`.
4. Implement install-state metadata model (`chromium.lock`, `chromium.lock.tmp`) and platform detection.
5. Acceptance: `omens browser status` correctly reports not-installed state and target build.
6. Tests: URL builder, build resolution, platform mapping, metadata parsing.

### Phase 2b: Browser Runtime Install/Upgrade/Rollback
1. Implement `omens browser install/upgrade/rollback/reset-profile`.
2. Add download + streaming checksum verification + atomic `chromium.lock.tmp -> chromium.lock` finalize.
3. Add `current` symlink switching and one-version rollback behavior.
4. Add macOS quarantine handling in installer path.
5. Acceptance: `omens browser install` then `omens browser status` shows active build.
6. Tests: checksum mismatch refusal, interrupted install cleanup, rollback symlink behavior.

### Phase 3: Browser Launch + Auth Bootstrap
1. Implement browser launch wiring with explicit binary and explicit Omens profile path.
2. Add `--ephemeral` profile mode for browser-launching commands.
3. Implement `omens auth bootstrap` headed flow.
4. Implement session validation contract: redirect check + marker check + optional protected probe.
5. Pin `chromiumoxide` crate version to the validated `PINNED_CHROMIUM_BUILD` compatibility pair.
6. Add auth-specific exit code behavior (`20` for auth required).
7. Add remote display session management (`omens display start|stop|status`) using dedicated Weston RDP backend instance with safe bind defaults.
8. Acceptance: manual login bootstrap succeeds and validation passes (manual-interactive gate), including SSH-host workflows with optional remote display session.
9. Tests: session validation logic with mocked browser/probe outcomes, ephemeral profile cleanup on success/failure, display session state management.

### Phase 4: Storage Core + Run Lock + Baseline Run Tracking
1. Add SQLite migrations and repositories for `runs/items/item_versions/signals/recipes/item_key_aliases`.
2. Implement process lock via configured lock path and mapped exit code (`30`).
3. Implement run lifecycle recording (`started/ended/status`).
4. Implement full retention logic (`keep_runs_days`, `keep_versions_per_item`) with tests.
5. Defer wiring retention execution into the full collection loop until Phase 6.
6. Acceptance: `collect run` can start/end a run and persist metadata even without extraction.
7. Tests: migration idempotency, lock contention behavior, run status transitions, retention rule correctness.

### Phase 5: Explore Mode + Recipe Lifecycle + Fixtures
1. Implement `explore start/review/promote`.
2. Persist recipe entities with `pending_review/active/degraded/retired`.
3. Add fixture capture output to `~/.omens/fixtures`.
4. Add failure bundle writer to `~/.omens/failure_bundles`.
5. Commit fixture corpus for both sections (`news`, `material-facts`) under repo fixtures path for CI.
6. Acceptance: exploration session captures at least one recipe and `promote` sets it active.
7. Tests: recipe status transitions, fixture file generation, failure bundle schema validation.

### Phase 6: Collector V1 for News + Material Facts
1. Implement collector flow with active recipes, pagination policy, and detail-open policy.
2. Implement normalization and canonical key dedup with alias mapping.
3. Implement `content_hash` change detection and conditional `item_versions` insert.
4. Persist new/changed items and basic signals from rules-only scoring.
5. Acceptance: `collect run --sections news,material-facts` ingests fixture-backed data end-to-end.
6. Tests: dedup alias reconciliation, no-change idempotency, max-pages enforcement, degraded recipe isolation.
7. Implementation note: treat this as two passes within the phase if needed: `(a) normalization/dedup/storage` then `(b) collector wiring`.

### Phase 7: Reporting + Doctor + Exit Semantics
1. Implement `report latest` terminal + `latest.json` + `latest.md`.
2. Enforce ordering by severity, confidence, published date.
3. Finalize exit code mapping `0/10/20/30/40`.
4. Expand `config doctor` to runtime age warning and dependency checks.
5. Acceptance: run output is script-friendly and report artifacts are deterministic.
6. Tests: report ordering, partial success code `10`, doctor warning paths.

### Phase 8: LM Studio Integration (Local-Only)
1. Implement LM Studio OpenAI-compatible client and strict response parser.
2. Add truncation with `analysis.lmstudio.max_input_chars`.
3. Implement fallback to rules-only on timeout/unavailable/malformed output.
4. Merge rules + model into final decision policy.
5. Acceptance: same collect pipeline works with LM Studio enabled and disabled.
6. Tests: malformed JSON handling, timeout fallback, confidence threshold behavior.

## Test Matrix (Cross-Phase)
1. Unit: config derivation, URL canonicalization, stable key generation, decision policy.
2. Integration: runtime install, auth bootstrap/expiry, collect run idempotency, lock contention.
3. Fixture-based extraction tests: run without browser against frozen HTML/JSON.
4. Manual acceptance: first login bootstrap, daily rerun with no duplicates, critical signal surfaced.

## Rollout and Verification
1. Roll out by phase completion only; do not parallelize unfinished dependencies.
2. After each phase, run full test suite plus one manual CLI smoke command.
3. Keep a changelog entry per phase with command examples and known limitations.

## Assumptions and Defaults
1. Implementation language stays all-Rust.
2. Default runtime mode remains bundled Chromium under `~/.omens`.
3. Each phase is constrained to 1-2 coding windows as requested.
4. Completion bar for every phase is `code + tests + runnable CLI`.
5. External-agent escalation remains out of scope for v1.
