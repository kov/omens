# Omens Implementation Progress

Last updated: 2026-02-27

## Phase Status
- [x] Phase 1 started
- [x] Phase 1 completed
- [x] Phase 2a started
- [x] Phase 2b started
- [x] Phase 2b completed
- [x] Phase 3 started
- [x] Phase 4 started
- [x] Phase 5 started
- [x] Phase 5 completed
- [x] Phase 6 started
- [x] Phase 6 completed
- [ ] Phase 7 started
- [ ] Phase 8 started

## Current Work Log
- Implemented CLI command skeleton for `auth`, `explore`, `collect`, `report`, `config`, and `browser` command groups.
- Added `config doctor` runnable path that loads config, resolves runtime paths, bootstraps expected directory layout, and prints effective paths.
- Reworked config handling from string-only keys to a typed schema with support for string/bool/int/float/string-array values and nested sections.
- Kept derived-path precedence semantics (`runtime.root_dir` defaults with explicit path overrides).
- Added semantic validation for `browser.mode`, supported section names, positive retention/page values, and threshold bounds.
- Added tests for typed parsing, invalid type handling, and semantic validation errors.
- Refactored CLI parsing to enforce command shape, reject malformed extra args, and support targeted group help.
- Expanded `config doctor` into a real check suite with warning/error reporting for missing config, browser runtime state, system-mode binary requirements, and runtime age policy.
- Added doctor-specific tests for warning/error scenarios and runtime-age warning behavior.
- Added runtime browser manager foundation (`BrowserMode`, install-state model, platform mapping, build resolution with `PINNED_CHROMIUM_BUILD`, lock metadata parsing, and CfT URL construction).
- Wired `omens browser status` to the runtime manager and print resolved install/target details.
- Implemented bundled runtime lifecycle commands: `omens browser install`, `omens browser upgrade`, `omens browser rollback`, and `omens browser reset-profile`.
- Added lock metadata finalize semantics using `chromium.lock.tmp -> chromium.lock` and stale tmp cleanup before install.
- Added current/previous symlink switching model and rollback behavior that restores previous installed build.
- Added runtime tests for install tmp cleanup, rollback link switching, profile reset cleanup, and bundled-mode command guards.
- Added archive checksum verification to installer flow with explicit mismatch refusal and no-state-change behavior.
- Added macOS quarantine removal attempt (`xattr -cr`) after install path activation.
- Replaced simulated installer artifact flow with real archive download via Rust HTTP client (`reqwest`) and extraction via `unzip`.
- Added checksum-source lookup from sidecar `.sha256` URL when available and enforced verification on matchable installs.
- Added interrupted-install cleanup guarantees for partial build directories and temporary metadata files.
- Added Chrome-for-Testing manifest resolution by pinned revision to map runtime `build` values to real artifact URLs.
- Removed Debian package fallback for bundled runtime after launch/linkage incompatibilities; bundled installer remains Chrome-for-Testing only.
- Added browser harness abstraction and chromiumoxide-backed CDP harness implementation for auth/session flows.
- Added auth session validation and login wait loop with redirect/marker/probe checks.
- Added `omens auth bootstrap [--ephemeral]` flow with auth-required exit semantics (`20`) and ephemeral profile cleanup.
- Added bundled/system browser binary resolution in runtime manager for auth launch wiring.
- Added dedicated remote display session manager (`weston` RDP backend) and CLI commands: `omens display start|stop|status`.
- Added `omens auth bootstrap --display` mode to launch browser inside the managed remote display session.
- Added automatic local TLS certificate/key generation for Weston RDP startup so managed display sessions can run without manual key provisioning.
- Added SQLite storage core module with schema migrations for `runs`, `items`, `item_versions`, `signals`, `recipes`, and `item_key_aliases`.
- Added collect-run process lock acquisition using configured lock path with dedicated lock contention error mapping.
- Wired `collect run` to initialize storage, persist run start/end lifecycle records, and report persisted run metadata.
- Added retention planning helper that computes candidate run/version deletions from `keep_runs_days` and `keep_versions_per_item` settings.
- Added tests for migration idempotency, lock contention behavior, run lifecycle transitions, and retention version-keep logic.
- Reworked explore mode from manual page-by-page capture to fully automated fund page tab crawling.
- `explore start <url-or-ticker>` navigates to a fund page (ticker auto-expanded to `clubefii.com.br/fiis/{TICKER}`), discovers all hash-anchor tabs via JS, and crawls each one automatically.
- Added CDP network-idle detection to `click_and_wait`: tracks in-flight requests via `EventRequestWillBeSent`/`EventLoadingFinished`/`EventLoadingFailed` event streams; waits until 0 in-flight for 500ms or 10s timeout.
- Added `TabSummary` structural capture: HTML table scan (selector hint, row/col counts, headers), repeating div-group detection (finds containers with ≥3 children sharing a class — catches court cases, contracts, photos, comments, news cards), and link pattern extraction with `{id}`/`{ticker}` placeholders.
- Saves HTML fixture per tab to `~/.omens/fixtures/<section>/` and a recipe with `selector_json` encoding the full structural summary.
- Confidence scoring: 1.0 when data tables (≥3 rows) or repeating groups found; 0.75 otherwise.
- Changed default browser mode from `bundled` to `system` with auto-detection of well-known binary paths.
- Added Wayland browser args (`--ozone-platform=wayland`, `--force-device-scale-factor=1`) and disabled chromiumoxide's default 800×600 viewport emulation (`viewport(None)`) for headed RDP sessions.
- Crawled BRCR11 (25 tabs) and RBRX11 (24 tabs) during development; all tabs successfully captured with full structural summaries.

- Implemented Phase 6 collection pipeline: `collect run --tickers BRCR11 [--sections proventos,comunicados]`
- `collect run` navigates to each ticker's fund page, discovers active recipes, clicks each tab, and extracts data using recipe-guided selectors.
- Tabular extraction: `extract_table_rows(selector_hint)` — finds table by CSS selector, returns all `tbody tr` rows as `Vec<Vec<String>>`.
- Repeating-group extraction: `extract_repeating_group_rows(container, child_sel, field_ids)` — finds container, iterates children, extracts text by `id` attribute match.
- `Store::upsert_item` — insert or update items with stable dedup key; returns `(item_id, is_new)`.
- `Store::insert_item_version_on_change` — `INSERT OR IGNORE` by `(item_id, content_hash)` UNIQUE constraint; returns true if new.
- `Store::apply_retention` — executes the retention plan computed by `build_retention_plan`.
- `content_hash_fnv` — deterministic FNV-1a 64-bit hash for content fingerprinting, no external dep.
- `collector.tickers` added to config (string array); `--tickers csv` CLI flag overrides it.
- `collector.sections` validation relaxed to accept any section name (not just "news"/"material-facts").
- `BrowserHarness` trait cleaned up: removed obsolete `capture_page_fingerprint` (and `PageFingerprint`, `CandidateSelector`, `SelectorKind`); removed `url`/`title` from `TabSummary`.
- Selector drift detected at extraction time: recipe is marked `Degraded` and collection continues for other sections.
- 57 tests passing.

- Improved collection pipeline robustness (post-Phase 6 session work on RBOP11):
- `capture_tab_summary` table selector now walks ancestor DOM for nearest element with an `id` when the table itself has none; produces stable `#container table` / `#container .class` selectors instead of fragile `table:nth-of-type(N)`. This fixed `cotacoes` (was 0 rows, now 64 rows from `#tabela_rentabilidade table`).
- `do_collect` table selection: prefers first table with headers AND ≥3 rows over first table with ≥3 rows only; skips single-row noise elements like `#tab_colaboradores`; ensures dividend history tables (e.g. `#tabela_proventos .thin` with column labels) are selected over unlabelled summary blocks.
- Added `BrowserHarness::dismiss_overlays()` — hides `#modal_masterpage` and `#mask` (site-wide ad popup) via JS after page load, before tab navigation. Called in both `do_collect` and `explore_start`.
- Blank header fields (e.g. the "report error" form column in `#tabela_proventos .thin`) now filtered out when building the field map; blank overflow cells also skipped.
- Verified live extraction against RBOP11: `comunicados` (300 rows, `#tabela_documentos_tb`), `cotacoes` (64 rows), `informacoes_basicas` (9–10 rows), `proventos` (13 rows with full dividend history: MÊS REF., DATA BASE, VALOR, VARIAÇÃO, DATA PAGAMENTO, COTAÇÃO DAT. BASE, YIELD DAT. BASE, TIPO).
- RBOP11 dividend analysis: 10+ consecutive months of NÃO DISTRIBUIÇÃO (Feb–Dec 2025), resumed Jan 2026 (R$ 0,500), increased Feb 2026 (R$ 0,550, +10%).

## Next Items
- Phase 7: Deterministic rules for scoring material facts, court case severity, dividend changes; LM Studio summarization.
- Phase 8: Terminal prioritized alert output; JSON/Markdown report artifacts.
- Filter site-wide noise from repeating groups during extraction (`.byFundo > DIV.modal`, `.menu-principal-scroll > LI.has-sub`, `#campos_data_hora` appear on every tab) — can be done as a pre-filter in `extract_repeating_group_rows` or as a post-processing step.
- `omens explore review` — enhance to show recipe selector preview alongside confidence.
- **Primary key stability**: first cell is not unique for categorical columns (e.g. comunicados "Categoria"). Use row-index suffix or composite key to avoid in-run stable-key collisions.
