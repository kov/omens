# Omens Implementation Progress

Last updated: 2026-02-25

## Phase Status
- [x] Phase 1 started
- [x] Phase 1 completed
- [x] Phase 2a started
- [x] Phase 2b started
- [x] Phase 2b completed
- [x] Phase 3 started
- [ ] Phase 4 started
- [ ] Phase 5 started
- [ ] Phase 6 started
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
- Added dedicated remote display session manager (`weston` + `wayvnc`) and CLI commands: `omens display start|stop|status`.
- Added `omens auth bootstrap --display` mode to launch browser inside the managed remote display session.

## Next Items
- Add collect-path preflight auth validation and `EX_AUTH_REQUIRED` handling.
