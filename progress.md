# Omens Implementation Progress

Last updated: 2026-02-25

## Phase Status
- [x] Phase 1 started
- [x] Phase 1 completed
- [x] Phase 2a started
- [ ] Phase 2b started
- [ ] Phase 3 started
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

## Next Items
- Continue Phase 2a with install-state metadata workflows and compile-time pin policy refinement.
