use crate::analyze;
use crate::auth::{self, AuthError, AuthValidationConfig, EphemeralProfile};
use crate::browser::harness::{BrowserHarness, ChromiumoxideHarness, TabSummary};
use crate::config::{self, DoctorIssueSeverity, OmensConfig};
use crate::explore::fixtures::FixtureWriter;
use crate::runtime::browser_manager::{BrowserInstallState, BrowserManager, BrowserMode};
use crate::runtime::display_manager::DisplayManager;
use crate::store::{self, LockError, RecipeStatus, RunStatus, SignalWithItem, Store};
use serde::Deserialize;
use std::collections::HashMap;
use std::io;
use std::time::{Duration, SystemTime};

use super::CliError;

// ── Recipe selector JSON deserialization ────────────────────────────────────

#[derive(Deserialize, Default)]
struct RecipeSelectorJson {
    #[serde(default)]
    tables: Vec<RecipeTableInfo>,
    #[serde(default)]
    repeating_groups: Vec<RecipeGroupInfo>,
}

#[derive(Deserialize, Default)]
struct RecipeTableInfo {
    hint: String,
    #[serde(default)]
    rows: usize,
    #[serde(default)]
    headers: Vec<String>,
}

#[derive(Deserialize, Default)]
struct RecipeGroupInfo {
    container: String,
    child: String,
    #[serde(default)]
    fields: Vec<String>,
}

struct CollectStats {
    items_seen: usize,
    items_new: usize,
    items_changed: usize,
}

// ── Command implementations ──────────────────────────────────────────────────

pub fn auth_bootstrap(ephemeral: bool, display: bool) -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    config::bootstrap_layout(&loaded).map_err(CliError::fatal)?;

    let manager = BrowserManager::from_config(&loaded).map_err(CliError::fatal)?;
    let browser_binary = manager.browser_binary_path().map_err(CliError::fatal)?;

    let ephemeral_root = loaded.resolved.root_dir.join("browser/profiles/ephemeral");
    let profile_path;
    let ephemeral_profile;

    if ephemeral {
        let profile = EphemeralProfile::create(&ephemeral_root).map_err(map_auth_error)?;
        profile_path = profile.path().to_path_buf();
        ephemeral_profile = Some(profile);
    } else {
        profile_path = manager.default_profile_dir().to_path_buf();
        std::fs::create_dir_all(&profile_path).map_err(|err| {
            CliError::fatal(format!(
                "failed to create browser profile {}: {err}",
                profile_path.display()
            ))
        })?;
        ephemeral_profile = None;
    }

    let mut launch_env = Vec::<(String, String)>::new();
    if display {
        let manager = DisplayManager::new(&loaded.resolved.root_dir);
        let status = manager.status().map_err(CliError::fatal)?;
        let session = status.session.ok_or_else(|| {
            CliError::fatal("display session is not running; run `omens display start`")
        })?;
        launch_env.push((
            "XDG_RUNTIME_DIR".to_string(),
            session.runtime_dir.display().to_string(),
        ));
        launch_env.push(("WAYLAND_DISPLAY".to_string(), session.wayland_socket));
    }

    let mut harness = ChromiumoxideHarness::new(browser_binary, profile_path.clone(), launch_env)
        .map_err(CliError::fatal)?;
    harness
        .launch(loaded.clubefii.login_url.as_str())
        .map_err(CliError::fatal)?;

    println!("auth bootstrap");
    println!("  opened login URL: {}", loaded.clubefii.login_url);
    println!("  profile: {}", profile_path.display());
    println!("  complete login in the browser, then press Enter here to validate session.");

    let mut line = String::new();
    io::stdin()
        .read_line(&mut line)
        .map_err(|err| CliError::fatal(format!("failed reading confirmation input: {err}")))?;

    let auth_config = AuthValidationConfig {
        base_url: loaded.clubefii.base_url.clone(),
        login_url: loaded.clubefii.login_url.clone(),
        required_marker: loaded.clubefii.auth_marker.clone(),
        protected_probe_url: loaded.clubefii.protected_probe_url.clone(),
        login_timeout: Duration::from_secs(120),
        poll_interval: Duration::from_secs(2),
    };

    let result = auth::wait_for_login(&harness, &auth_config).map_err(map_auth_error);
    let _ = harness.shutdown();
    drop(ephemeral_profile);

    result?;
    println!("auth bootstrap: session validation passed");
    Ok(())
}

pub fn explore_start(url: String) -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    config::bootstrap_layout(&loaded).map_err(CliError::fatal)?;

    let manager = BrowserManager::from_config(&loaded).map_err(CliError::fatal)?;
    let browser_binary = manager.browser_binary_path().map_err(CliError::fatal)?;
    let profile_path = manager.default_profile_dir().to_path_buf();
    std::fs::create_dir_all(&profile_path).map_err(|err| {
        CliError::fatal(format!(
            "failed to create browser profile {}: {err}",
            profile_path.display()
        ))
    })?;

    let mut launch_env = Vec::<(String, String)>::new();
    let display_mgr = DisplayManager::new(&loaded.resolved.root_dir);
    if let Ok(status) = display_mgr.status()
        && let Some(session) = status.session
    {
        launch_env.push((
            "XDG_RUNTIME_DIR".to_string(),
            session.runtime_dir.display().to_string(),
        ));
        launch_env.push(("WAYLAND_DISPLAY".to_string(), session.wayland_socket));
    }

    let mut harness = ChromiumoxideHarness::new(browser_binary, profile_path, launch_env)
        .map_err(CliError::fatal)?;

    let store = Store::open(&loaded.resolved.storage_db_path).map_err(CliError::fatal)?;
    store.migrate().map_err(CliError::fatal)?;

    let fixture_writer = FixtureWriter::new(&loaded.resolved.root_dir.join("fixtures"));

    println!("explore start: {url}");
    harness.launch(&url).map_err(CliError::fatal)?;

    // Wait for initial page load, then dismiss any blocking overlays
    std::thread::sleep(std::time::Duration::from_secs(3));
    harness.dismiss_overlays();

    println!("  discovering tabs...");
    let tabs = harness.discover_tab_anchors().map_err(CliError::fatal)?;
    let tabs: Vec<_> = tabs
        .into_iter()
        .filter(|t| {
            let a = t.anchor.as_str();
            // Skip non-content anchors
            a.starts_with('#') && a != "#" && !a.contains("modal") && !a.contains("Modal")
        })
        .collect();

    println!("  found {} tabs:\n", tabs.len());
    for (i, tab) in tabs.iter().enumerate() {
        println!("    {i:>2}. {} ({})", tab.label, tab.anchor);
    }

    println!("\n  crawling each tab...\n");

    let base_url = harness.current_url().unwrap_or_else(|_| url.clone());
    let base_url_no_hash = base_url.split('#').next().unwrap_or(&base_url);

    for (i, tab) in tabs.iter().enumerate() {
        let section_name = tab.anchor.trim_start_matches('#');
        println!("  [{}/{}] {} ...", i + 1, tabs.len(), section_name);

        // Click the tab link and wait for content to load
        let click_selector = format!("a[href='{}']", tab.anchor);
        if let Err(err) = harness.click_and_wait(&click_selector, 10_000) {
            println!("    skip: click failed: {err}");
            continue;
        }

        // Capture structural summary
        let summary = match harness.capture_tab_summary() {
            Ok(s) => s,
            Err(err) => {
                println!("    skip: capture failed: {err}");
                continue;
            }
        };

        // Save fixture
        let page_html = harness.page_source().ok();
        if let Some(html) = &page_html {
            let tab_url = format!("{base_url_no_hash}{}", tab.anchor);
            match fixture_writer.save_page(section_name, &tab_url, html) {
                Ok(path) => println!("    fixture: {}", path.display()),
                Err(err) => println!("    fixture save failed: {err}"),
            }
        }

        // Print summary
        if summary.tables.is_empty()
            && summary.link_patterns.is_empty()
            && summary.repeating_groups.is_empty()
        {
            println!("    content: no tables, link patterns, or repeating groups found");
        }

        for table in &summary.tables {
            let headers_str = if table.headers.is_empty() {
                "no headers".to_string()
            } else {
                table.headers.join(" | ")
            };
            println!(
                "    table: {} ({} rows, {} cols) [{}]",
                table.selector_hint, table.row_count, table.column_count, headers_str
            );
        }

        for rg in &summary.repeating_groups {
            let fields_str = if rg.sample_fields.is_empty() {
                String::new()
            } else {
                format!(" fields: {}", rg.sample_fields.join(", "))
            };
            println!(
                "    repeat: {} > {} (×{}){}",
                rg.container_hint, rg.child_selector, rg.count, fields_str
            );
        }

        for lp in &summary.link_patterns {
            println!(
                "    links: {} (×{}) sample: \"{}\"",
                lp.pattern, lp.count, lp.sample_text
            );
        }

        if summary.text_blocks > 0 {
            println!("    text blocks: {}", summary.text_blocks);
        }

        // Save recipe for this tab
        let selector_json = build_tab_selector_json(&summary);
        let confidence = compute_tab_confidence(&summary);
        let name = format!("{section_name}-auto");
        let now = store::now_epoch_seconds().map_err(CliError::fatal)?;

        let recipe_id = store
            .insert_recipe(
                section_name,
                &name,
                Some(confidence),
                &selector_json,
                None,
                now,
            )
            .map_err(CliError::fatal)?;

        println!("    recipe: id={recipe_id} confidence={confidence:.2}\n");
    }

    let _ = harness.shutdown();
    println!("explore start: done. {} tabs crawled.", tabs.len());
    println!("  run `omens explore review` to see all recipes");
    Ok(())
}

pub fn explore_review() -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    config::bootstrap_layout(&loaded).map_err(CliError::fatal)?;

    let store = Store::open(&loaded.resolved.storage_db_path).map_err(CliError::fatal)?;
    store.migrate().map_err(CliError::fatal)?;

    let recipes = store.list_recipes(None).map_err(CliError::fatal)?;
    if recipes.is_empty() {
        println!("explore review: no recipes found");
        println!("  run `omens explore start` to capture candidates");
        return Ok(());
    }

    println!("explore review: {} recipe(s)\n", recipes.len());
    for recipe in &recipes {
        let confidence_str = recipe
            .confidence
            .map(|c| format!("{c:.2}"))
            .unwrap_or_else(|| "-".to_string());
        println!(
            "  id={:<4} section={:<20} status={:<15} confidence={:<6} name={}",
            recipe.id,
            recipe.section,
            recipe.status.as_str(),
            confidence_str,
            recipe.name,
        );
    }
    Ok(())
}

pub fn explore_promote(recipe_id: String) -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    config::bootstrap_layout(&loaded).map_err(CliError::fatal)?;

    let store = Store::open(&loaded.resolved.storage_db_path).map_err(CliError::fatal)?;
    store.migrate().map_err(CliError::fatal)?;

    let id: i64 = recipe_id
        .parse()
        .map_err(|_| CliError::fatal(format!("invalid recipe id: {recipe_id}")))?;

    let now = store::now_epoch_seconds().map_err(CliError::fatal)?;
    let promoted = store.promote_recipe(id, now).map_err(CliError::fatal)?;

    println!("explore promote: recipe {} is now active", promoted.id);
    println!("  section: {}", promoted.section);
    println!("  name: {}", promoted.name);

    let active = store
        .get_active_recipe(&promoted.section)
        .map_err(CliError::fatal)?;
    if let Some(active) = active
        && active.id == promoted.id
    {
        println!(
            "  verified: active recipe for '{}' is id={}",
            promoted.section, active.id
        );
    }
    Ok(())
}

fn build_tab_selector_json(summary: &TabSummary) -> String {
    let tables: Vec<String> = summary
        .tables
        .iter()
        .map(|t| {
            format!(
                "{{\"hint\":\"{}\",\"rows\":{},\"cols\":{},\"headers\":{}}}",
                t.selector_hint.replace('\"', "\\\""),
                t.row_count,
                t.column_count,
                json_string_array(&t.headers.iter().map(|h| h.as_str()).collect::<Vec<_>>()),
            )
        })
        .collect();

    let links: Vec<String> = summary
        .link_patterns
        .iter()
        .map(|l| {
            format!(
                "{{\"pattern\":\"{}\",\"count\":{}}}",
                l.pattern.replace('\"', "\\\""),
                l.count,
            )
        })
        .collect();

    let groups: Vec<String> = summary
        .repeating_groups
        .iter()
        .map(|g| {
            format!(
                "{{\"container\":\"{}\",\"child\":\"{}\",\"count\":{},\"fields\":{}}}",
                g.container_hint.replace('\"', "\\\""),
                g.child_selector.replace('\"', "\\\""),
                g.count,
                json_string_array(
                    &g.sample_fields
                        .iter()
                        .map(|f| f.as_str())
                        .collect::<Vec<_>>()
                ),
            )
        })
        .collect();

    format!(
        "{{\"tables\":[{}],\"links\":[{}],\"repeating_groups\":[{}],\"text_blocks\":{}}}",
        tables.join(","),
        links.join(","),
        groups.join(","),
        summary.text_blocks,
    )
}

fn json_string_array(items: &[&str]) -> String {
    let entries: Vec<String> = items
        .iter()
        .map(|s| format!("\"{}\"", s.replace('\"', "\\\"")))
        .collect();
    format!("[{}]", entries.join(","))
}

fn compute_tab_confidence(summary: &TabSummary) -> f64 {
    let has_tables = !summary.tables.is_empty();
    let has_links = !summary.link_patterns.is_empty();
    let has_data_tables = summary.tables.iter().any(|t| t.row_count >= 3);
    let has_repeating = summary.repeating_groups.iter().any(|g| g.count >= 3);

    let mut score = 0.0;
    if has_data_tables {
        score += 0.5;
    } else if has_tables {
        score += 0.25;
    }
    if has_repeating {
        score += 0.4;
    }
    if has_links {
        score += 0.3;
    }
    if summary.text_blocks > 5 {
        score += 0.2;
    }
    if score > 1.0 {
        score = 1.0;
    }
    score
}

pub fn collect_run(sections: Option<String>, tickers: Option<String>) -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    config::bootstrap_layout(&loaded).map_err(CliError::fatal)?;

    let _lock = match store::acquire_collect_lock(&loaded.resolved.storage_lock_path) {
        Ok(lock) => lock,
        Err(LockError::Contended(message)) => return Err(CliError::lock_conflict(message)),
        Err(LockError::Runtime(message)) => return Err(CliError::fatal(message)),
    };

    // Resolve tickers: CLI flag > config > error
    let ticker_list: Vec<String> = if let Some(t) = tickers {
        t.split(',')
            .map(|s| s.trim().to_uppercase())
            .filter(|s| !s.is_empty())
            .collect()
    } else if !loaded.collector.tickers.is_empty() {
        loaded
            .collector
            .tickers
            .iter()
            .map(|s| s.to_uppercase())
            .collect()
    } else {
        return Err(CliError::fatal(
            "no tickers specified; use --tickers TICKER,... or set collector.tickers in config",
        ));
    };

    let section_filter: Option<Vec<String>> = sections.map(|s| {
        s.split(',')
            .map(|p| p.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    });

    let store = Store::open(&loaded.resolved.storage_db_path).map_err(CliError::fatal)?;
    store.migrate().map_err(CliError::fatal)?;

    // Backfill published_at for rows scraped before this feature existed.
    // One-time pass; subsequent runs find no NULL rows and skip immediately.
    let missing = store
        .items_missing_published_at()
        .map_err(CliError::fatal)?;
    if !missing.is_empty() {
        for (item_id, section, json) in &missing {
            if let Some(ts) = extract_published_at(section, json) {
                store.set_published_at(*item_id, ts).map_err(CliError::fatal)?;
            }
        }
        println!("  backfilled published_at for {} items", missing.len());
    }

    let sections_csv = format!(
        "tickers={} sections={}",
        ticker_list.join(","),
        section_filter
            .as_ref()
            .map(|v| v.join(","))
            .unwrap_or_else(|| "all".to_string())
    );
    let started = store::now_epoch_seconds().map_err(CliError::fatal)?;
    let run_id = store
        .start_run(&sections_csv, started)
        .map_err(CliError::fatal)?;

    let collect_result = do_collect(
        &loaded,
        &store,
        run_id,
        &ticker_list,
        section_filter.as_deref(),
    );

    let ended = store::now_epoch_seconds().map_err(CliError::fatal)?;
    match collect_result {
        Ok(stats) => {
            store
                .finish_run(run_id, RunStatus::Success, ended, None)
                .map_err(CliError::fatal)?;
            let retention = store
                .build_retention_plan(
                    ended,
                    loaded.storage.retention.keep_runs_days,
                    loaded.storage.retention.keep_versions_per_item,
                )
                .map_err(CliError::fatal)?;
            store.apply_retention(&retention).map_err(CliError::fatal)?;

            // Phase 7: analyze new/changed items
            let raw = store.items_for_analysis(run_id).map_err(CliError::fatal)?;
            let analysis_items: Vec<analyze::AnalysisItem> = raw
                .into_iter()
                .map(|r| analyze::AnalysisItem {
                    item_id: r.item_id,
                    section: r.section,
                    stable_key: r.stable_key,
                    payload_json: r.payload_json,
                    is_new: r.version_count == 1,
                })
                .collect();
            let signals = analyze::analyze_items(&analysis_items, &loaded.analysis);
            for sig in &signals {
                store
                    .insert_signal(
                        sig.item_id,
                        run_id,
                        &sig.kind,
                        sig.severity.as_str(),
                        sig.confidence,
                        &sig.reasons,
                        &sig.summary,
                        ended,
                    )
                    .map_err(CliError::fatal)?;
            }

            let critical_count = signals
                .iter()
                .filter(|s| matches!(s.severity, analyze::Severity::Critical))
                .count();
            let high_count = signals
                .iter()
                .filter(|s| matches!(s.severity, analyze::Severity::High))
                .count();
            let medium_count = signals
                .iter()
                .filter(|s| matches!(s.severity, analyze::Severity::Medium))
                .count();
            let low_count = signals
                .iter()
                .filter(|s| matches!(s.severity, analyze::Severity::Low))
                .count();

            let mut breakdown_parts: Vec<String> = Vec::new();
            if critical_count > 0 {
                breakdown_parts.push(format!("{critical_count} critical"));
            }
            if high_count > 0 {
                breakdown_parts.push(format!("{high_count} high"));
            }
            if medium_count > 0 {
                breakdown_parts.push(format!("{medium_count} medium"));
            }
            if low_count > 0 {
                breakdown_parts.push(format!("{low_count} low"));
            }
            let breakdown = if breakdown_parts.is_empty() {
                String::new()
            } else {
                format!(" ({})", breakdown_parts.join(", "))
            };

            println!("collect run");
            println!("  run_id: {run_id}");
            println!("  tickers: {}", ticker_list.join(", "));
            println!("  db_path: {}", loaded.resolved.storage_db_path.display());
            println!("  status: success");
            println!("  items_seen: {}", stats.items_seen);
            println!("  items_new: {}", stats.items_new);
            println!("  items_changed: {}", stats.items_changed);
            println!("  signals: {}{}", signals.len(), breakdown);
            println!(
                "  retention: runs_deleted={}, versions_deleted={}",
                retention.run_ids_to_delete.len(),
                retention.version_ids_to_delete.len()
            );

            if !signals.is_empty() {
                println!("\nSignals:");
                for sig in &signals {
                    println!(
                        "  [{:<8} {:.2}] {}",
                        sig.severity.as_str().to_uppercase(),
                        sig.confidence,
                        sig.summary
                    );
                }
            }

            Ok(())
        }
        Err(err) => {
            let _ = store.finish_run(run_id, RunStatus::Failed, ended, Some(err.as_str()));
            Err(CliError::fatal(err))
        }
    }
}

fn do_collect(
    loaded: &OmensConfig,
    store: &Store,
    run_id: i64,
    tickers: &[String],
    section_filter: Option<&[String]>,
) -> Result<CollectStats, String> {
    let manager = BrowserManager::from_config(loaded).map_err(|e| e.to_string())?;
    let browser_binary = manager.browser_binary_path().map_err(|e| e.to_string())?;
    let profile_path = manager.default_profile_dir().to_path_buf();
    std::fs::create_dir_all(&profile_path)
        .map_err(|e| format!("failed to create browser profile: {e}"))?;

    let mut launch_env = Vec::<(String, String)>::new();
    let display_mgr = DisplayManager::new(&loaded.resolved.root_dir);
    if let Ok(status) = display_mgr.status()
        && let Some(session) = status.session
    {
        launch_env.push((
            "XDG_RUNTIME_DIR".to_string(),
            session.runtime_dir.display().to_string(),
        ));
        launch_env.push(("WAYLAND_DISPLAY".to_string(), session.wayland_socket));
    }

    let mut harness = ChromiumoxideHarness::new(browser_binary, profile_path, launch_env)
        .map_err(|e| e.to_string())?;

    let base_url = &loaded.clubefii.base_url;
    let mut stats = CollectStats {
        items_seen: 0,
        items_new: 0,
        items_changed: 0,
    };
    let mut first = true;

    for ticker in tickers {
        let fund_url = format!("{base_url}/fiis/{ticker}");
        println!("collect: navigating to {fund_url}");

        if first {
            harness.launch(&fund_url).map_err(|e| e.to_string())?;
            first = false;
        } else {
            harness.navigate(&fund_url).map_err(|e| e.to_string())?;
        }
        std::thread::sleep(std::time::Duration::from_secs(3));
        harness.dismiss_overlays();

        // Load active recipes filtered by section_filter
        let all_recipes = store.list_recipes(None)?;
        let active_recipes: Vec<_> = all_recipes
            .iter()
            .filter(|r| r.status == RecipeStatus::Active)
            .filter(|r| {
                section_filter
                    .map(|f| f.iter().any(|s| s == &r.section))
                    .unwrap_or(true)
            })
            .collect();

        if active_recipes.is_empty() {
            println!(
                "  [{ticker}] no active recipes found; run `omens explore start {ticker}` \
                 then `omens explore promote <id>`"
            );
            continue;
        }

        for recipe in &active_recipes {
            let section = &recipe.section;
            println!("  [{ticker}/{section}] extracting...");

            // Click the tab anchor
            let click_sel = format!("a[href='#{section}']");
            if let Err(e) = harness.click_and_wait(&click_sel, 10_000) {
                println!("    skip: tab click failed: {e}");
                let now = store::now_epoch_seconds()?;
                let _ = store.update_recipe_status(recipe.id, RecipeStatus::Degraded, now);
                continue;
            }

            // Parse selector JSON from recipe
            let selector: RecipeSelectorJson =
                serde_json::from_str(&recipe.selector_json).unwrap_or_default();

            let now = store::now_epoch_seconds()?;
            let tab_url = format!("{base_url}/fiis/{ticker}#{section}");

            // Pick the best table to extract:
            // 1. First table with headers AND ≥3 rows (labelled, non-noise)
            // 2. First table with ≥3 rows (non-noise, no headers)
            // 3. First table (last resort)
            // This skips single-row noise elements like #tab_colaboradores and
            // prefers labelled data tables (e.g. #tabela_proventos .thin) over
            // unlabelled summary blocks (e.g. #tabela_info_basica).
            let primary_table = selector
                .tables
                .iter()
                .find(|t| t.rows >= 3 && !t.headers.is_empty())
                .or_else(|| selector.tables.iter().find(|t| t.rows >= 3))
                .or_else(|| selector.tables.first());

            if let Some(table) = primary_table {
                // Tabular extraction
                let rows = match harness.extract_table_rows(&table.hint, 10_000) {
                    Ok(r) => r,
                    Err(e) => {
                        println!("    skip: table extraction failed: {e}");
                        let _ = store.update_recipe_status(recipe.id, RecipeStatus::Degraded, now);
                        continue;
                    }
                };
                println!("    extracted {} rows from {}", rows.len(), table.hint);

                // Pre-compute a stable, unique compound primary key for every row.
                // Rows whose first cell is already unique get a 1-cell key.
                // Non-unique rows get additional cells appended (from a preferred
                // header list) until the key is batch-unique or all options are
                // exhausted.  This handles cases like:
                //   • Fato Relevante + unique Assunto  → 2-cell key
                //   • Informe Mensal + N/D Assunto + same Data Referência for
                //     V.1 & V.2 → 3-cell key (date + Data Entrega)
                const STABLE_HDRS: &[&str] = &[
                    "Assunto", "assunto",
                    "Data Referência", "Data Referencia",
                    "Data Entrega",
                    "MÊS REF.", "MES REF.",
                    "DATA COM",
                    "Status / Modalidade Envio",
                ];
                let is_placeholder =
                    |v: &str| v.is_empty() || v == "N/D" || v == "N/A" || v == "-" || v == "--";

                // Seed: every row starts with its trimmed first cell (or row index).
                let mut compound_keys: Vec<String> = rows
                    .iter()
                    .enumerate()
                    .map(|(i, cells)| {
                        let first = cells.first().map(|s| s.trim()).unwrap_or("");
                        if first.is_empty() { i.to_string() } else { first.to_string() }
                    })
                    .collect();

                // Repeatedly extend non-unique keys using the next preferred header.
                // Use owned-key counts to avoid borrowing compound_keys while mutating it.
                'outer: for hdr in STABLE_HDRS {
                    let counts: HashMap<String, usize> =
                        compound_keys.iter().fold(HashMap::new(), |mut m, k| {
                            *m.entry(k.clone()).or_insert(0) += 1;
                            m
                        });
                    if counts.values().all(|&c| c <= 1) {
                        break 'outer;
                    }
                    let Some(col_idx) = table.headers.iter().position(|h| h.as_str() == *hdr)
                    else {
                        continue;
                    };
                    for (i, cells) in rows.iter().enumerate() {
                        if counts.get(&compound_keys[i]).copied().unwrap_or(0) <= 1 {
                            continue;
                        }
                        if let Some(val) = cells.get(col_idx) {
                            let t = val.trim();
                            if !is_placeholder(t) {
                                compound_keys[i] = format!("{}|{t}", compound_keys[i]);
                            }
                        }
                    }
                }
                // Last-resort tiebreaker for any remaining collisions.
                {
                    let counts: HashMap<String, usize> =
                        compound_keys.iter().fold(HashMap::new(), |mut m, k| {
                            *m.entry(k.clone()).or_insert(0) += 1;
                            m
                        });
                    let tiebreakers: Vec<bool> = compound_keys
                        .iter()
                        .map(|k| counts.get(k).copied().unwrap_or(0) > 1)
                        .collect();
                    for (i, key) in compound_keys.iter_mut().enumerate() {
                        if tiebreakers[i] {
                            *key = format!("{key}|{i}");
                        }
                    }
                }

                for (row_idx, cells) in rows.iter().enumerate() {
                    let mut fields: HashMap<String, String> = table
                        .headers
                        .iter()
                        .enumerate()
                        .filter(|(_, h)| !h.trim().is_empty())
                        .map(|(i, h)| {
                            let val = cells.get(i).cloned().unwrap_or_default();
                            (h.clone(), val)
                        })
                        .collect();
                    // Fill unnamed columns (skip blank overflow cells)
                    for (i, cell) in cells.iter().enumerate() {
                        if i >= table.headers.len() && !cell.trim().is_empty() {
                            fields.insert(format!("col_{i}"), cell.clone());
                        }
                    }

                    let primary_key = compound_keys[row_idx].clone();
                    let external_id = format!("{ticker}/{section}/{primary_key}");
                    let stable_key = format!("external_id:{external_id}");

                    let normalized_json = build_normalized_json(&fields);
                    let hash = store::content_hash_fnv(&normalized_json);
                    let published_at = extract_published_at(section, &normalized_json);

                    let (item_id, is_new) = store.upsert_item(
                        "clubefii",
                        section,
                        Some(&tab_url),
                        Some(&external_id),
                        &stable_key,
                        Some(&primary_key),
                        None,
                        &hash,
                        &normalized_json,
                        now,
                        published_at,
                    )?;
                    let is_changed = store.insert_item_version_on_change(
                        item_id,
                        run_id,
                        &hash,
                        &normalized_json,
                        now,
                    )?;

                    stats.items_seen += 1;
                    if is_new {
                        stats.items_new += 1;
                    } else if is_changed {
                        stats.items_changed += 1;
                    }
                }
            } else if let Some(group) = selector.repeating_groups.first() {
                // Repeating-group extraction
                let field_ids: Vec<&str> = group
                    .fields
                    .iter()
                    .filter_map(|f| f.split(": ").next())
                    .collect();

                let rows = match harness.extract_repeating_group_rows(
                    &group.container,
                    &group.child,
                    &field_ids,
                    10_000,
                ) {
                    Ok(r) => r,
                    Err(e) => {
                        println!("    skip: group extraction failed: {e}");
                        let _ = store.update_recipe_status(recipe.id, RecipeStatus::Degraded, now);
                        continue;
                    }
                };
                println!(
                    "    extracted {} items from {}",
                    rows.len(),
                    group.container
                );

                // Detect non-unique first-field values for compound key building
                let first_fid_counts: HashMap<String, usize> = {
                    let first_fid = field_ids.first().copied().unwrap_or("id");
                    rows.iter().fold(HashMap::new(), |mut map, fields| {
                        if let Some(v) = fields.get(first_fid) {
                            *map.entry(v.trim().to_string()).or_insert(0) += 1;
                        }
                        map
                    })
                };

                for (row_idx, fields) in rows.iter().enumerate() {
                    let first_fid = field_ids.first().copied().unwrap_or("id");
                    let first_val = fields
                        .get(first_fid)
                        .map(|s| s.trim())
                        .unwrap_or("");
                    let primary_key = if first_val.is_empty() {
                        row_idx.to_string()
                    } else if first_fid_counts.get(first_val).copied().unwrap_or(0) <= 1 {
                        first_val.to_string()
                    } else {
                        let second_fid = field_ids.get(1).copied().unwrap_or("_");
                        let second: String = fields
                            .get(second_fid)
                            .map(|s| s.trim())
                            .filter(|s| !s.is_empty())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| row_idx.to_string());
                        format!("{first_val}|{second}")
                    };
                    let external_id = format!("{ticker}/{section}/{primary_key}");
                    let stable_key = format!("external_id:{external_id}");

                    let normalized_json = build_normalized_json(fields);
                    let hash = store::content_hash_fnv(&normalized_json);
                    let published_at = extract_published_at(section, &normalized_json);

                    let (item_id, is_new) = store.upsert_item(
                        "clubefii",
                        section,
                        Some(&tab_url),
                        Some(&external_id),
                        &stable_key,
                        Some(&primary_key),
                        None,
                        &hash,
                        &normalized_json,
                        now,
                        published_at,
                    )?;
                    let is_changed = store.insert_item_version_on_change(
                        item_id,
                        run_id,
                        &hash,
                        &normalized_json,
                        now,
                    )?;

                    stats.items_seen += 1;
                    if is_new {
                        stats.items_new += 1;
                    } else if is_changed {
                        stats.items_changed += 1;
                    }
                }
            } else {
                println!("    skip: no extractable table or repeating group in recipe");
            }
        }
    }

    harness.shutdown().ok();
    Ok(stats)
}

/// Parse Brazilian date "DD/MM/YYYY" (or "DD/MM/YYYY HH:MM:SS") → Unix epoch seconds (UTC midnight).
fn parse_date_br(s: &str) -> Option<i64> {
    // Strip optional time suffix (e.g. "27/02/2026 23:41:00" → "27/02/2026")
    let s = s.split_whitespace().next()?;
    let mut parts = s.splitn(3, '/');
    let d: i64 = parts.next()?.trim().parse().ok()?;
    let m: i64 = parts.next()?.trim().parse().ok()?;
    let y: i64 = parts.next()?.trim().parse().ok()?;
    if !(1..=31).contains(&d) || !(1..=12).contains(&m) || !(1970..=2100).contains(&y) {
        return None;
    }
    // Gregorian calendar → Julian Day Number (proleptic, includes century corrections)
    let a = (14 - m) / 12;
    let yy = y + 4800 - a;
    let mm = m + 12 * a - 3;
    let jdn = d + (153 * mm + 2) / 5 + 365 * yy + yy / 4 - yy / 100 + yy / 400 - 32045;
    Some((jdn - 2_440_588) * 86_400)
}

/// Extract the best publication date from a normalized_json payload.
/// Returns None for `cotacoes` (historical price data, lower priority).
fn extract_published_at(section: &str, normalized_json: &str) -> Option<i64> {
    let pairs: Vec<[String; 2]> = serde_json::from_str(normalized_json).ok()?;
    let date_keys: &[&str] = match section {
        "comunicados" => &[
            "Data Referência",
            "Data Referencia",
            "Data Entrega",
            "data referência",
            "data referencia",
            "data entrega",
        ],
        "proventos" => &["DATA BASE", "DATA PAGAMENTO", "Data Referência", "Data Referencia"],
        "informacoes_basicas" => &[
            "data referência",
            "data referencia",
            "data entrega",
            "DATA COM",
            "MÊS REF.",
            "MES REF.",
        ],
        _ => return None,
    };
    for [key, val] in &pairs {
        if date_keys.contains(&key.as_str()) && let Some(epoch) = parse_date_br(val) {
            return Some(epoch);
        }
    }
    None
}

/// Parse "--since" value: "YYYY-MM-DD" or "Nd" (e.g. "30d") → Unix epoch seconds.
pub fn parse_since(s: &str) -> Result<i64, String> {
    if let Some(days_str) = s.strip_suffix('d') {
        let n: i64 = days_str
            .parse()
            .map_err(|_| format!("invalid --since value: {s}"))?;
        let now = store::now_epoch_seconds().map_err(|e| e.to_string())?;
        return Ok(now - n * 86_400);
    }
    // YYYY-MM-DD
    let parts: Vec<&str> = s.splitn(3, '-').collect();
    if parts.len() != 3 {
        return Err(format!("invalid --since date: {s} (expected YYYY-MM-DD or Nd)"));
    }
    parse_date_br(&format!("{}/{}/{}", parts[2], parts[1], parts[0]))
        .ok_or_else(|| format!("invalid --since date: {s}"))
}

fn build_normalized_json(fields: &HashMap<String, String>) -> String {
    let mut sorted: Vec<(&str, &str)> = fields
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    sorted.sort_by_key(|(k, _)| *k);
    serde_json::to_string(&sorted).unwrap_or_else(|_| "[]".to_string())
}

pub fn run_all(since: Option<i64>) -> Result<(), CliError> {
    collect_run(None, None)?;
    report_latest(since)
}

pub fn config_doctor() -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    config::bootstrap_layout(&loaded).map_err(CliError::fatal)?;

    print_config(&loaded);

    let report = config::run_doctor_checks(&loaded, SystemTime::now());
    for issue in report.issues {
        match issue.severity {
            DoctorIssueSeverity::Warning => println!("warning: {}", issue.message),
            DoctorIssueSeverity::Error => println!("error: {}", issue.message),
        }
    }

    if report.error_count > 0 {
        return Err(CliError::fatal(format!(
            "config doctor found {} error(s)",
            report.error_count
        )));
    }

    println!(
        "config doctor completed (warnings: {}, errors: {})",
        report.warning_count, report.error_count
    );
    Ok(())
}

pub fn browser_status() -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    config::bootstrap_layout(&loaded).map_err(CliError::fatal)?;

    let manager = BrowserManager::from_config(&loaded).map_err(CliError::fatal)?;
    let status = manager.status();
    let mode = match status.mode {
        BrowserMode::Bundled => "bundled",
        BrowserMode::System => "system",
    };

    println!("browser status");
    println!("  mode: {mode}");
    println!("  platform: {}", status.platform.as_str());
    println!("  target_build: {}", status.target_build);
    println!(
        "  active_build: {}",
        status
            .active_build
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    );
    println!(
        "  installed: {}",
        if status.is_installed { "yes" } else { "no" }
    );
    println!("  current_path: {}", status.current_path.display());
    println!("  metadata_path: {}", status.lock_path.display());
    println!("  download_url: {}", status.download_url);

    Ok(())
}

pub fn browser_install(force: bool) -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    config::bootstrap_layout(&loaded).map_err(CliError::fatal)?;
    let manager = BrowserManager::from_config(&loaded).map_err(CliError::fatal)?;
    let status = manager.install(force).map_err(CliError::fatal)?;
    print_browser_status_result("browser install", &status);
    Ok(())
}

pub fn browser_upgrade() -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    config::bootstrap_layout(&loaded).map_err(CliError::fatal)?;
    let manager = BrowserManager::from_config(&loaded).map_err(CliError::fatal)?;
    let status = manager.upgrade().map_err(CliError::fatal)?;
    print_browser_status_result("browser upgrade", &status);
    Ok(())
}

pub fn browser_rollback() -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    config::bootstrap_layout(&loaded).map_err(CliError::fatal)?;
    let manager = BrowserManager::from_config(&loaded).map_err(CliError::fatal)?;
    let status = manager.rollback().map_err(CliError::fatal)?;
    print_browser_status_result("browser rollback", &status);
    Ok(())
}

pub fn browser_reset_profile() -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    config::bootstrap_layout(&loaded).map_err(CliError::fatal)?;
    let manager = BrowserManager::from_config(&loaded).map_err(CliError::fatal)?;
    manager.reset_profile().map_err(CliError::fatal)?;
    println!(
        "browser reset-profile completed: {}",
        loaded.resolved.browser_user_data_dir.display()
    );
    Ok(())
}

pub fn display_start(listen_addr: String) -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    config::bootstrap_layout(&loaded).map_err(CliError::fatal)?;
    let manager = DisplayManager::new(&loaded.resolved.root_dir);
    let session = manager
        .start(listen_addr.as_str())
        .map_err(CliError::fatal)?;
    println!("display start");
    println!("  listen_addr: {}", session.listen_addr);
    println!("  runtime_dir: {}", session.runtime_dir.display());
    println!("  wayland_socket: {}", session.wayland_socket);
    println!("  weston_pid: {}", session.weston_pid);
    Ok(())
}

pub fn display_stop() -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    let manager = DisplayManager::new(&loaded.resolved.root_dir);
    manager.stop().map_err(CliError::fatal)?;
    println!("display stop: session terminated");
    Ok(())
}

pub fn display_status() -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    let manager = DisplayManager::new(&loaded.resolved.root_dir);
    let status = manager.status().map_err(CliError::fatal)?;
    println!("display status");
    if let Some(session) = status.session {
        println!("  running: {}", if status.running { "yes" } else { "no" });
        println!("  listen_addr: {}", session.listen_addr);
        println!("  runtime_dir: {}", session.runtime_dir.display());
        println!("  wayland_socket: {}", session.wayland_socket);
        println!("  weston_pid: {}", session.weston_pid);
    } else {
        println!("  running: no");
    }
    Ok(())
}

fn print_browser_status_result(title: &str, status: &BrowserInstallState) {
    println!("{title}");
    println!(
        "  active_build: {}",
        status
            .active_build
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    );
    println!(
        "  rollback_available: {}",
        if status.rollback_available {
            "yes"
        } else {
            "no"
        }
    );
    println!("  current_path: {}", status.current_path.display());
    println!("  metadata_path: {}", status.lock_path.display());
}

fn print_config(config: &OmensConfig) {
    println!("config doctor: resolved runtime paths");
    println!("  config.file: {}", config.resolved.config_file.display());
    println!("  runtime.root_dir: {}", config.resolved.root_dir.display());
    println!(
        "  browser.user_data_dir: {}",
        config.resolved.browser_user_data_dir.display()
    );
    println!(
        "  storage.db_path: {}",
        config.resolved.storage_db_path.display()
    );
    println!(
        "  storage.lock_path: {}",
        config.resolved.storage_lock_path.display()
    );
    println!(
        "  reports.output_dir: {}",
        config.resolved.reports_output_dir.display()
    );
}

pub fn report_latest(since: Option<i64>) -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    config::bootstrap_layout(&loaded).map_err(CliError::fatal)?;

    let store = Store::open(&loaded.resolved.storage_db_path).map_err(CliError::fatal)?;
    store.migrate().map_err(CliError::fatal)?;

    let run_id = match store.latest_run_id().map_err(CliError::fatal)? {
        Some(id) => id,
        None => {
            println!("report latest: no runs found");
            println!("  run `omens collect run --tickers TICKER` first");
            return Ok(());
        }
    };

    let all_signals = store
        .signals_with_items_for_run(run_id)
        .map_err(CliError::fatal)?;

    let high_impact = loaded.analysis.thresholds.high_impact;

    // Apply display filter: critical/high always; medium only above high_impact; low/ignore hidden
    let mut filtered: Vec<&SignalWithItem> = all_signals
        .iter()
        .filter(|s| match s.severity.as_str() {
            "critical" | "high" => true,
            "medium" => s.confidence >= high_impact,
            _ => false,
        })
        .collect();

    // Apply --since filter: keep signal if published_at >= since OR published_at IS NULL
    if let Some(since_ts) = since {
        filtered.retain(|s| s.published_at.is_none_or(|ts| ts >= since_ts));
    }

    // Sort: severity rank desc, confidence desc, published_at desc
    filtered.sort_by(|a, b| {
        severity_rank(&b.severity)
            .cmp(&severity_rank(&a.severity))
            .then(
                b.confidence
                    .partial_cmp(&a.confidence)
                    .unwrap_or(std::cmp::Ordering::Equal),
            )
            .then(
                b.published_at
                    .unwrap_or(0)
                    .cmp(&a.published_at.unwrap_or(0)),
            )
    });

    println!("report latest");
    println!("  run_id: {run_id}");
    println!("  total_signals: {}", all_signals.len());
    if let Some(since_ts) = since {
        println!("  since: {since_ts} (epoch)");
    }
    println!(
        "  shown: {} (critical/high + medium >= {:.0}% confidence)",
        filtered.len(),
        high_impact * 100.0
    );

    if !filtered.is_empty() {
        println!();
        let mut current_severity = String::new();
        for sig in &filtered {
            if sig.severity != current_severity {
                current_severity = sig.severity.clone();
                println!("--- {} ---", current_severity.to_uppercase());
            }
            println!(
                "  [{:<8} {:.2}] {}",
                sig.severity.to_uppercase(),
                sig.confidence,
                sig.summary
            );
            println!("    section: {} | key: {}", sig.section, sig.stable_key);
            if let Some(url) = &sig.url {
                println!("    url: {url}");
            }
            if let Some(reasons_json) = &sig.reasons_json
                && let Ok(reasons) = serde_json::from_str::<Vec<String>>(reasons_json)
                && !reasons.is_empty()
            {
                println!("    reasons: {}", reasons.join("; "));
            }
        }
    }

    let generated_at = store::now_epoch_seconds().map_err(CliError::fatal)?;

    // Write reports/latest.json
    let json_path = loaded.resolved.reports_output_dir.join("latest.json");
    let json_str = build_report_json(run_id, generated_at, &all_signals, &filtered);
    std::fs::write(&json_path, &json_str).map_err(|err| {
        CliError::fatal(format!(
            "failed writing {}: {err}",
            json_path.display()
        ))
    })?;

    // Write reports/latest.md
    let md_path = loaded.resolved.reports_output_dir.join("latest.md");
    let md_str = build_report_md(run_id, generated_at, &all_signals, &filtered);
    std::fs::write(&md_path, &md_str).map_err(|err| {
        CliError::fatal(format!(
            "failed writing {}: {err}",
            md_path.display()
        ))
    })?;

    println!("\n  reports:");
    println!("    {}", json_path.display());
    println!("    {}", md_path.display());

    Ok(())
}

fn severity_rank(s: &str) -> u8 {
    match s {
        "critical" => 4,
        "high" => 3,
        "medium" => 2,
        "low" => 1,
        _ => 0,
    }
}

fn build_report_json(
    run_id: i64,
    generated_at: i64,
    all_signals: &[SignalWithItem],
    filtered: &[&SignalWithItem],
) -> String {
    let signals_json: Vec<serde_json::Value> = filtered
        .iter()
        .map(|s| {
            let reasons: Vec<String> = s
                .reasons_json
                .as_deref()
                .and_then(|j| serde_json::from_str(j).ok())
                .unwrap_or_default();
            serde_json::json!({
                "severity": s.severity,
                "confidence": s.confidence,
                "kind": s.kind,
                "summary": s.summary,
                "reasons": reasons,
                "section": s.section,
                "stable_key": s.stable_key,
                "title": s.title,
                "url": s.url,
                "published_at": s.published_at,
            })
        })
        .collect();

    let report = serde_json::json!({
        "run_id": run_id,
        "generated_at": generated_at,
        "total_signals": all_signals.len(),
        "shown": filtered.len(),
        "signals": signals_json,
    });

    serde_json::to_string_pretty(&report).unwrap_or_else(|_| "{}".to_string())
}

fn build_report_md(
    run_id: i64,
    generated_at: i64,
    all_signals: &[SignalWithItem],
    filtered: &[&SignalWithItem],
) -> String {
    use std::fmt::Write as _;

    let mut md = String::new();
    let _ = writeln!(md, "# Omens Report — Run #{run_id}");
    let _ = writeln!(md);
    let _ = writeln!(md, "Generated at epoch: {generated_at}");
    let _ = writeln!(md);
    let _ = writeln!(
        md,
        "## Signals ({} shown / {} total)",
        filtered.len(),
        all_signals.len()
    );

    if filtered.is_empty() {
        let _ = writeln!(md);
        let _ = writeln!(md, "_No signals to display._");
        return md;
    }

    let mut current_severity = String::new();
    for sig in filtered {
        if sig.severity != current_severity {
            current_severity = sig.severity.clone();
            let _ = writeln!(md);
            let _ = writeln!(md, "### {}", current_severity.to_uppercase());
        }
        let reasons: Vec<String> = sig
            .reasons_json
            .as_deref()
            .and_then(|j| serde_json::from_str(j).ok())
            .unwrap_or_default();

        let _ = writeln!(
            md,
            "- **[{} {:.2}]** {}",
            sig.severity.to_uppercase(),
            sig.confidence,
            sig.summary
        );
        let _ = writeln!(
            md,
            "  - Section: `{}` | Key: `{}`",
            sig.section, sig.stable_key
        );
        if let Some(url) = &sig.url {
            let _ = writeln!(md, "  - URL: {url}");
        }
        if !reasons.is_empty() {
            let _ = writeln!(md, "  - Reasons: {}", reasons.join("; "));
        }
    }

    md
}

pub fn map_auth_error(err: AuthError) -> CliError {
    match err {
        AuthError::AuthRequired(msg) => CliError::auth_required(msg),
        AuthError::Runtime(msg) => CliError::fatal(msg),
    }
}

#[cfg(test)]
mod tests {
    use super::{extract_published_at, parse_date_br, parse_since};

    // 2023-08-31 00:00:00 UTC
    const AUG31_2023: i64 = 1693440000;

    #[test]
    fn parse_date_br_valid() {
        assert_eq!(parse_date_br("31/08/2023"), Some(AUG31_2023));
    }

    #[test]
    fn parse_date_br_epoch_start() {
        // 1970-01-01 = epoch 0
        assert_eq!(parse_date_br("01/01/1970"), Some(0));
    }

    #[test]
    fn parse_date_br_with_timestamp() {
        // Dates like "29/12/2025 10:00:00" should strip the time and parse fine
        assert_eq!(parse_date_br("29/12/2025 10:00:00"), parse_date_br("29/12/2025"));
    }

    #[test]
    fn parse_date_br_invalid() {
        assert_eq!(parse_date_br("not-a-date"), None);
        assert_eq!(parse_date_br("32/01/2023"), None);
        assert_eq!(parse_date_br("01/13/2023"), None);
        assert_eq!(parse_date_br(""), None);
    }

    #[test]
    fn extract_date_comunicados() {
        let json = r#"[["Assunto","Foo"],["Data Referência","31/08/2023"]]"#;
        assert_eq!(extract_published_at("comunicados", json), Some(AUG31_2023));
    }

    #[test]
    fn extract_date_proventos() {
        let json = r#"[["DATA BASE","31/08/2023"],["VALOR","R$1.00"]]"#;
        assert_eq!(extract_published_at("proventos", json), Some(AUG31_2023));
    }

    #[test]
    fn extract_date_informacoes_basicas() {
        let json = r#"[["DATA COM","31/08/2023"],["CNPJ","00.000.000/0001-00"]]"#;
        assert_eq!(
            extract_published_at("informacoes_basicas", json),
            Some(AUG31_2023)
        );
    }

    #[test]
    fn extract_date_missing_key() {
        let json = r#"[["Assunto","Foo"],["Valor","R$1.00"]]"#;
        assert_eq!(extract_published_at("comunicados", json), None);
    }

    #[test]
    fn extract_date_cotacoes_skipped() {
        let json = r#"[["Data Referência","31/08/2023"]]"#;
        assert_eq!(extract_published_at("cotacoes", json), None);
    }

    #[test]
    fn parse_since_iso_date() {
        assert_eq!(parse_since("2023-08-31"), Ok(AUG31_2023));
    }

    #[test]
    fn parse_since_relative_zero() {
        // 0d means "now" — just verify it doesn't error and returns a plausible epoch
        let result = parse_since("0d");
        assert!(result.is_ok());
        assert!(result.unwrap() > 1_000_000_000);
    }

    #[test]
    fn parse_since_invalid() {
        assert!(parse_since("bad").is_err());
        assert!(parse_since("2023-08").is_err());
        assert!(parse_since("xd").is_err());
    }
}
