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

const FNET_LANDING_URL: &str = "https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosCVM?paginaCertificados=false&tipoFundo=1";

fn is_fnet_url(url: &str) -> bool {
    url.contains("fnet.bmfbovespa.com.br")
        || url.contains("bvmf.bmfbovespa.com.br")
        || url.contains("b3.com.br")
}

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

pub fn auth_bootstrap(ephemeral: bool, system_display: bool) -> Result<(), CliError> {
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

    let launch_env = display_launch_env(&loaded.resolved.root_dir, system_display)?;

    let mut harness = ChromiumoxideHarness::new(
        browser_binary,
        profile_path.clone(),
        launch_env,
        loaded.browser.extra_args.clone(),
    )
    .map_err(CliError::fatal)?;
    harness
        .launch(loaded.clubefii.base_url.as_str())
        .map_err(CliError::fatal)?;
    harness.enable_stealth().map_err(CliError::fatal)?;

    println!("auth bootstrap");
    println!("  opened: {}", loaded.clubefii.base_url);
    println!("  profile: {}", profile_path.display());
    println!("  complete login in the browser, then press Enter here to validate session.");

    let mut line = String::new();
    io::stdin()
        .read_line(&mut line)
        .map_err(|err| CliError::fatal(format!("failed reading confirmation input: {err}")))?;

    let auth_config = AuthValidationConfig {
        base_url: loaded.clubefii.base_url.clone(),
        required_marker: loaded.clubefii.auth_marker.clone(),
        protected_probe_url: loaded.clubefii.protected_probe_url.clone(),
        login_timeout: Duration::from_secs(120),
        poll_interval: Duration::from_secs(2),
    };

    let result = auth::wait_for_login(&harness, &auth_config).map_err(map_auth_error);

    if result.is_ok() {
        match harness.persist_session_cookies("clubefii") {
            Ok(n) if n > 0 => println!("  persisted {n} session cookie(s)"),
            Ok(_) => {}
            Err(e) => eprintln!("  warning: failed to persist session cookies: {e}"),
        }
    }

    let _ = harness.shutdown();
    drop(ephemeral_profile);

    result?;
    println!("auth bootstrap: session validation passed");
    Ok(())
}

pub fn browser_open(
    url: Option<String>,
    system_display: bool,
    cli_extra_args: Vec<String>,
) -> Result<(), CliError> {
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

    let target = url.as_deref().unwrap_or("chrome://newtab");

    let mut cmd = std::process::Command::new(&browser_binary);
    cmd.arg(format!("--user-data-dir={}", profile_path.display()));

    // Config extra_args first, then CLI overrides
    for arg in &loaded.browser.extra_args {
        cmd.arg(arg);
    }
    for arg in &cli_extra_args {
        cmd.arg(arg);
    }

    cmd.arg(target);

    let launch_env = display_launch_env(&loaded.resolved.root_dir, system_display)?;
    for (key, value) in &launch_env {
        cmd.env(key, value);
    }
    if !launch_env.is_empty() {
        cmd.arg("--ozone-platform=wayland");
        cmd.arg("--force-device-scale-factor=1");
    }

    println!("browser open");
    println!("  url: {target}");
    println!("  profile: {}", profile_path.display());

    let mut child = cmd.spawn().map_err(|e| {
        CliError::fatal(format!(
            "failed to launch browser {}: {e}",
            browser_binary.display()
        ))
    })?;

    child
        .wait()
        .map_err(|e| CliError::fatal(format!("failed waiting for browser process: {e}")))?;

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

    let launch_env = display_launch_env(&loaded.resolved.root_dir, false)?;

    let mut harness = ChromiumoxideHarness::new(
        browser_binary,
        profile_path,
        launch_env,
        loaded.browser.extra_args.clone(),
    )
    .map_err(CliError::fatal)?;

    let store = Store::open(&loaded.resolved.storage_db_path).map_err(CliError::fatal)?;
    store.migrate().map_err(CliError::fatal)?;

    let fixture_writer = FixtureWriter::new(&loaded.resolved.root_dir.join("fixtures"));

    println!("explore start: {url}");
    harness.launch(&url).map_err(CliError::fatal)?;
    let _ = harness.enable_stealth();

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
                store
                    .set_published_at(*item_id, ts)
                    .map_err(CliError::fatal)?;
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
                    external_id: r.external_id,
                    stable_key: r.stable_key,
                    payload_json: r.payload_json,
                    is_new: r.version_count == 1,
                })
                .collect();

            // Build provento history for context-aware scoring (one DB query per ticker).
            let mut history_map: std::collections::HashMap<
                String,
                Vec<analyze::HistoricalProvento>,
            > = std::collections::HashMap::new();
            for item in &analysis_items {
                if item.section == "proventos" {
                    let ticker = item.external_id.split('/').next().unwrap_or("");
                    if !ticker.is_empty() && !history_map.contains_key(ticker) {
                        let payloads = store
                            .recent_proventos_for_ticker(ticker, run_id)
                            .map_err(CliError::fatal)?;
                        let history = payloads
                            .iter()
                            .map(|p| analyze::HistoricalProvento::from_payload(p))
                            .collect();
                        history_map.insert(ticker.to_string(), history);
                    }
                }
            }

            let signals = analyze::analyze_items(&analysis_items, &history_map, &loaded.analysis);
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
            if err.contains("re-authenticate with") {
                Err(CliError::auth_required(err))
            } else {
                Err(CliError::fatal(err))
            }
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

    let launch_env =
        display_launch_env(&loaded.resolved.root_dir, false).map_err(|e| e.to_string())?;

    let mut harness = ChromiumoxideHarness::new(
        browser_binary,
        profile_path,
        launch_env,
        loaded.browser.extra_args.clone(),
    )
    .map_err(|e| e.to_string())?;

    let base_url = &loaded.clubefii.base_url;
    let mut stats = CollectStats {
        items_seen: 0,
        items_new: 0,
        items_changed: 0,
    };
    let mut first = true;

    // Load active recipes once — they are global (not per-ticker)
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
            "  no active recipes found; run `omens explore` then `omens explore promote <id>`"
        );
        return Ok(stats);
    }

    for ticker in tickers {
        let fund_url = format!("{base_url}/fiis/{ticker}");
        println!("collect: navigating to {fund_url}");

        if first {
            harness.launch(&fund_url).map_err(|e| e.to_string())?;
            let _ = harness.enable_stealth();
            first = false;
        } else {
            harness.navigate(&fund_url).map_err(|e| e.to_string())?;
        }
        std::thread::sleep(std::time::Duration::from_secs(3));
        harness.dismiss_overlays();

        check_page_auth(&harness).map_err(|e| e.message)?;

        for recipe in &active_recipes {
            let section = &recipe.section;
            println!("  [{ticker}/{section}] extracting...");

            // Click the tab anchor
            let click_sel = format!("a[href='#{section}']");
            if let Err(e) = harness.click_and_wait(&click_sel, 10_000) {
                // Tab click failures are often transient (timing, browser state);
                // skip this section for this ticker without permanently degrading.
                println!("    skip: tab click failed: {e}");
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
                    "Data Referência",
                    "Data Referencia",
                    "Data Entrega",
                    "MÊS REF.",
                    "MES REF.",
                    "DATA COM",
                    "Assunto",
                    "assunto",
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
                        if first.is_empty() {
                            i.to_string()
                        } else {
                            first.to_string()
                        }
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
                    let hash = content_hash_for_section(section, &normalized_json);
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
                    let first_val = fields.get(first_fid).map(|s| s.trim()).unwrap_or("");
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
                    let hash = content_hash_for_section(section, &normalized_json);
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
        "proventos" => &[
            "DATA BASE",
            "DATA PAGAMENTO",
            "Data Referência",
            "Data Referencia",
        ],
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
        if date_keys.contains(&key.as_str())
            && let Some(epoch) = parse_date_br(val)
        {
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
        return Err(format!(
            "invalid --since date: {s} (expected YYYY-MM-DD or Nd)"
        ));
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

/// Fields excluded from the content hash for proventos items.
/// These are derived from market price and change daily without reflecting
/// an actual change to the dividend declaration.
const PROVENTOS_VOLATILE_FIELDS: &[&str] = &["COTAÇÃO DAT. BASE", "YIELD DAT. BASE"];

/// Compute a content hash that excludes volatile fields for certain sections.
fn content_hash_for_section(section: &str, normalized_json: &str) -> String {
    if section == "proventos"
        && let Ok(pairs) = serde_json::from_str::<Vec<[String; 2]>>(normalized_json)
    {
        let filtered: Vec<&[String; 2]> = pairs
            .iter()
            .filter(|[k, _]| !PROVENTOS_VOLATILE_FIELDS.contains(&k.as_str()))
            .collect();
        return store::content_hash_fnv(
            &serde_json::to_string(&filtered).unwrap_or_else(|_| "[]".to_string()),
        );
    }
    store::content_hash_fnv(normalized_json)
}

pub fn run_all() -> Result<(), CliError> {
    collect_run(None, None)?;
    do_report(None)
}

pub fn send_email(path: String) -> Result<(), CliError> {
    use lettre::message::header::ContentType;
    use lettre::transport::smtp::authentication::Credentials;
    use lettre::{Message, SmtpTransport, Transport};
    use std::fs;
    use std::path::Path;

    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    let email_cfg = &loaded.email;

    if !email_cfg.enabled {
        return Err(CliError::fatal(
            "email is disabled; set email.enabled = true in config",
        ));
    }
    if email_cfg.to.is_empty() {
        return Err(CliError::fatal(
            "email.to is empty; add at least one recipient in config",
        ));
    }
    if email_cfg.smtp_username.is_empty() || email_cfg.smtp_password.is_empty() {
        return Err(CliError::fatal(
            "email.smtp_username and email.smtp_password must be set in config",
        ));
    }

    let body = fs::read_to_string(&path)
        .map_err(|e| CliError::fatal(format!("failed to read {path}: {e}")))?;

    let stem = Path::new(&path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("report");

    // Scan signal lines (e.g. "- C 0.90 ..." or "- H 0.85 ...") for the highest severity.
    let highest = body
        .lines()
        .filter_map(|line| {
            let s = line.trim_start_matches("- ").trim_start();
            s.split_whitespace().next()
        })
        .filter_map(|token| match token {
            "C" => Some(4u8),
            "H" => Some(3),
            "M" => Some(2),
            "L" => Some(1),
            _ => None,
        })
        .max();
    let severity_tag = match highest {
        Some(4) => " [CRITICAL]",
        Some(3) => " [HIGH]",
        Some(2) => " [MEDIUM]",
        Some(1) => " [LOW]",
        _ => "",
    };
    let subject = format!("{severity_tag} {stem} \u{2014} omens report");

    let from_addr = if email_cfg.from.is_empty() {
        email_cfg.smtp_username.clone()
    } else {
        email_cfg.from.clone()
    };

    let mut builder = Message::builder()
        .from(
            from_addr
                .parse()
                .map_err(|e| CliError::fatal(format!("invalid from address {from_addr:?}: {e}")))?,
        )
        .subject(&subject);

    for recipient in &email_cfg.to {
        builder = builder.to(recipient.parse().map_err(|e| {
            CliError::fatal(format!("invalid recipient address {recipient:?}: {e}"))
        })?);
    }

    let message = builder
        .header(ContentType::TEXT_PLAIN)
        .body(body)
        .map_err(|e| CliError::fatal(format!("failed to build email message: {e}")))?;

    let creds = Credentials::new(
        email_cfg.smtp_username.clone(),
        email_cfg.smtp_password.clone(),
    );

    let mailer = SmtpTransport::starttls_relay(&email_cfg.smtp_host)
        .map_err(|e| CliError::fatal(format!("failed to connect to SMTP host: {e}")))?
        .port(email_cfg.smtp_port)
        .credentials(creds)
        .build();

    mailer
        .send(&message)
        .map_err(|e| CliError::fatal(format!("failed to send email: {e}")))?;

    let recipients = email_cfg.to.join(", ");
    println!("email sent: {subject} \u{2192} {recipients}");

    Ok(())
}

pub fn chat(system_display: bool) -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    config::bootstrap_layout(&loaded).map_err(CliError::fatal)?;

    if !loaded.chat.enabled {
        return Err(CliError::fatal(
            "chat is disabled; set chat.enabled = true in config",
        ));
    }
    if loaded.chat.model.is_empty() {
        return Err(CliError::fatal(
            "chat.model is empty; set a model name in config",
        ));
    }

    let manager = BrowserManager::from_config(&loaded).map_err(CliError::fatal)?;
    let browser_binary = manager.browser_binary_path().map_err(CliError::fatal)?;

    let profile_path = manager.default_profile_dir().to_path_buf();
    std::fs::create_dir_all(&profile_path).map_err(|err| {
        CliError::fatal(format!(
            "failed to create browser profile {}: {err}",
            profile_path.display()
        ))
    })?;

    let launch_env = display_launch_env(&loaded.resolved.root_dir, system_display)?;

    let mut harness = ChromiumoxideHarness::new(
        browser_binary,
        profile_path,
        launch_env,
        loaded.browser.extra_args.clone(),
    )
    .map_err(CliError::fatal)?;
    harness.launch("about:blank").map_err(CliError::fatal)?;

    let result = crate::chat::run_chat_loop(&mut harness, &loaded.chat);
    let _ = harness.shutdown();
    result.map_err(CliError::fatal)
}

pub fn browse(cmd: super::BrowseCommand) -> Result<(), CliError> {
    use crate::browse;
    use crate::browse::session::BrowseSessionManager;

    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    config::bootstrap_layout(&loaded).map_err(CliError::fatal)?;

    let session_mgr = BrowseSessionManager::new(&loaded.resolved.root_dir);

    match cmd {
        super::BrowseCommand::Start {
            port,
            system_display,
        } => {
            let manager = BrowserManager::from_config(&loaded).map_err(CliError::fatal)?;
            let browser_binary = manager.browser_binary_path().map_err(CliError::fatal)?;
            let profile_dir = manager.default_profile_dir().to_path_buf();

            let launch_env = display_launch_env(&loaded.resolved.root_dir, system_display)?;

            let mut extra_args = loaded.browser.extra_args.clone();
            if !launch_env.is_empty() {
                extra_args.push("--ozone-platform=wayland".to_string());
                extra_args.push("--force-device-scale-factor=1".to_string());
            }

            let session = session_mgr
                .start(
                    &browser_binary,
                    &profile_dir,
                    port,
                    &launch_env,
                    &extra_args,
                )
                .map_err(CliError::fatal)?;

            println!("browse session started");
            println!("  pid: {}", session.pid);
            println!("  port: {}", session.port);
            println!("  profile: {}", session.profile_dir.display());
            Ok(())
        }
        super::BrowseCommand::Stop => {
            session_mgr.stop().map_err(CliError::fatal)?;
            println!("browse session stopped");
            Ok(())
        }
        super::BrowseCommand::Status => {
            match session_mgr.status().map_err(CliError::fatal)? {
                Some(session) => {
                    println!("browse session running");
                    println!("  pid: {}", session.pid);
                    println!("  port: {}", session.port);
                    println!("  profile: {}", session.profile_dir.display());
                }
                None => {
                    println!("browse session not running");
                }
            }
            Ok(())
        }
        super::BrowseCommand::Navigate { url } => {
            let port = require_session(&session_mgr)?.port;
            browse::commands::navigate(port, &url).map_err(CliError::fatal)
        }
        super::BrowseCommand::Content { max_chars, full } => {
            let port = require_session(&session_mgr)?.port;
            let max = if max_chars > 0 {
                max_chars
            } else {
                loaded.browser.max_page_chars
            };
            browse::commands::content(port, max, full).map_err(CliError::fatal)
        }
        super::BrowseCommand::Click { selector } => {
            let port = require_session(&session_mgr)?.port;
            browse::commands::click(port, &selector).map_err(CliError::fatal)
        }
        super::BrowseCommand::Type { selector, text } => {
            let port = require_session(&session_mgr)?.port;
            browse::commands::type_text(port, &selector, &text).map_err(CliError::fatal)
        }
        super::BrowseCommand::Find {
            selector,
            max_results,
        } => {
            let port = require_session(&session_mgr)?.port;
            browse::commands::find(port, &selector, max_results).map_err(CliError::fatal)
        }
        super::BrowseCommand::Scroll { direction, pixels } => {
            let port = require_session(&session_mgr)?.port;
            browse::commands::scroll(port, direction, pixels).map_err(CliError::fatal)
        }
        super::BrowseCommand::Eval { expression } => {
            let port = require_session(&session_mgr)?.port;
            browse::commands::eval(port, &expression).map_err(CliError::fatal)
        }
        super::BrowseCommand::Links {
            contains,
            max_results,
        } => {
            let port = require_session(&session_mgr)?.port;
            browse::commands::links(port, contains.as_deref(), max_results).map_err(CliError::fatal)
        }
        super::BrowseCommand::Source => {
            let port = require_session(&session_mgr)?.port;
            browse::commands::source(port).map_err(CliError::fatal)
        }
        super::BrowseCommand::Url => {
            let port = require_session(&session_mgr)?.port;
            browse::commands::url(port).map_err(CliError::fatal)
        }
    }
}

/// Returns display environment variables for browser launch.
///
/// When `system_display` is true, inherits the caller's display environment
/// (e.g. DISPLAY or WAYLAND_DISPLAY already set in the process) and returns
/// an empty vec — the browser will use whatever display the system provides.
///
/// Otherwise, ensures a managed Weston RDP display is running (auto-starting
/// it if needed) and returns the env vars pointing at it.
fn display_launch_env(
    root_dir: &std::path::Path,
    system_display: bool,
) -> Result<Vec<(String, String)>, CliError> {
    if system_display {
        return Ok(vec![]);
    }

    let dm = DisplayManager::new(root_dir);
    let session = dm
        .ensure_running(crate::runtime::display_manager::DEFAULT_LISTEN_ADDR)
        .map_err(CliError::fatal)?;
    Ok(vec![
        (
            "XDG_RUNTIME_DIR".to_string(),
            session.runtime_dir.display().to_string(),
        ),
        ("WAYLAND_DISPLAY".to_string(), session.wayland_socket),
    ])
}

/// Check whether the current clubefii page indicates the user is not logged in
/// or is stuck on a bot-verification challenge page.
/// Returns `Err(CliError::auth_required)` if the session appears invalid.
fn check_page_auth(harness: &ChromiumoxideHarness) -> Result<(), CliError> {
    let js = r#"(function() {
        var tools = document.querySelector('#tools');
        if (tools && tools.textContent.includes('ENTRAR')) return 'login';
        var body = document.body ? document.body.textContent : '';
        if (body.includes('security verification') || body.includes('not a bot')) return 'challenge';
        if (!tools) return 'no-nav';
        return 'ok';
    })()"#;
    let result = harness.evaluate_js(js).map_err(CliError::fatal)?;
    let status = result.trim().trim_matches('"');
    match status {
        "login" => Err(CliError::auth_required(
            "session expired; re-authenticate with `omens auth bootstrap`",
        )),
        "challenge" => Err(CliError::auth_required(
            "blocked by bot verification; re-authenticate with `omens auth bootstrap`",
        )),
        "no-nav" => Err(CliError::auth_required(
            "page did not load (no navigation bar); re-authenticate with `omens auth bootstrap`",
        )),
        _ => Ok(()),
    }
}

fn require_session(
    mgr: &crate::browse::session::BrowseSessionManager,
) -> Result<crate::browse::session::BrowseSession, CliError> {
    mgr.status()
        .map_err(CliError::fatal)?
        .ok_or_else(|| CliError::fatal("no browse session running; run `omens browse start`"))
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

/// Convert a Unix epoch timestamp to "YYYY-MM-DD" (UTC midnight only; time component ignored).
fn epoch_to_date_str(ts: i64) -> String {
    let jdn = ts / 86_400 + 2_440_588;
    let l = jdn + 68_569;
    let n = 4 * l / 146_097;
    let l = l - (146_097 * n + 3) / 4;
    let y = 4_000 * (l + 1) / 1_461_001;
    let l = l - 1_461 * y / 4 + 31;
    let m = 80 * l / 2_447;
    let d = l - 2_447 * m / 80;
    let l2 = m / 11;
    let m = m + 2 - 12 * l2;
    let y = 100 * (n - 49) + y + l2;
    format!("{y:04}-{m:02}-{d:02}")
}

pub fn report_latest() -> Result<(), CliError> {
    do_report(None)
}

pub fn report_since(since_ts: i64) -> Result<(), CliError> {
    do_report(Some(since_ts))
}

// ── fetch-doc ────────────────────────────────────────────────────────────────

/// Fetch the text content of a document identified by URL or stable_key.
/// Navigates using the authenticated browser, handles FNET HTML pages and
/// clubefii embed PDF pages. Outputs text to stdout. Caches in
/// ~/.cache/omens/docs/.
pub fn fetch_doc(url_or_key: String) -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    config::bootstrap_layout(&loaded).map_err(CliError::fatal)?;

    let home = std::env::var("HOME")
        .map_err(|_| CliError::fatal("HOME environment variable is not set"))?;
    let cache_dir = std::path::Path::new(&home).join(".cache/omens/docs");
    std::fs::create_dir_all(&cache_dir)
        .map_err(|e| CliError::fatal(format!("failed to create cache dir: {e}")))?;

    let is_url = url_or_key.starts_with("http://") || url_or_key.starts_with("https://");

    // For direct URLs, check cache before launching the browser.
    if is_url {
        let cache_path = cache_dir.join(format!("{}.txt", store::content_hash_fnv(&url_or_key)));
        if cache_path.exists() {
            let text = std::fs::read_to_string(&cache_path)
                .map_err(|e| CliError::fatal(format!("read cache: {e}")))?;
            print!("{text}");
            return Ok(());
        }
    }

    // Set up browser (needed for both stable_key lookup and authenticated fetch).
    let manager = BrowserManager::from_config(&loaded).map_err(CliError::fatal)?;
    let browser_binary = manager.browser_binary_path().map_err(CliError::fatal)?;
    let profile_path = manager.default_profile_dir().to_path_buf();

    let launch_env = display_launch_env(&loaded.resolved.root_dir, false)?;

    // Determine the initial URL to open in the browser.
    let initial_url = if is_url {
        url_or_key.clone()
    } else {
        // stable_key: look up list-page URL from the DB.
        let stable_key = if url_or_key.starts_with("external_id:") {
            url_or_key.clone()
        } else {
            format!("external_id:{}", url_or_key)
        };
        let store = Store::open(&loaded.resolved.storage_db_path).map_err(CliError::fatal)?;
        store.migrate().map_err(CliError::fatal)?;
        let item = store
            .find_item_by_stable_key(&stable_key)
            .map_err(CliError::fatal)?
            .ok_or_else(|| CliError::fatal(format!("item not found in DB: {stable_key}")))?;
        item.url
            .ok_or_else(|| CliError::fatal("item has no source URL stored in DB"))?
    };

    // For FNET URLs, avoid navigating Chrome directly to PDF-serving pages
    // (Chrome dumps PDF content to stdout). Instead, land on a neutral HTML page
    // on the same domain to pass Cloudflare, then use JS fetch for the document.
    let is_fnet_direct = is_url && is_fnet_url(&initial_url);
    let launch_url = if is_fnet_direct {
        "about:blank".to_string()
    } else {
        initial_url.clone()
    };

    let mut harness = ChromiumoxideHarness::new(
        browser_binary,
        profile_path,
        launch_env,
        loaded.browser.extra_args.clone(),
    )
    .map_err(CliError::fatal)?;
    harness.launch(&launch_url).map_err(CliError::fatal)?;
    let _ = harness.enable_stealth();
    if is_fnet_direct {
        harness
            .navigate(FNET_LANDING_URL)
            .map_err(CliError::fatal)?;
    }
    std::thread::sleep(Duration::from_secs(5));
    harness.dismiss_overlays();

    // Detect expired/missing login before attempting any tab navigation.
    if !is_url {
        check_page_auth(&harness)?;
    }

    // For stable_key: navigate list page, click the tab, and find the document link.
    let doc_url = if !is_url {
        // Extract stable_key again for metadata lookup.
        let stable_key = if url_or_key.starts_with("external_id:") {
            url_or_key.clone()
        } else {
            format!("external_id:{}", url_or_key)
        };
        let store = Store::open(&loaded.resolved.storage_db_path).map_err(CliError::fatal)?;
        let item = store
            .find_item_by_stable_key(&stable_key)
            .map_err(CliError::fatal)?
            .ok_or_else(|| CliError::fatal(format!("item not found: {stable_key}")))?;

        // Build search term sets for matching the document row in the table.
        // Prefer date fields (short, stable, discriminating) over Assunto text.
        let normalized = item.normalized_json.unwrap_or_default();
        let pairs: Vec<Vec<String>> = serde_json::from_str(&normalized).unwrap_or_default();
        let field = |keys: &[&str]| -> Option<String> {
            for kv in &pairs {
                if let (Some(k), Some(v)) = (kv.first(), kv.get(1)) {
                    let kl = k.trim();
                    if keys.iter().any(|&want| kl == want) {
                        let t = v.trim();
                        if !t.is_empty() && t != "N/D" && t != "N/A" && t != "-" && t != "--" {
                            return Some(t.to_string());
                        }
                    }
                }
            }
            None
        };

        let date_ref = field(&["Data Referência", "Data Referencia", "data referência"]);
        let categoria = field(&["Categoria", "CATEGORIA  \u{25be}"]);
        let assunto = field(&["Assunto", "assunto"]);

        // Build candidate search term sets, most specific first.
        // Each entry is a list of strings that must ALL match in a single row.
        let mut search_term_sets: Vec<Vec<String>> = Vec::new();
        if let (Some(cat), Some(date)) = (&categoria, &date_ref) {
            // e.g. ["Relatório Gerencial", "28/02/2026"] — very precise
            search_term_sets.push(vec![cat.chars().take(30).collect(), date.clone()]);
        }
        if let Some(date) = &date_ref {
            search_term_sets.push(vec![date.clone()]);
        }
        if let Some(subj) = &assunto {
            search_term_sets.push(vec![subj.chars().take(40).collect()]);
        }
        if let Some(cat) = &categoria {
            search_term_sets.push(vec![cat.chars().take(40).collect()]);
        }
        // Last resort: part of the stable_key after the last '|'
        if search_term_sets.is_empty() {
            let fallback: String = stable_key
                .rsplit('|')
                .next()
                .unwrap_or("")
                .chars()
                .take(40)
                .collect();
            if !fallback.is_empty() {
                search_term_sets.push(vec![fallback]);
            }
        }

        // Extract section from stable_key to know which tab to click.
        let section = stable_key
            .strip_prefix("external_id:")
            .unwrap_or(&stable_key)
            .split('/')
            .nth(1)
            .unwrap_or("comunicados");

        // Click the section tab.
        let tab_sel = format!("a[href='#{section}']");
        if let Err(e) = harness.click_and_wait(&tab_sel, 10_000) {
            eprintln!("fetch-doc: tab click failed ({e}), trying to find link without clicking");
        } else {
            std::thread::sleep(Duration::from_millis(500));
        }

        // Try each search term set until we find a matching row.
        let mut found_href: Option<String> = None;
        for terms in &search_term_sets {
            let refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            let result = harness
                .find_row_link_by_texts(&refs)
                .map_err(CliError::fatal)?;
            if let Some(href) = result {
                let href = href.lines().next().unwrap_or(&href).trim().to_string();
                eprintln!(
                    "fetch-doc: found document link via [{}]: {href}",
                    refs.join(", ")
                );
                found_href = Some(href);
                break;
            }
        }
        match found_href {
            Some(href) => href,
            None => {
                harness.shutdown().ok();
                let tried: Vec<String> = search_term_sets.iter().map(|t| t.join("+")).collect();
                return Err(CliError::fatal(format!(
                    "no document link found in table (tried: {})",
                    tried.join(", ")
                )));
            }
        }
    } else {
        url_or_key.clone()
    };

    // Check cache for the resolved doc URL.
    let cache_path = cache_dir.join(format!("{}.txt", store::content_hash_fnv(&doc_url)));
    if cache_path.exists() {
        harness.shutdown().ok();
        let text = std::fs::read_to_string(&cache_path)
            .map_err(|e| CliError::fatal(format!("read cache: {e}")))?;
        print!("{text}");
        return Ok(());
    }

    // Navigate to the document URL if we haven't already.
    // For FNET URLs, navigate to the landing page (not the PDF URL) to avoid
    // Chrome dumping PDF content to stdout, then use JS fetch for the document.
    let doc_is_fnet = is_fnet_url(&doc_url);
    if doc_is_fnet && !is_fnet_direct {
        harness
            .navigate(FNET_LANDING_URL)
            .map_err(CliError::fatal)?;
        std::thread::sleep(Duration::from_secs(5));
    } else if !doc_is_fnet && (!is_url || doc_url != url_or_key) {
        harness.navigate(&doc_url).map_err(CliError::fatal)?;
        std::thread::sleep(Duration::from_secs(5));
    }

    let text = fetch_doc_extract_text(&harness, &doc_url)?;

    harness.shutdown().ok();

    if !text.trim().is_empty() {
        let _ = std::fs::write(&cache_path, &text);
    }
    print!("{text}");
    Ok(())
}

/// Extract text from the current page.  Handles clubefii embed pages (find the
/// BAIXAR COMUNICADO link, download PDF) and FNET pages (may render PDF directly
/// or have a download link), and falls back to HTML-to-text for other pages.
fn fetch_doc_extract_text(harness: &ChromiumoxideHarness, url: &str) -> Result<String, CliError> {
    if url.contains("fundo_comunicados_embed") {
        // clubefii embed page: look for the actual document download link.
        let link = harness
            .find_link_href(
                "a[href*='fnet'], a[href*='exibirDocumento'], a[href$='.pdf'], \
                 a[href*='download'], a[href*='bmfbovespa']",
            )
            .map_err(CliError::fatal)?;
        if let Some(href) = link {
            // For FNET links, navigate to the landing page (not the PDF URL) to
            // avoid Chrome dumping PDF content to stdout.
            let nav_target = if is_fnet_url(&href) {
                FNET_LANDING_URL
            } else {
                &href
            };
            harness.navigate(nav_target).map_err(CliError::fatal)?;
            std::thread::sleep(Duration::from_secs(5));
            return fetch_doc_extract_text(harness, &href);
        }
        // Fall back to page text (the embed page might have inline text).
        let html = harness.page_source().map_err(CliError::fatal)?;
        Ok(html_to_text(&html))
    } else if is_fnet_url(url) || url.ends_with(".pdf") {
        // FNET/B3 document page — browser already navigated here with stealth.
        // Most FNET URLs serve PDFs (even exibirDocumento). Try browser JS fetch
        // first; it handles both PDF and HTML responses.
        match fetch_doc_pdf_via_browser(harness, url) {
            Ok(text) if !text.trim().is_empty() => return Ok(text),
            _ => {}
        }
        // Maybe the page has a separate PDF download link.
        let pdf_link = harness
            .find_link_href("a[href$='.pdf'], #lnkDownload, a[href*='download']")
            .map_err(CliError::fatal)?;
        if let Some(href) = pdf_link {
            return fetch_doc_pdf_via_browser(harness, &href);
        }
        // Last resort: extract text from page source.
        let html = harness.page_source().map_err(CliError::fatal)?;
        Ok(html_to_text(&html))
    } else {
        let html = harness.page_source().map_err(CliError::fatal)?;
        Ok(html_to_text(&html))
    }
}

/// Download a document via the browser's JS fetch (inherits Cloudflare cookies).
/// If the response is PDF, convert via `pdftotext -layout`; otherwise treat as HTML.
fn fetch_doc_pdf_via_browser(
    harness: &ChromiumoxideHarness,
    url: &str,
) -> Result<String, CliError> {
    let bytes = harness
        .fetch_bytes(url)
        .map_err(|e| CliError::fatal(format!("browser fetch {url}: {e}")))?;

    if !bytes.starts_with(b"%PDF") {
        let html = String::from_utf8_lossy(&bytes).into_owned();
        return Ok(html_to_text(&html));
    }

    let tmp_path = format!("/tmp/omens-doc-{}.pdf", store::content_hash_fnv(url));
    std::fs::write(&tmp_path, &bytes)
        .map_err(|e| CliError::fatal(format!("write temp PDF: {e}")))?;

    let output = std::process::Command::new("pdftotext")
        .arg("-layout")
        .arg(&tmp_path)
        .arg("-")
        .output()
        .map_err(|e| CliError::fatal(format!("pdftotext failed (is it installed?): {e}")))?;

    let _ = std::fs::remove_file(&tmp_path);

    if !output.status.success() {
        return Err(CliError::fatal(format!(
            "pdftotext exit {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Extract a field value from a `[["key","val"],...]` normalized_json string.
/// Strip HTML tags and normalize whitespace.  Not a full HTML parser but
/// sufficient for extracting the readable text from structured pages.
fn html_to_text(html: &str) -> String {
    let mut out = String::with_capacity(html.len() / 2);
    let mut in_tag = false;
    let mut in_script = false;
    let mut in_style = false;
    let mut tag_buf = String::new();

    for c in html.chars() {
        match c {
            '<' => {
                in_tag = true;
                tag_buf.clear();
            }
            '>' if in_tag => {
                in_tag = false;
                let tl = tag_buf.trim().to_lowercase();
                let name = tl
                    .trim_start_matches('/')
                    .split_whitespace()
                    .next()
                    .unwrap_or("");
                match name {
                    "script" => in_script = !tl.starts_with('/'),
                    "style" => in_style = !tl.starts_with('/'),
                    "br" | "p" | "div" | "tr" | "li" | "h1" | "h2" | "h3" | "h4" | "h5" | "h6"
                    | "td" | "th" => {
                        if !in_script && !in_style {
                            out.push('\n');
                        }
                    }
                    _ => {}
                }
                tag_buf.clear();
            }
            _ if in_tag => {
                tag_buf.push(c);
            }
            _ if !in_script && !in_style => {
                out.push(c);
            }
            _ => {}
        }
    }

    // Collapse runs of whitespace; preserve single blank lines.
    let mut result = String::with_capacity(out.len());
    let mut blank_lines = 0u32;
    for line in out.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            blank_lines += 1;
            if blank_lines == 1 {
                result.push('\n');
            }
        } else {
            blank_lines = 0;
            result.push_str(trimmed);
            result.push('\n');
        }
    }
    result.trim().to_string()
}

fn do_report(since: Option<i64>) -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    config::bootstrap_layout(&loaded).map_err(CliError::fatal)?;

    let store = Store::open(&loaded.resolved.storage_db_path).map_err(CliError::fatal)?;
    store.migrate().map_err(CliError::fatal)?;

    // In --since mode: cross-run query filtered by published_at.
    // In default mode: signals from the latest collect run only.
    let (run_id, all_signals): (Option<i64>, Vec<SignalWithItem>) = if let Some(ts) = since {
        (None, store.signals_since(ts).map_err(CliError::fatal)?)
    } else {
        let id = match store.latest_run_id().map_err(CliError::fatal)? {
            Some(id) => id,
            None => {
                println!("report latest: no runs found");
                println!("  run `omens collect run --tickers TICKER` first");
                return Ok(());
            }
        };
        (
            Some(id),
            store
                .signals_with_items_for_run(id)
                .map_err(CliError::fatal)?,
        )
    };

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
    if let Some(id) = run_id {
        println!("  run_id: {id}");
    }
    if let Some(ts) = since {
        println!("  since: {}", epoch_to_date_str(ts));
    }
    println!("  total_signals: {}", all_signals.len());
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
    std::fs::write(&json_path, &json_str)
        .map_err(|err| CliError::fatal(format!("failed writing {}: {err}", json_path.display())))?;

    // Write reports/latest.md
    let md_path = loaded.resolved.reports_output_dir.join("latest.md");
    let md_str = build_report_md(run_id, generated_at, &all_signals, &filtered);
    std::fs::write(&md_path, &md_str)
        .map_err(|err| CliError::fatal(format!("failed writing {}: {err}", md_path.display())))?;

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
    run_id: Option<i64>,
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

/// Extract the ticker from a stable_key of the form `external_id:TICKER/section/...`.
fn ticker_from_stable_key(stable_key: &str) -> &str {
    let rest = stable_key
        .strip_prefix("external_id:")
        .unwrap_or(stable_key);
    rest.split('/').next().unwrap_or(stable_key)
}

/// Strip boilerplate from a signal summary, returning just the human-readable content.
///
/// Input forms:
///   "new announcement: external_id:TICKER/SECTION/TYPE|DESCRIPTION"
///   "new announcement: external_id:TICKER/SECTION/N/DTITLE"
///   "new dividend: external_id:TICKER/proventos/MM/YYYY valor=X"
///
/// Output: the meaningful trailing content (after `|` when present, or after `N/D`).
fn compact_summary(summary: &str) -> &str {
    // Strip leading verb and "external_id:" prefix
    let rest = summary
        .split_once("external_id:")
        .map(|(_, r)| r)
        .unwrap_or(summary);

    // Skip TICKER and SECTION (first two '/'-delimited segments), keep the rest.
    // Use splitn(3) so that '/' inside TYPE/DESCRIPTION is not consumed.
    let content = rest.splitn(3, '/').nth(2).unwrap_or(rest);

    // Prefer the part after '|' (the human-readable description).
    // For N/D items there is no '|'; strip the literal "N/D" marker instead.
    if let Some(idx) = content.find('|') {
        &content[idx + 1..]
    } else {
        content.strip_prefix("N/D").unwrap_or(content)
    }
}

fn build_report_md(
    run_id: Option<i64>,
    generated_at: i64,
    all_signals: &[SignalWithItem],
    filtered: &[&SignalWithItem],
) -> String {
    use std::fmt::Write as _;

    let mut md = String::new();
    let run_label = run_id
        .map(|id| format!("run-{id}"))
        .unwrap_or_else(|| "cross-run".to_string());
    let _ = writeln!(
        md,
        "# omens {run_label} · epoch:{generated_at} · {}/{} signals shown",
        filtered.len(),
        all_signals.len()
    );

    if filtered.is_empty() {
        let _ = writeln!(md, "\n_No signals._");
        return md;
    }

    // Group by ticker preserving the existing severity-sorted order.
    let mut groups: Vec<(&str, Vec<&SignalWithItem>)> = Vec::new();
    for sig in filtered {
        let ticker = ticker_from_stable_key(&sig.stable_key);
        if let Some(g) = groups.iter_mut().find(|(t, _)| *t == ticker) {
            g.1.push(sig);
        } else {
            groups.push((ticker, vec![sig]));
        }
    }

    for (ticker, sigs) in &groups {
        let _ = writeln!(md, "\n## {ticker}");
        for sig in sigs {
            let sev = match sig.severity.as_str() {
                "critical" => "CRIT",
                "high" => "H",
                "medium" => "M",
                "low" => "L",
                s => s,
            };
            let desc = compact_summary(&sig.summary);
            let reasons: Vec<String> = sig
                .reasons_json
                .as_deref()
                .and_then(|j| serde_json::from_str(j).ok())
                .unwrap_or_default();
            if reasons.is_empty() {
                let _ = writeln!(md, "- {sev} {:.2} {} {}", sig.confidence, sig.section, desc);
            } else {
                let _ = writeln!(
                    md,
                    "- {sev} {:.2} {} {} _{}_",
                    sig.confidence,
                    sig.section,
                    desc,
                    reasons.join("; ")
                );
            }
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
    use super::{build_report_md, extract_published_at, parse_date_br, parse_since};
    use crate::store::SignalWithItem;

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
        assert_eq!(
            parse_date_br("29/12/2025 10:00:00"),
            parse_date_br("29/12/2025")
        );
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

    // Regression: report markdown must not include a redundant Key/Section line —
    // the summary already embeds the full stable_key, so repeating it wastes tokens.
    #[test]
    fn report_md_grouped_by_ticker_compact() {
        let make_sig = |ticker: &str, summary: &str| SignalWithItem {
            signal_id: 1,
            run_id: 1,
            kind: "new_announcement".into(),
            severity: "high".into(),
            confidence: 0.9,
            reasons_json: Some(r#"["contains 'fato relevante'"]"#.into()),
            summary: summary.into(),
            item_id: 1,
            section: "comunicados".into(),
            stable_key: format!("external_id:{ticker}/comunicados/Fato Relevante"),
            title: None,
            url: Some(format!(
                "https://www.clubefii.com.br/fiis/{ticker}#comunicados"
            )),
            published_at: None,
        };

        let a = make_sig(
            "BRCO11",
            "new announcement: external_id:BRCO11/comunicados/Fato Relevante|Expansão",
        );
        let b = make_sig(
            "HGLG11",
            "new announcement: external_id:HGLG11/comunicados/Fato Relevante|11ª emissão",
        );

        let all = [a.clone(), b.clone()];
        let filtered: Vec<&SignalWithItem> = all.iter().collect();
        let md = build_report_md(Some(1), 0, &all, &filtered);

        // Grouped by ticker
        assert!(md.contains("## BRCO11"), "should have BRCO11 section");
        assert!(md.contains("## HGLG11"), "should have HGLG11 section");

        // Human-readable description present, boilerplate absent
        assert!(md.contains("Expansão"), "description should appear");
        assert!(
            md.contains("11ª emissão"),
            "second description should appear"
        );
        assert!(
            !md.contains("external_id:"),
            "internal id prefix must not appear"
        );
        assert!(
            !md.contains("Key:") && !md.contains("Section:"),
            "redundant key/section lines must not appear"
        );

        // Reasons present
        assert!(md.contains("fato relevante"), "reasons should appear");
    }

    #[test]
    fn compact_summary_strips_boilerplate() {
        use super::compact_summary;

        // Announcement with pipe-separated description
        assert_eq!(
            compact_summary(
                "new announcement: external_id:BRCO11/comunicados/Fato Relevante|Expansão de locação"
            ),
            "Expansão de locação"
        );

        // N/D item (no pipe, bare title after N/D marker)
        assert_eq!(
            compact_summary(
                "new announcement: external_id:BODB11/comunicados/N/DRelatório Gerencial - Janeiro/2026"
            ),
            "Relatório Gerencial - Janeiro/2026"
        );

        // Dividend
        assert_eq!(
            compact_summary("new dividend: external_id:GTWR11/proventos/12/2025 valor=0,900"),
            "12/2025 valor=0,900"
        );

        // Unknown format — returned as-is
        assert_eq!(
            compact_summary("some unknown format"),
            "some unknown format"
        );
    }

    #[test]
    fn content_hash_for_section_excludes_volatile_proventos_fields() {
        use super::content_hash_for_section;

        let full = r#"[["COTAÇÃO DAT. BASE","85,94"],["DATA BASE","30/12/2026"],["TIPO","RENDIMENTO"],["VALOR","0,900"],["YIELD DAT. BASE","1,05 %"]]"#;
        let price_changed = r#"[["COTAÇÃO DAT. BASE","85,45"],["DATA BASE","30/12/2026"],["TIPO","RENDIMENTO"],["VALOR","0,900"],["YIELD DAT. BASE","1,10 %"]]"#;
        let valor_changed = r#"[["COTAÇÃO DAT. BASE","85,94"],["DATA BASE","30/12/2026"],["TIPO","RENDIMENTO"],["VALOR","1,000"],["YIELD DAT. BASE","1,05 %"]]"#;

        // Price-only change should produce the same hash
        assert_eq!(
            content_hash_for_section("proventos", full),
            content_hash_for_section("proventos", price_changed),
        );

        // Actual valor change should produce a different hash
        assert_ne!(
            content_hash_for_section("proventos", full),
            content_hash_for_section("proventos", valor_changed),
        );

        // Non-proventos sections hash the full payload
        assert_ne!(
            content_hash_for_section("comunicados", full),
            content_hash_for_section("comunicados", price_changed),
        );
    }
}
