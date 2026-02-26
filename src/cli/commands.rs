use crate::auth::{self, AuthError, AuthValidationConfig, EphemeralProfile};
use crate::browser::harness::{BrowserHarness, ChromiumoxideHarness};
use crate::config::{self, DoctorIssueSeverity, OmensConfig};
use crate::runtime::browser_manager::{BrowserInstallState, BrowserManager, BrowserMode};
use crate::runtime::display_manager::DisplayManager;
use crate::store::{self, LockError, RunStatus, Store};
use std::io;
use std::time::{Duration, SystemTime};

use super::CliError;

pub fn noop(name: &str) -> Result<(), CliError> {
    println!("{name}: not implemented yet");
    Ok(())
}

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

pub fn collect_run(sections: String) -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    config::bootstrap_layout(&loaded).map_err(CliError::fatal)?;

    let _lock = match store::acquire_collect_lock(&loaded.resolved.storage_lock_path) {
        Ok(lock) => lock,
        Err(LockError::Contended(message)) => return Err(CliError::lock_conflict(message)),
        Err(LockError::Runtime(message)) => return Err(CliError::fatal(message)),
    };

    let store = Store::open(&loaded.resolved.storage_db_path).map_err(CliError::fatal)?;
    store.migrate().map_err(CliError::fatal)?;

    let started = store::now_epoch_seconds().map_err(CliError::fatal)?;
    let run_id = store
        .start_run(sections.as_str(), started)
        .map_err(CliError::fatal)?;

    let collect_result: Result<(), String> = Ok(());
    let ended = store::now_epoch_seconds().map_err(CliError::fatal)?;
    match collect_result {
        Ok(()) => store
            .finish_run(run_id, RunStatus::Success, ended, None)
            .map_err(CliError::fatal)?,
        Err(err) => {
            let _ = store.finish_run(run_id, RunStatus::Failed, ended, Some(err.as_str()));
            return Err(CliError::fatal(err));
        }
    }
    let persisted = store
        .run_row(run_id)
        .map_err(CliError::fatal)?
        .ok_or_else(|| CliError::fatal(format!("run row {run_id} not found after finalize")))?;
    let retention = store
        .build_retention_plan(
            ended,
            loaded.storage.retention.keep_runs_days,
            loaded.storage.retention.keep_versions_per_item,
        )
        .map_err(CliError::fatal)?;

    println!("collect run");
    println!("  run_id: {run_id}");
    println!("  sections: {sections}");
    println!("  db_path: {}", loaded.resolved.storage_db_path.display());
    println!("  status: success");
    println!("  persisted_status: {}", persisted.0);
    println!(
        "  retention_candidates: runs={}, versions={}",
        retention.run_ids_to_delete.len(),
        retention.version_ids_to_delete.len()
    );
    println!("  note: collection pipeline is not wired yet; run record persisted");
    Ok(())
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

pub fn map_auth_error(err: AuthError) -> CliError {
    match err {
        AuthError::AuthRequired(msg) => CliError::auth_required(msg),
        AuthError::Runtime(msg) => CliError::fatal(msg),
    }
}
