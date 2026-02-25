use crate::auth::{self, AuthError, AuthValidationConfig, EphemeralProfile};
use crate::browser::harness::{BrowserHarness, ChromiumoxideHarness};
use crate::config::{self, DoctorIssueSeverity, OmensConfig};
use crate::runtime::browser_manager::{BrowserInstallState, BrowserManager, BrowserMode};
use std::io;
use std::time::{Duration, SystemTime};

pub const EX_FATAL: i32 = 40;
pub const EX_AUTH_REQUIRED: i32 = 20;

#[derive(Debug, Clone)]
pub struct CliError {
    pub code: i32,
    pub message: String,
}

impl CliError {
    fn fatal(message: impl Into<String>) -> Self {
        Self {
            code: EX_FATAL,
            message: message.into(),
        }
    }

    fn auth_required(message: impl Into<String>) -> Self {
        Self {
            code: EX_AUTH_REQUIRED,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

pub fn run(args: &[String]) -> Result<(), CliError> {
    let command = Command::parse(args).map_err(CliError::fatal)?;

    match command {
        Command::AuthBootstrap { ephemeral } => auth_bootstrap(ephemeral),
        Command::ExploreStart => noop("explore start"),
        Command::ExploreReview => noop("explore review"),
        Command::ExplorePromote { recipe_id } => noop(&format!("explore promote {recipe_id}")),
        Command::CollectRun { sections } => noop(&format!("collect run --sections {sections}")),
        Command::ReportLatest => noop("report latest"),
        Command::BrowserStatus => browser_status(),
        Command::BrowserInstall => browser_install(),
        Command::BrowserUpgrade => browser_upgrade(),
        Command::BrowserRollback => browser_rollback(),
        Command::BrowserResetProfile => browser_reset_profile(),
        Command::ConfigDoctor => config_doctor(),
        Command::Help { topic } => {
            print_usage(topic);
            Ok(())
        }
    }
}

fn noop(name: &str) -> Result<(), CliError> {
    println!("{name}: not implemented yet");
    Ok(())
}

fn auth_bootstrap(ephemeral: bool) -> Result<(), CliError> {
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

    let mut harness =
        ChromiumoxideHarness::new(browser_binary, profile_path.clone()).map_err(CliError::fatal)?;
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

fn config_doctor() -> Result<(), CliError> {
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

fn browser_status() -> Result<(), CliError> {
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

fn browser_install() -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    config::bootstrap_layout(&loaded).map_err(CliError::fatal)?;
    let manager = BrowserManager::from_config(&loaded).map_err(CliError::fatal)?;
    let status = manager.install().map_err(CliError::fatal)?;
    print_browser_status_result("browser install", &status);
    Ok(())
}

fn browser_upgrade() -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    config::bootstrap_layout(&loaded).map_err(CliError::fatal)?;
    let manager = BrowserManager::from_config(&loaded).map_err(CliError::fatal)?;
    let status = manager.upgrade().map_err(CliError::fatal)?;
    print_browser_status_result("browser upgrade", &status);
    Ok(())
}

fn browser_rollback() -> Result<(), CliError> {
    let loaded = config::load_default_config().map_err(CliError::fatal)?;
    config::bootstrap_layout(&loaded).map_err(CliError::fatal)?;
    let manager = BrowserManager::from_config(&loaded).map_err(CliError::fatal)?;
    let status = manager.rollback().map_err(CliError::fatal)?;
    print_browser_status_result("browser rollback", &status);
    Ok(())
}

fn browser_reset_profile() -> Result<(), CliError> {
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

fn map_auth_error(err: AuthError) -> CliError {
    match err {
        AuthError::AuthRequired(msg) => CliError::auth_required(msg),
        AuthError::Runtime(msg) => CliError::fatal(msg),
    }
}

fn print_usage(topic: HelpTopic) {
    match topic {
        HelpTopic::Root => {
            println!(
                "Usage:\n  \
  omens auth bootstrap [--ephemeral]\n  \
  omens explore start\n  \
  omens explore review\n  \
  omens explore promote <recipe_id>\n  \
  omens collect run [--sections csv]\n  \
  omens report latest\n  \
  omens config doctor\n  \
  omens browser status|install|upgrade|rollback|reset-profile"
            );
        }
        HelpTopic::Auth => println!("Usage:\n  omens auth bootstrap [--ephemeral]"),
        HelpTopic::Explore => {
            println!(
                "Usage:\n  omens explore start\n  omens explore review\n  omens explore promote <recipe_id>"
            )
        }
        HelpTopic::Collect => println!("Usage:\n  omens collect run [--sections csv]"),
        HelpTopic::Report => println!("Usage:\n  omens report latest"),
        HelpTopic::Config => println!("Usage:\n  omens config doctor"),
        HelpTopic::Browser => {
            println!("Usage:\n  omens browser status|install|upgrade|rollback|reset-profile")
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HelpTopic {
    Root,
    Auth,
    Explore,
    Collect,
    Report,
    Config,
    Browser,
}

enum Command {
    AuthBootstrap { ephemeral: bool },
    ExploreStart,
    ExploreReview,
    ExplorePromote { recipe_id: String },
    CollectRun { sections: String },
    ReportLatest,
    ConfigDoctor,
    BrowserStatus,
    BrowserInstall,
    BrowserUpgrade,
    BrowserRollback,
    BrowserResetProfile,
    Help { topic: HelpTopic },
}

impl Command {
    fn parse(args: &[String]) -> Result<Self, String> {
        if args.len() <= 1 {
            return Ok(Self::Help {
                topic: HelpTopic::Root,
            });
        }

        if is_help(args[1].as_str()) {
            return Ok(Self::Help {
                topic: HelpTopic::Root,
            });
        }

        if args[1] == "help" {
            return Ok(Self::Help {
                topic: parse_help_topic(args.get(2).map(String::as_str))?,
            });
        }

        match args[1].as_str() {
            "auth" => parse_auth(args),
            "explore" => parse_explore(args),
            "collect" => parse_collect(args),
            "report" => parse_report(args),
            "config" => parse_config(args),
            "browser" => parse_browser(args),
            _ => Err("unknown command. run `omens --help`".to_string()),
        }
    }
}

fn parse_help_topic(raw: Option<&str>) -> Result<HelpTopic, String> {
    match raw {
        None => Ok(HelpTopic::Root),
        Some("auth") => Ok(HelpTopic::Auth),
        Some("explore") => Ok(HelpTopic::Explore),
        Some("collect") => Ok(HelpTopic::Collect),
        Some("report") => Ok(HelpTopic::Report),
        Some("config") => Ok(HelpTopic::Config),
        Some("browser") => Ok(HelpTopic::Browser),
        Some(other) => Err(format!("unknown help topic `{other}`")),
    }
}

fn parse_auth(args: &[String]) -> Result<Command, String> {
    if args.len() == 3 && is_help(args[2].as_str()) {
        return Ok(Command::Help {
            topic: HelpTopic::Auth,
        });
    }
    if args.len() == 3 && args[2] == "bootstrap" {
        return Ok(Command::AuthBootstrap { ephemeral: false });
    }
    if args.len() == 4 && args[2] == "bootstrap" && args[3] == "--ephemeral" {
        return Ok(Command::AuthBootstrap { ephemeral: true });
    }
    Err("usage: omens auth bootstrap [--ephemeral]".to_string())
}

fn parse_explore(args: &[String]) -> Result<Command, String> {
    if args.len() == 3 && is_help(args[2].as_str()) {
        return Ok(Command::Help {
            topic: HelpTopic::Explore,
        });
    }
    if args.len() == 3 && args[2] == "start" {
        return Ok(Command::ExploreStart);
    }
    if args.len() == 3 && args[2] == "review" {
        return Ok(Command::ExploreReview);
    }
    if args.len() == 4 && args[2] == "promote" {
        return Ok(Command::ExplorePromote {
            recipe_id: args[3].clone(),
        });
    }
    Err("usage: omens explore start|review|promote <recipe_id>".to_string())
}

fn parse_collect(args: &[String]) -> Result<Command, String> {
    if args.len() == 3 && is_help(args[2].as_str()) {
        return Ok(Command::Help {
            topic: HelpTopic::Collect,
        });
    }
    if args.len() == 3 && args[2] == "run" {
        return Ok(Command::CollectRun {
            sections: "news,material-facts".to_string(),
        });
    }
    if args.len() == 5 && args[2] == "run" && args[3] == "--sections" {
        return Ok(Command::CollectRun {
            sections: args[4].clone(),
        });
    }
    Err("usage: omens collect run [--sections csv]".to_string())
}

fn parse_report(args: &[String]) -> Result<Command, String> {
    if args.len() == 3 && args[2] == "latest" {
        return Ok(Command::ReportLatest);
    }
    if args.len() == 3 && is_help(args[2].as_str()) {
        return Ok(Command::Help {
            topic: HelpTopic::Report,
        });
    }
    Err("usage: omens report latest".to_string())
}

fn parse_config(args: &[String]) -> Result<Command, String> {
    if args.len() == 3 && args[2] == "doctor" {
        return Ok(Command::ConfigDoctor);
    }
    if args.len() == 3 && is_help(args[2].as_str()) {
        return Ok(Command::Help {
            topic: HelpTopic::Config,
        });
    }
    Err("usage: omens config doctor".to_string())
}

fn parse_browser(args: &[String]) -> Result<Command, String> {
    if args.len() == 3 && is_help(args[2].as_str()) {
        return Ok(Command::Help {
            topic: HelpTopic::Browser,
        });
    }
    if args.len() != 3 {
        return Err(
            "usage: omens browser status|install|upgrade|rollback|reset-profile".to_string(),
        );
    }

    match args[2].as_str() {
        "status" => Ok(Command::BrowserStatus),
        "install" => Ok(Command::BrowserInstall),
        "upgrade" => Ok(Command::BrowserUpgrade),
        "rollback" => Ok(Command::BrowserRollback),
        "reset-profile" => Ok(Command::BrowserResetProfile),
        _ => Err("usage: omens browser status|install|upgrade|rollback|reset-profile".to_string()),
    }
}

fn is_help(value: &str) -> bool {
    value == "--help" || value == "-h"
}

#[cfg(test)]
mod tests {
    use super::{Command, EX_AUTH_REQUIRED, HelpTopic, map_auth_error};
    use crate::auth::AuthError;

    fn to_args(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|p| p.to_string()).collect()
    }

    #[test]
    fn parse_known_commands() {
        assert!(matches!(
            Command::parse(&to_args(&["omens", "config", "doctor"])).expect("should parse"),
            Command::ConfigDoctor
        ));

        assert!(matches!(
            Command::parse(&to_args(&["omens", "browser", "status"])).expect("should parse"),
            Command::BrowserStatus
        ));
    }

    #[test]
    fn parse_auth_ephemeral_flag() {
        assert!(matches!(
            Command::parse(&to_args(&["omens", "auth", "bootstrap", "--ephemeral"]))
                .expect("auth should parse"),
            Command::AuthBootstrap { ephemeral: true }
        ));
    }

    #[test]
    fn parse_collect_sections_flag() {
        let command = Command::parse(&to_args(&["omens", "collect", "run", "--sections", "news"]))
            .expect("collect run should parse");

        match command {
            Command::CollectRun { sections } => assert_eq!(sections, "news"),
            _ => panic!("unexpected command variant"),
        }
    }

    #[test]
    fn parse_group_help() {
        assert!(matches!(
            Command::parse(&to_args(&["omens", "browser", "--help"])).expect("should parse"),
            Command::Help {
                topic: HelpTopic::Browser
            }
        ));
    }

    #[test]
    fn parse_top_level_help() {
        assert!(matches!(
            Command::parse(&to_args(&["omens", "--help"])).expect("should parse"),
            Command::Help {
                topic: HelpTopic::Root
            }
        ));
    }

    #[test]
    fn parse_unknown_command() {
        let result = Command::parse(&to_args(&["omens", "nope"]));
        assert!(result.is_err());
    }

    #[test]
    fn parse_malformed_command_errors() {
        let collect = Command::parse(&to_args(&["omens", "collect", "run", "--sections"]));
        assert!(collect.is_err());

        let auth = Command::parse(&to_args(&["omens", "auth", "bootstrap", "bad"]));
        assert!(auth.is_err());
    }

    #[test]
    fn auth_error_maps_to_exit_code_20() {
        let err = map_auth_error(AuthError::AuthRequired("login".to_string()));
        assert_eq!(err.code, EX_AUTH_REQUIRED);
    }
}
