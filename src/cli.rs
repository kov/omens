use crate::config::{self, DoctorIssueSeverity, OmensConfig};
use crate::runtime::browser_manager::{BrowserManager, BrowserMode};
use std::time::SystemTime;

pub fn run(args: &[String]) -> Result<(), String> {
    let command = Command::parse(args)?;

    match command {
        Command::AuthBootstrap => noop("auth bootstrap"),
        Command::ExploreStart => noop("explore start"),
        Command::ExploreReview => noop("explore review"),
        Command::ExplorePromote { recipe_id } => noop(&format!("explore promote {recipe_id}")),
        Command::CollectRun { sections } => noop(&format!("collect run --sections {sections}")),
        Command::ReportLatest => noop("report latest"),
        Command::BrowserStatus => browser_status(),
        Command::BrowserInstall => noop("browser install"),
        Command::BrowserUpgrade => noop("browser upgrade"),
        Command::BrowserRollback => noop("browser rollback"),
        Command::BrowserResetProfile => noop("browser reset-profile"),
        Command::ConfigDoctor => config_doctor(),
        Command::Help { topic } => {
            print_usage(topic);
            Ok(())
        }
    }
}

fn noop(name: &str) -> Result<(), String> {
    println!("{name}: not implemented yet");
    Ok(())
}

fn config_doctor() -> Result<(), String> {
    let loaded = config::load_default_config()?;
    config::bootstrap_layout(&loaded)?;

    print_config(&loaded);

    let report = config::run_doctor_checks(&loaded, SystemTime::now());
    for issue in report.issues {
        match issue.severity {
            DoctorIssueSeverity::Warning => println!("warning: {}", issue.message),
            DoctorIssueSeverity::Error => println!("error: {}", issue.message),
        }
    }

    if report.error_count > 0 {
        return Err(format!(
            "config doctor found {} error(s)",
            report.error_count
        ));
    }

    println!(
        "config doctor completed (warnings: {}, errors: {})",
        report.warning_count, report.error_count
    );
    Ok(())
}

fn browser_status() -> Result<(), String> {
    let loaded = config::load_default_config()?;
    config::bootstrap_layout(&loaded)?;

    let manager = BrowserManager::from_config(&loaded)?;
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

fn print_usage(topic: HelpTopic) {
    match topic {
        HelpTopic::Root => {
            println!(
                "Usage:\n  \
  omens auth bootstrap\n  \
  omens explore start\n  \
  omens explore review\n  \
  omens explore promote <recipe_id>\n  \
  omens collect run [--sections csv]\n  \
  omens report latest\n  \
  omens config doctor\n  \
  omens browser status|install|upgrade|rollback|reset-profile"
            );
        }
        HelpTopic::Auth => println!("Usage:\n  omens auth bootstrap"),
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

#[derive(Debug, Clone, Copy)]
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
    AuthBootstrap,
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
    if args.len() == 3 && args[2] == "bootstrap" {
        return Ok(Command::AuthBootstrap);
    }
    if args.len() == 3 && is_help(args[2].as_str()) {
        return Ok(Command::Help {
            topic: HelpTopic::Auth,
        });
    }
    Err("usage: omens auth bootstrap".to_string())
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
    use super::{Command, HelpTopic};

    fn to_args(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|p| p.to_string()).collect()
    }

    #[test]
    fn parse_known_commands() {
        assert!(matches!(
            Command::parse(&to_args(&["omens", "config", "doctor"]))
                .expect("config doctor should parse"),
            Command::ConfigDoctor
        ));

        assert!(matches!(
            Command::parse(&to_args(&["omens", "browser", "status"]))
                .expect("browser status should parse"),
            Command::BrowserStatus
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
            Command::parse(&to_args(&["omens", "browser", "--help"]))
                .expect("group help should parse"),
            Command::Help {
                topic: HelpTopic::Browser
            }
        ));
    }

    #[test]
    fn parse_top_level_help() {
        assert!(matches!(
            Command::parse(&to_args(&["omens", "--help"])).expect("help should parse"),
            Command::Help {
                topic: HelpTopic::Root
            }
        ));

        assert!(matches!(
            Command::parse(&to_args(&["omens", "help", "collect"])).expect("help should parse"),
            Command::Help {
                topic: HelpTopic::Collect
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

        let auth = Command::parse(&to_args(&["omens", "auth", "bootstrap", "extra"]));
        assert!(auth.is_err());
    }
}
