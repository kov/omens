mod commands;

use crate::browser::harness::ScrollDirection;

pub const EX_FATAL: i32 = 40;
pub const EX_AUTH_REQUIRED: i32 = 20;
pub const EX_LOCK_CONFLICT: i32 = 30;

#[derive(Debug, Clone)]
pub struct CliError {
    pub code: i32,
    pub message: String,
}

impl CliError {
    pub(crate) fn fatal(message: impl Into<String>) -> Self {
        Self {
            code: EX_FATAL,
            message: message.into(),
        }
    }

    pub(crate) fn auth_required(message: impl Into<String>) -> Self {
        Self {
            code: EX_AUTH_REQUIRED,
            message: message.into(),
        }
    }

    pub(crate) fn lock_conflict(message: impl Into<String>) -> Self {
        Self {
            code: EX_LOCK_CONFLICT,
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
        Command::AuthBootstrap {
            ephemeral,
            system_display,
        } => commands::auth_bootstrap(ephemeral, system_display),
        Command::ExploreStart { url } => commands::explore_start(url),
        Command::ExploreReview => commands::explore_review(),
        Command::ExplorePromote { recipe_id } => commands::explore_promote(recipe_id),
        Command::Run => commands::run_all(),
        Command::CollectRun { sections, tickers } => commands::collect_run(sections, tickers),
        Command::ReportLatest => commands::report_latest(),
        Command::ReportSince { since } => commands::report_since(since),
        Command::BrowserStatus => commands::browser_status(),
        Command::BrowserInstall { force } => commands::browser_install(force),
        Command::BrowserUpgrade => commands::browser_upgrade(),
        Command::BrowserRollback => commands::browser_rollback(),
        Command::BrowserOpen {
            url,
            system_display,
            extra_args,
        } => commands::browser_open(url, system_display, extra_args),
        Command::BrowserResetProfile => commands::browser_reset_profile(),
        Command::DisplayStart { listen_addr } => commands::display_start(listen_addr),
        Command::DisplayStop => commands::display_stop(),
        Command::DisplayStatus => commands::display_status(),
        Command::FetchDoc { url_or_key } => commands::fetch_doc(url_or_key),
        Command::SendEmail { path } => commands::send_email(path),
        Command::Chat { system_display } => commands::chat(system_display),
        Command::Browse(sub) => commands::browse(sub),
        Command::ConfigDoctor => commands::config_doctor(),
        Command::Help { topic } => {
            print_usage(topic);
            Ok(())
        }
    }
}

fn print_usage(topic: HelpTopic) {
    match topic {
        HelpTopic::Root => {
            println!(
                "Usage:\n  \
  omens run\n  \
  omens auth bootstrap [--ephemeral] [--system-display]\n  \
  omens explore start <url-or-ticker>\n  \
  omens explore review\n  \
  omens explore promote <recipe_id>\n  \
  omens collect run [--sections csv] [--tickers csv]\n  \
  omens report latest\n  \
  omens report since DATE|Nd\n  \
  omens fetch-doc <url-or-stable-key>\n  \
  omens send-email <file>\n  \
  omens chat [--system-display]\n  \
  omens browse start|stop|status|navigate|content|click|...\n  \
  omens config doctor\n  \
  omens browser open [url] [--system-display] [-- CHROMIUM_ARGS...]\n  \
  omens browser status|install|upgrade|rollback|reset-profile\n  \
  omens display start|stop|status"
            );
        }
        HelpTopic::Auth => {
            println!("Usage:\n  omens auth bootstrap [--ephemeral] [--system-display]")
        }
        HelpTopic::Explore => {
            println!(
                "Usage:\n  omens explore start <url-or-ticker>\n  omens explore review\n  omens explore promote <recipe_id>"
            )
        }
        HelpTopic::Collect => {
            println!("Usage:\n  omens collect run [--sections csv] [--tickers csv]")
        }
        HelpTopic::Report => {
            println!("Usage:\n  omens report latest\n  omens report since DATE|Nd")
        }
        HelpTopic::Chat => println!("Usage:\n  omens chat [--system-display]"),
        HelpTopic::Config => println!("Usage:\n  omens config doctor"),
        HelpTopic::Browse => {
            println!(
                "Usage:\n  \
  omens browse start [--port PORT] [--system-display]\n  \
  omens browse stop\n  \
  omens browse status\n  \
  omens browse navigate <url>\n  \
  omens browse content [--max-chars N] [--full]\n  \
  omens browse links [--contains TEXT] [--max N]\n  \
  omens browse click <selector>\n  \
  omens browse type <selector> <text>\n  \
  omens browse find <selector> [--max N]\n  \
  omens browse scroll <up|down> [pixels]\n  \
  omens browse eval <expression>\n  \
  omens browse source\n  \
  omens browse url"
            )
        }
        HelpTopic::Browser => {
            println!(
                "Usage:\n  omens browser open [url] [--system-display] [-- CHROMIUM_ARGS...]\n  omens browser status|install [--force]|upgrade|rollback|reset-profile"
            )
        }
        HelpTopic::Display => println!(
            "Usage:\n  omens display start [--listen addr:port]\n  omens display stop\n  omens display status"
        ),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HelpTopic {
    Root,
    Auth,
    Explore,
    Collect,
    Report,
    Chat,
    Config,
    Browse,
    Browser,
    Display,
}

enum Command {
    Run,
    AuthBootstrap {
        ephemeral: bool,
        system_display: bool,
    },
    ExploreStart {
        url: String,
    },
    ExploreReview,
    ExplorePromote {
        recipe_id: String,
    },
    CollectRun {
        sections: Option<String>,
        tickers: Option<String>,
    },
    ReportLatest,
    ReportSince {
        since: i64,
    },
    FetchDoc {
        url_or_key: String,
    },
    SendEmail {
        path: String,
    },
    Chat {
        system_display: bool,
    },
    ConfigDoctor,
    BrowserStatus,
    BrowserInstall {
        force: bool,
    },
    BrowserUpgrade,
    BrowserRollback,
    BrowserOpen {
        url: Option<String>,
        system_display: bool,
        extra_args: Vec<String>,
    },
    BrowserResetProfile,
    Browse(BrowseCommand),
    DisplayStart {
        listen_addr: String,
    },
    DisplayStop,
    DisplayStatus,
    Help {
        topic: HelpTopic,
    },
}

#[derive(Debug, Clone)]
pub enum BrowseCommand {
    Start {
        port: u16,
        system_display: bool,
    },
    Stop,
    Status,
    Navigate {
        url: String,
    },
    Content {
        max_chars: u32,
        full: bool,
    },
    Click {
        selector: String,
    },
    Type {
        selector: String,
        text: String,
    },
    Find {
        selector: String,
        max_results: usize,
    },
    Scroll {
        direction: ScrollDirection,
        pixels: u32,
    },
    Eval {
        expression: String,
    },
    Links {
        contains: Option<String>,
        max_results: usize,
    },
    Source,
    Url,
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
            "run" => {
                if args.len() > 2 {
                    return Err("usage: omens run".to_string());
                }
                Ok(Command::Run)
            }
            "auth" => parse_auth(args),
            "explore" => parse_explore(args),
            "collect" => parse_collect(args),
            "report" => parse_report(args),
            "fetch-doc" => parse_fetch_doc(args),
            "send-email" => parse_send_email(args),
            "chat" => parse_chat(args),
            "config" => parse_config(args),
            "browse" => parse_browse(args),
            "browser" => parse_browser(args),
            "display" => parse_display(args),
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
        Some("chat") => Ok(HelpTopic::Chat),
        Some("config") => Ok(HelpTopic::Config),
        Some("browse") => Ok(HelpTopic::Browse),
        Some("browser") => Ok(HelpTopic::Browser),
        Some("display") => Ok(HelpTopic::Display),
        Some(other) => Err(format!("unknown help topic `{other}`")),
    }
}

fn parse_auth(args: &[String]) -> Result<Command, String> {
    if args.len() == 3 && is_help(args[2].as_str()) {
        return Ok(Command::Help {
            topic: HelpTopic::Auth,
        });
    }
    if args.len() >= 3 && args[2] == "bootstrap" {
        let mut ephemeral = false;
        let mut system_display = false;
        for arg in args.iter().skip(3) {
            match arg.as_str() {
                "--ephemeral" => ephemeral = true,
                "--system-display" => system_display = true,
                _ => {
                    return Err(
                        "usage: omens auth bootstrap [--ephemeral] [--system-display]".to_string(),
                    );
                }
            }
        }
        return Ok(Command::AuthBootstrap {
            ephemeral,
            system_display,
        });
    }
    Err("usage: omens auth bootstrap [--ephemeral] [--system-display]".to_string())
}

fn parse_explore(args: &[String]) -> Result<Command, String> {
    if args.len() == 3 && is_help(args[2].as_str()) {
        return Ok(Command::Help {
            topic: HelpTopic::Explore,
        });
    }
    if (args.len() == 3 || args.len() == 4) && args[2] == "start" {
        let url = if args.len() == 4 {
            let arg = &args[3];
            if arg.starts_with("http://") || arg.starts_with("https://") {
                arg.clone()
            } else {
                format!("https://www.clubefii.com.br/fiis/{arg}")
            }
        } else {
            return Err(
                "usage: omens explore start <url-or-ticker>\n  example: omens explore start XPML11"
                    .to_string(),
            );
        };
        return Ok(Command::ExploreStart { url });
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
    if args.len() >= 3 && args[2] == "run" {
        let mut sections = None::<String>;
        let mut tickers = None::<String>;
        let mut i = 3usize;
        while i < args.len() {
            match args[i].as_str() {
                "--sections" => {
                    let val = args
                        .get(i + 1)
                        .ok_or_else(|| "missing value after --sections".to_string())?;
                    sections = Some(val.clone());
                    i += 2;
                }
                "--tickers" => {
                    let val = args
                        .get(i + 1)
                        .ok_or_else(|| "missing value after --tickers".to_string())?;
                    tickers = Some(val.clone());
                    i += 2;
                }
                _ => {
                    return Err(
                        "usage: omens collect run [--sections csv] [--tickers csv]".to_string()
                    );
                }
            }
        }
        return Ok(Command::CollectRun { sections, tickers });
    }
    Err("usage: omens collect run [--sections csv] [--tickers csv]".to_string())
}

fn parse_report(args: &[String]) -> Result<Command, String> {
    if args.len() == 3 && is_help(args[2].as_str()) {
        return Ok(Command::Help {
            topic: HelpTopic::Report,
        });
    }
    if args.len() == 3 && args[2] == "latest" {
        return Ok(Command::ReportLatest);
    }
    if args.len() == 4 && args[2] == "since" {
        let since = commands::parse_since(&args[3])?;
        return Ok(Command::ReportSince { since });
    }
    Err("usage: omens report latest\n       omens report since DATE|Nd".to_string())
}

fn parse_fetch_doc(args: &[String]) -> Result<Command, String> {
    if args.len() == 3 && is_help(args[2].as_str()) {
        return Ok(Command::Help {
            topic: HelpTopic::Root,
        });
    }
    if args.len() == 3 {
        return Ok(Command::FetchDoc {
            url_or_key: args[2].clone(),
        });
    }
    Err("usage: omens fetch-doc <url-or-stable-key>".to_string())
}

fn parse_send_email(args: &[String]) -> Result<Command, String> {
    match args.get(2).map(|s| s.as_str()) {
        Some(path) => Ok(Command::SendEmail {
            path: path.to_string(),
        }),
        None => Err("usage: omens send-email <file>".to_string()),
    }
}

fn parse_chat(args: &[String]) -> Result<Command, String> {
    if args.len() == 3 && is_help(args[2].as_str()) {
        return Ok(Command::Help {
            topic: HelpTopic::Chat,
        });
    }
    let mut system_display = false;
    for arg in args.iter().skip(2) {
        match arg.as_str() {
            "--system-display" => system_display = true,
            _ => return Err("usage: omens chat [--system-display]".to_string()),
        }
    }
    Ok(Command::Chat { system_display })
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

fn parse_browse(args: &[String]) -> Result<Command, String> {
    if args.len() == 3 && is_help(args[2].as_str()) {
        return Ok(Command::Help {
            topic: HelpTopic::Browse,
        });
    }

    if args.len() < 3 {
        return Err("usage: omens browse <subcommand>. run `omens browse --help`".to_string());
    }

    match args[2].as_str() {
        "start" => {
            let mut port = 9222u16;
            let mut system_display = false;
            let mut i = 3usize;
            while i < args.len() {
                match args[i].as_str() {
                    "--port" => {
                        let val = args
                            .get(i + 1)
                            .ok_or_else(|| "missing value after --port".to_string())?;
                        port = val
                            .parse::<u16>()
                            .map_err(|_| format!("invalid port: {val}"))?;
                        i += 2;
                    }
                    "--system-display" => {
                        system_display = true;
                        i += 1;
                    }
                    _ => {
                        return Err("usage: omens browse start [--port PORT] [--system-display]"
                            .to_string());
                    }
                }
            }
            Ok(Command::Browse(BrowseCommand::Start {
                port,
                system_display,
            }))
        }
        "stop" => {
            if args.len() != 3 {
                return Err("usage: omens browse stop".to_string());
            }
            Ok(Command::Browse(BrowseCommand::Stop))
        }
        "status" => {
            if args.len() != 3 {
                return Err("usage: omens browse status".to_string());
            }
            Ok(Command::Browse(BrowseCommand::Status))
        }
        "navigate" => {
            if args.len() != 4 {
                return Err("usage: omens browse navigate <url>".to_string());
            }
            Ok(Command::Browse(BrowseCommand::Navigate {
                url: args[3].clone(),
            }))
        }
        "content" => {
            let mut max_chars = 0u32; // 0 = use default from config
            let mut full = false;
            let mut i = 3usize;
            while i < args.len() {
                match args[i].as_str() {
                    "--max-chars" => {
                        let val = args
                            .get(i + 1)
                            .ok_or_else(|| "missing value after --max-chars".to_string())?;
                        max_chars = val
                            .parse::<u32>()
                            .map_err(|_| format!("invalid --max-chars value: {val}"))?;
                        i += 2;
                    }
                    "--full" => {
                        full = true;
                        i += 1;
                    }
                    _ => {
                        return Err(
                            "usage: omens browse content [--max-chars N] [--full]".to_string()
                        );
                    }
                }
            }
            Ok(Command::Browse(BrowseCommand::Content { max_chars, full }))
        }
        "click" => {
            if args.len() != 4 {
                return Err("usage: omens browse click <selector>".to_string());
            }
            Ok(Command::Browse(BrowseCommand::Click {
                selector: args[3].clone(),
            }))
        }
        "type" => {
            if args.len() != 5 {
                return Err("usage: omens browse type <selector> <text>".to_string());
            }
            Ok(Command::Browse(BrowseCommand::Type {
                selector: args[3].clone(),
                text: args[4].clone(),
            }))
        }
        "find" => {
            if args.len() < 4 {
                return Err("usage: omens browse find <selector> [--max N]".to_string());
            }
            let selector = args[3].clone();
            let mut max_results = 20usize;
            let mut i = 4usize;
            while i < args.len() {
                match args[i].as_str() {
                    "--max" => {
                        let val = args
                            .get(i + 1)
                            .ok_or_else(|| "missing value after --max".to_string())?;
                        max_results = val
                            .parse::<usize>()
                            .map_err(|_| format!("invalid --max value: {val}"))?;
                        i += 2;
                    }
                    _ => return Err("usage: omens browse find <selector> [--max N]".to_string()),
                }
            }
            Ok(Command::Browse(BrowseCommand::Find {
                selector,
                max_results,
            }))
        }
        "scroll" => {
            if args.len() < 4 || args.len() > 5 {
                return Err("usage: omens browse scroll <up|down> [pixels]".to_string());
            }
            let direction = ScrollDirection::parse(&args[3])?;
            let pixels = if args.len() == 5 {
                args[4]
                    .parse::<u32>()
                    .map_err(|_| format!("invalid pixels value: {}", args[4]))?
            } else {
                600
            };
            Ok(Command::Browse(BrowseCommand::Scroll { direction, pixels }))
        }
        "eval" => {
            if args.len() != 4 {
                return Err("usage: omens browse eval <expression>".to_string());
            }
            Ok(Command::Browse(BrowseCommand::Eval {
                expression: args[3].clone(),
            }))
        }
        "links" => {
            let mut contains = None::<String>;
            let mut max_results = 200usize;
            let mut i = 3usize;
            while i < args.len() {
                match args[i].as_str() {
                    "--contains" => {
                        let val = args
                            .get(i + 1)
                            .ok_or_else(|| "missing value after --contains".to_string())?;
                        contains = Some(val.clone());
                        i += 2;
                    }
                    "--max" => {
                        let val = args
                            .get(i + 1)
                            .ok_or_else(|| "missing value after --max".to_string())?;
                        max_results = val
                            .parse::<usize>()
                            .map_err(|_| format!("invalid --max value: {val}"))?;
                        i += 2;
                    }
                    _ => {
                        return Err(
                            "usage: omens browse links [--contains TEXT] [--max N]".to_string()
                        );
                    }
                }
            }
            Ok(Command::Browse(BrowseCommand::Links {
                contains,
                max_results,
            }))
        }
        "source" => {
            if args.len() != 3 {
                return Err("usage: omens browse source".to_string());
            }
            Ok(Command::Browse(BrowseCommand::Source))
        }
        "url" => {
            if args.len() != 3 {
                return Err("usage: omens browse url".to_string());
            }
            Ok(Command::Browse(BrowseCommand::Url))
        }
        _ => Err("unknown browse subcommand. run `omens browse --help`".to_string()),
    }
}

fn parse_browser(args: &[String]) -> Result<Command, String> {
    if args.len() == 3 && is_help(args[2].as_str()) {
        return Ok(Command::Help {
            topic: HelpTopic::Browser,
        });
    }

    if args.len() >= 3 && args[2] == "open" {
        let mut url = None;
        let mut system_display = false;
        let mut extra_args = Vec::new();
        let mut after_dashdash = false;
        for arg in args.iter().skip(3) {
            if after_dashdash {
                extra_args.push(arg.clone());
                continue;
            }
            match arg.as_str() {
                "--" => after_dashdash = true,
                "--system-display" => system_display = true,
                _ if url.is_none() && !arg.starts_with('-') => url = Some(arg.clone()),
                _ => {
                    return Err(
                        "usage: omens browser open [url] [--system-display] [-- CHROMIUM_ARGS...]"
                            .to_string(),
                    );
                }
            }
        }
        return Ok(Command::BrowserOpen {
            url,
            system_display,
            extra_args,
        });
    }

    if args.len() >= 3 && args[2] == "install" {
        let mut force = false;
        for arg in args.iter().skip(3) {
            match arg.as_str() {
                "--force" => force = true,
                _ => return Err("usage: omens browser install [--force]".to_string()),
            }
        }
        return Ok(Command::BrowserInstall { force });
    }

    if args.len() != 3 {
        return Err(
            "usage: omens browser status|install|upgrade|rollback|reset-profile".to_string(),
        );
    }

    match args[2].as_str() {
        "status" => Ok(Command::BrowserStatus),
        "upgrade" => Ok(Command::BrowserUpgrade),
        "rollback" => Ok(Command::BrowserRollback),
        "reset-profile" => Ok(Command::BrowserResetProfile),
        _ => Err("usage: omens browser status|install|upgrade|rollback|reset-profile".to_string()),
    }
}

fn parse_display(args: &[String]) -> Result<Command, String> {
    if args.len() == 3 && is_help(args[2].as_str()) {
        return Ok(Command::Help {
            topic: HelpTopic::Display,
        });
    }
    if args.len() == 3 && args[2] == "stop" {
        return Ok(Command::DisplayStop);
    }
    if args.len() == 3 && args[2] == "status" {
        return Ok(Command::DisplayStatus);
    }
    if args.len() >= 3 && args[2] == "start" {
        let mut listen_addr = "127.0.0.1:3389".to_string();
        let mut i = 3usize;
        while i < args.len() {
            match args[i].as_str() {
                "--listen" => {
                    let value = args
                        .get(i + 1)
                        .ok_or_else(|| "missing value after --listen".to_string())?;
                    listen_addr = value.clone();
                    i += 2;
                }
                _ => {
                    return Err("usage: omens display start [--listen addr:port]".to_string());
                }
            }
        }
        return Ok(Command::DisplayStart { listen_addr });
    }

    Err("usage: omens display start|stop|status".to_string())
}

fn is_help(value: &str) -> bool {
    value == "--help" || value == "-h"
}

#[cfg(test)]
mod tests {
    use super::commands::map_auth_error;
    use super::{Command, EX_AUTH_REQUIRED, HelpTopic};
    use crate::auth::AuthError;

    fn to_args(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|p| p.to_string()).collect()
    }

    #[test]
    fn parse_run_command() {
        assert!(matches!(
            Command::parse(&to_args(&["omens", "run"])).expect("should parse"),
            Command::Run
        ));
    }

    #[test]
    fn parse_run_rejects_unknown_flags() {
        assert!(Command::parse(&to_args(&["omens", "run", "--since", "30d"])).is_err());
        assert!(Command::parse(&to_args(&["omens", "run", "extra"])).is_err());
    }

    #[test]
    fn parse_report_latest() {
        assert!(matches!(
            Command::parse(&to_args(&["omens", "report", "latest"])).expect("should parse"),
            Command::ReportLatest
        ));
    }

    #[test]
    fn parse_report_since_iso() {
        let cmd = Command::parse(&to_args(&["omens", "report", "since", "2023-08-31"]))
            .expect("report since should parse");
        match cmd {
            Command::ReportSince { since } => assert_eq!(since, 1693440000),
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn parse_report_since_days() {
        let cmd = Command::parse(&to_args(&["omens", "report", "since", "30d"]))
            .expect("report since 30d should parse");
        assert!(matches!(cmd, Command::ReportSince { .. }));
    }

    #[test]
    fn parse_report_latest_rejects_extra_args() {
        assert!(
            Command::parse(&to_args(&["omens", "report", "latest", "--since", "30d"])).is_err()
        );
        assert!(Command::parse(&to_args(&["omens", "report", "latest", "extra"])).is_err());
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
            Command::AuthBootstrap {
                ephemeral: true,
                system_display: false
            }
        ));
    }

    #[test]
    fn parse_auth_system_display_flag() {
        assert!(matches!(
            Command::parse(&to_args(&[
                "omens",
                "auth",
                "bootstrap",
                "--system-display"
            ]))
            .expect("auth should parse"),
            Command::AuthBootstrap {
                ephemeral: false,
                system_display: true
            }
        ));
    }

    #[test]
    fn parse_collect_sections_flag() {
        let command = Command::parse(&to_args(&["omens", "collect", "run", "--sections", "news"]))
            .expect("collect run should parse");

        match command {
            Command::CollectRun { sections, .. } => {
                assert_eq!(sections, Some("news".to_string()))
            }
            _ => panic!("unexpected command variant"),
        }
    }

    #[test]
    fn parse_collect_tickers_flag() {
        let command = Command::parse(&to_args(&[
            "omens",
            "collect",
            "run",
            "--tickers",
            "BRCR11,RBRX11",
        ]))
        .expect("collect run with --tickers should parse");

        match command {
            Command::CollectRun { tickers, .. } => {
                assert_eq!(tickers, Some("BRCR11,RBRX11".to_string()))
            }
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
    fn parse_display_start_with_options() {
        let command = Command::parse(&to_args(&[
            "omens",
            "display",
            "start",
            "--listen",
            "0.0.0.0:3389",
        ]))
        .expect("display start should parse");
        match command {
            Command::DisplayStart { listen_addr } => {
                assert_eq!(listen_addr, "0.0.0.0:3389");
            }
            _ => panic!("unexpected command variant"),
        }
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
    fn parse_browser_open_no_args() {
        assert!(matches!(
            Command::parse(&to_args(&["omens", "browser", "open"])).expect("should parse"),
            Command::BrowserOpen {
                url: None,
                system_display: false,
                ..
            }
        ));
    }

    #[test]
    fn parse_browser_open_with_url() {
        let cmd = Command::parse(&to_args(&[
            "omens",
            "browser",
            "open",
            "https://example.com",
        ]))
        .expect("should parse");
        match cmd {
            Command::BrowserOpen {
                url,
                system_display,
                ..
            } => {
                assert_eq!(url, Some("https://example.com".to_string()));
                assert!(!system_display);
            }
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn parse_browser_open_with_system_display() {
        assert!(matches!(
            Command::parse(&to_args(&["omens", "browser", "open", "--system-display"]))
                .expect("should parse"),
            Command::BrowserOpen {
                url: None,
                system_display: true,
                ..
            }
        ));
    }

    #[test]
    fn parse_browser_open_url_and_display() {
        let cmd = Command::parse(&to_args(&[
            "omens",
            "browser",
            "open",
            "https://example.com",
            "--system-display",
        ]))
        .expect("should parse");
        match cmd {
            Command::BrowserOpen {
                url,
                system_display,
                ..
            } => {
                assert_eq!(url, Some("https://example.com".to_string()));
                assert!(system_display);
            }
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn auth_error_maps_to_exit_code_20() {
        let err = map_auth_error(AuthError::AuthRequired("login".to_string()));
        assert_eq!(err.code, EX_AUTH_REQUIRED);
    }
}
