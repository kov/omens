use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use serde::Deserialize;

mod defaults {
    pub fn clubefii_base_url() -> String {
        "https://www.clubefii.com.br".to_string()
    }
    pub fn clubefii_login_url() -> String {
        "https://www.clubefii.com.br/login".to_string()
    }
    pub fn browser_mode() -> String {
        "system".to_string()
    }
    pub fn max_pages_per_section() -> u32 {
        20
    }
    pub fn keep_runs_days() -> u32 {
        180
    }
    pub fn keep_versions_per_item() -> u32 {
        20
    }
    pub fn lmstudio_enabled() -> bool {
        true
    }
    pub fn lmstudio_base_url() -> String {
        "http://127.0.0.1:1234/v1".to_string()
    }
    pub fn lmstudio_max_input_chars() -> u32 {
        12000
    }
    pub fn high_impact() -> f64 {
        0.8
    }
    pub fn low_confidence() -> f64 {
        0.3
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct OmensConfig {
    #[serde(default)]
    pub clubefii: ClubeFiiConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub browser: BrowserConfig,
    #[serde(default)]
    pub collector: CollectorConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub analysis: AnalysisConfig,
    #[serde(default)]
    pub reports: ReportsConfig,
    #[serde(skip)]
    pub resolved: ResolvedPaths,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClubeFiiConfig {
    #[serde(default = "defaults::clubefii_base_url")]
    pub base_url: String,
    #[serde(default = "defaults::clubefii_login_url")]
    pub login_url: String,
    #[serde(default)]
    pub auth_marker: Option<String>,
    #[serde(default)]
    pub protected_probe_url: Option<String>,
}

impl Default for ClubeFiiConfig {
    fn default() -> Self {
        Self {
            base_url: defaults::clubefii_base_url(),
            login_url: defaults::clubefii_login_url(),
            auth_marker: None,
            protected_probe_url: None,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default)]
    pub root_dir: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BrowserConfig {
    #[serde(default = "defaults::browser_mode")]
    pub mode: String,
    #[serde(default)]
    pub system_binary_path: Option<String>,
    #[serde(default)]
    pub bundled_build: u64,
    #[serde(default)]
    pub user_data_dir: Option<String>,
}

impl Default for BrowserConfig {
    fn default() -> Self {
        Self {
            mode: defaults::browser_mode(),
            system_binary_path: None,
            bundled_build: 0,
            user_data_dir: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CollectorConfig {
    #[serde(default)]
    pub tickers: Vec<String>,
    #[serde(default = "defaults::max_pages_per_section")]
    pub max_pages_per_section: u32,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            tickers: Vec::new(),
            max_pages_per_section: defaults::max_pages_per_section(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct StorageConfig {
    #[serde(default)]
    pub db_path: Option<String>,
    #[serde(default)]
    pub lock_path: Option<String>,
    #[serde(default)]
    pub retention: RetentionConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RetentionConfig {
    #[serde(default = "defaults::keep_runs_days")]
    pub keep_runs_days: u32,
    #[serde(default = "defaults::keep_versions_per_item")]
    pub keep_versions_per_item: u32,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            keep_runs_days: defaults::keep_runs_days(),
            keep_versions_per_item: defaults::keep_versions_per_item(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AnalysisConfig {
    #[serde(default)]
    pub lmstudio: LmStudioConfig,
    #[serde(default)]
    pub thresholds: AnalysisThresholds,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LmStudioConfig {
    #[serde(default = "defaults::lmstudio_enabled")]
    pub enabled: bool,
    #[serde(default = "defaults::lmstudio_base_url")]
    pub base_url: String,
    #[serde(default)]
    pub model: String,
    #[serde(default = "defaults::lmstudio_max_input_chars")]
    pub max_input_chars: u32,
}

impl Default for LmStudioConfig {
    fn default() -> Self {
        Self {
            enabled: defaults::lmstudio_enabled(),
            base_url: defaults::lmstudio_base_url(),
            model: String::new(),
            max_input_chars: defaults::lmstudio_max_input_chars(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AnalysisThresholds {
    #[serde(default = "defaults::high_impact")]
    pub high_impact: f64,
    #[serde(default = "defaults::low_confidence")]
    pub low_confidence: f64,
}

impl Default for AnalysisThresholds {
    fn default() -> Self {
        Self {
            high_impact: defaults::high_impact(),
            low_confidence: defaults::low_confidence(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ReportsConfig {
    #[serde(default)]
    pub output_dir: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ResolvedPaths {
    pub config_file: PathBuf,
    pub root_dir: PathBuf,
    pub browser_user_data_dir: PathBuf,
    pub storage_db_path: PathBuf,
    pub storage_lock_path: PathBuf,
    pub reports_output_dir: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoctorIssueSeverity {
    Warning,
    Error,
}

#[derive(Debug, Clone)]
pub struct DoctorIssue {
    pub severity: DoctorIssueSeverity,
    pub message: String,
}

#[derive(Debug, Clone, Default)]
pub struct DoctorReport {
    pub issues: Vec<DoctorIssue>,
    pub warning_count: usize,
    pub error_count: usize,
}

pub fn load_default_config() -> Result<OmensConfig, String> {
    let home = home_dir()?;
    let config_file = home.join(".omens/config/omens.toml");
    let mut config = if config_file.exists() {
        parse_config_file(&config_file)?
    } else {
        OmensConfig::default()
    };
    validate_semantics(&config)?;
    config.resolved = resolve_paths(config_file, &config)?;
    Ok(config)
}

pub fn bootstrap_layout(config: &OmensConfig) -> Result<(), String> {
    let dirs = [
        config.resolved.root_dir.join("config"),
        config.resolved.root_dir.join("browser/profiles/default"),
        config.resolved.root_dir.join("browser/profiles/ephemeral"),
        config.resolved.root_dir.join("browser/chromium"),
        config.resolved.root_dir.join("db"),
        config.resolved.root_dir.join("logs"),
        config.resolved.root_dir.join("reports"),
        config.resolved.root_dir.join("fixtures"),
        config.resolved.root_dir.join("failure_bundles"),
        config.resolved.root_dir.join("lock"),
    ];

    for dir in dirs {
        fs::create_dir_all(&dir).map_err(|e| format!("failed to create {}: {e}", dir.display()))?;
    }

    if let Some(parent) = config.resolved.storage_db_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
    }

    if let Some(parent) = config.resolved.storage_lock_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
    }

    fs::create_dir_all(&config.resolved.reports_output_dir).map_err(|e| {
        format!(
            "failed to create {}: {e}",
            config.resolved.reports_output_dir.display()
        )
    })?;

    Ok(())
}

fn parse_config_file(path: &Path) -> Result<OmensConfig, String> {
    let text = fs::read_to_string(path)
        .map_err(|e| format!("failed to read config file {}: {e}", path.display()))?;
    let config: OmensConfig =
        toml::from_str(&text).map_err(|e| format!("config parse error: {e}"))?;
    validate_semantics(&config)?;
    Ok(config)
}

fn validate_semantics(config: &OmensConfig) -> Result<(), String> {
    match config.browser.mode.as_str() {
        "bundled" | "system" => {}
        _ => {
            return Err(format!(
                "browser.mode must be `bundled` or `system`, got `{}`",
                config.browser.mode
            ));
        }
    }

    if config.collector.max_pages_per_section == 0 {
        return Err("collector.max_pages_per_section must be greater than 0".to_string());
    }

    if config.storage.retention.keep_runs_days == 0 {
        return Err("storage.retention.keep_runs_days must be greater than 0".to_string());
    }

    if config.storage.retention.keep_versions_per_item == 0 {
        return Err("storage.retention.keep_versions_per_item must be greater than 0".to_string());
    }

    if !(0.0..=1.0).contains(&config.analysis.thresholds.high_impact) {
        return Err("analysis.thresholds.high_impact must be between 0.0 and 1.0".to_string());
    }

    if !(0.0..=1.0).contains(&config.analysis.thresholds.low_confidence) {
        return Err("analysis.thresholds.low_confidence must be between 0.0 and 1.0".to_string());
    }

    Ok(())
}

pub fn run_doctor_checks(config: &OmensConfig, now: SystemTime) -> DoctorReport {
    let mut report = DoctorReport::default();

    if !config.resolved.config_file.exists() {
        push_issue(
            &mut report,
            DoctorIssueSeverity::Warning,
            format!(
                "config file not found at {}; defaults are in use",
                config.resolved.config_file.display()
            ),
        );
    }

    check_parent_path(
        &mut report,
        "storage.db_path",
        config.resolved.storage_db_path.parent(),
    );
    check_parent_path(
        &mut report,
        "storage.lock_path",
        config.resolved.storage_lock_path.parent(),
    );

    match config.browser.mode.as_str() {
        "system" => check_system_browser(&mut report, config),
        "bundled" => check_bundled_browser(&mut report, config, now),
        _ => {}
    }

    report
}

fn check_parent_path(report: &mut DoctorReport, name: &str, parent: Option<&Path>) {
    match parent {
        Some(path) if path.exists() => {}
        Some(path) => push_issue(
            report,
            DoctorIssueSeverity::Error,
            format!("{name} parent directory does not exist: {}", path.display()),
        ),
        None => push_issue(
            report,
            DoctorIssueSeverity::Error,
            format!("{name} has no parent directory"),
        ),
    }
}

fn check_system_browser(report: &mut DoctorReport, config: &OmensConfig) {
    let explicit_path = config
        .browser
        .system_binary_path
        .as_deref()
        .unwrap_or("")
        .trim()
        .to_string();

    if !explicit_path.is_empty() {
        let binary = Path::new(&explicit_path);
        if !binary.exists() {
            push_issue(
                report,
                DoctorIssueSeverity::Error,
                format!(
                    "browser.system_binary_path does not exist: {}",
                    binary.display()
                ),
            );
        }
        return;
    }

    let well_known = [
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
    ];
    if !well_known.iter().any(|p| Path::new(p).exists()) {
        push_issue(
            report,
            DoctorIssueSeverity::Error,
            "no system browser found; set browser.system_binary_path in config".to_string(),
        );
    }
}

fn check_bundled_browser(report: &mut DoctorReport, config: &OmensConfig, now: SystemTime) {
    let current = config.resolved.root_dir.join("browser/chromium/current");
    if !current.exists() {
        push_issue(
            report,
            DoctorIssueSeverity::Warning,
            "bundled browser is not installed; run `omens browser install`".to_string(),
        );
    }

    let lock_file = config
        .resolved
        .root_dir
        .join("browser/chromium/chromium.lock");
    let metadata = match fs::metadata(&lock_file) {
        Ok(metadata) => metadata,
        Err(_) => return,
    };

    let modified = match metadata.modified() {
        Ok(modified) => modified,
        Err(_) => return,
    };

    let age = match now.duration_since(modified) {
        Ok(age) => age,
        Err(_) => return,
    };

    let max_age = Duration::from_secs(90 * 24 * 60 * 60);
    if age > max_age {
        let days = age.as_secs() / (24 * 60 * 60);
        push_issue(
            report,
            DoctorIssueSeverity::Warning,
            format!(
                "bundled runtime metadata is {days} days old; consider `omens browser upgrade`"
            ),
        );
    }
}

fn push_issue(report: &mut DoctorReport, severity: DoctorIssueSeverity, message: String) {
    match severity {
        DoctorIssueSeverity::Warning => report.warning_count += 1,
        DoctorIssueSeverity::Error => report.error_count += 1,
    }
    report.issues.push(DoctorIssue { severity, message });
}

fn resolve_paths(config_file: PathBuf, config: &OmensConfig) -> Result<ResolvedPaths, String> {
    let home = home_dir()?;

    let root_dir = resolve_with_default(config.runtime.root_dir.as_deref(), home.join(".omens"))?;

    let browser_user_data_dir = resolve_with_default(
        config.browser.user_data_dir.as_deref(),
        root_dir.join("browser/profiles/default"),
    )?;

    let storage_db_path = resolve_with_default(
        config.storage.db_path.as_deref(),
        root_dir.join("db/omens.db"),
    )?;

    let storage_lock_path = resolve_with_default(
        config.storage.lock_path.as_deref(),
        root_dir.join("lock/collect.lock"),
    )?;

    let reports_output_dir = resolve_with_default(
        config.reports.output_dir.as_deref(),
        root_dir.join("reports"),
    )?;

    Ok(ResolvedPaths {
        config_file,
        root_dir,
        browser_user_data_dir,
        storage_db_path,
        storage_lock_path,
        reports_output_dir,
    })
}

fn resolve_with_default(value: Option<&str>, fallback: PathBuf) -> Result<PathBuf, String> {
    match value {
        Some(v) if !v.trim().is_empty() => expand_tilde(v),
        _ => Ok(fallback),
    }
}

fn expand_tilde(input: &str) -> Result<PathBuf, String> {
    if let Some(rest) = input.strip_prefix("~/") {
        return Ok(home_dir()?.join(rest));
    }

    Ok(PathBuf::from(input))
}

fn home_dir() -> Result<PathBuf, String> {
    let home =
        std::env::var("HOME").map_err(|_| "HOME environment variable is not set".to_string())?;
    Ok(PathBuf::from(home))
}

#[cfg(test)]
mod tests {
    use super::{
        DoctorIssueSeverity, OmensConfig, parse_config_file, resolve_paths, run_doctor_checks,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn unique_temp_file(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("omens-{name}-{nanos}.toml"))
    }

    fn unique_temp_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("omens-{name}-dir-{nanos}"))
    }

    #[test]
    fn parse_typed_config_values() {
        let path = unique_temp_file("parse");
        fs::write(
            &path,
            "[clubefii]\nauth_marker = \"dashboard\"\nprotected_probe_url = \"https://probe.local\"\n\
             [runtime]\nroot_dir = \"~/custom\"\n\
             [browser]\nbundled_build = 123\n\
             [collector]\nmax_pages_per_section = 9\n\
             [analysis.thresholds]\nhigh_impact = 0.9\n",
        )
        .expect("should write config file");

        let config = parse_config_file(&path).expect("config should parse");
        assert_eq!(config.runtime.root_dir.as_deref(), Some("~/custom"));
        assert_eq!(config.clubefii.auth_marker.as_deref(), Some("dashboard"));
        assert_eq!(
            config.clubefii.protected_probe_url.as_deref(),
            Some("https://probe.local")
        );
        assert_eq!(config.browser.bundled_build, 123);
        assert_eq!(config.collector.max_pages_per_section, 9);
        assert_eq!(config.analysis.thresholds.high_impact, 0.9);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn explicit_override_wins_over_derived_paths() {
        let mut config = OmensConfig::default();
        config.runtime.root_dir = Some("/tmp/omens-root".to_string());
        config.storage.db_path = Some("/tmp/explicit.db".to_string());

        let resolved =
            resolve_paths(PathBuf::from("/tmp/omens.toml"), &config).expect("paths should resolve");
        assert_eq!(resolved.root_dir, PathBuf::from("/tmp/omens-root"));
        assert_eq!(resolved.storage_db_path, PathBuf::from("/tmp/explicit.db"));
    }

    #[test]
    fn invalid_type_errors() {
        let path = unique_temp_file("invalid");
        fs::write(&path, "[analysis.lmstudio]\nenabled = \"yes\"\n")
            .expect("should write invalid config");

        let err = parse_config_file(&path).expect_err("parsing should fail");
        assert!(err.contains("config parse error"));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn invalid_array_item_errors() {
        let path = unique_temp_file("invalid-array");
        fs::write(&path, "[collector]\ntickers = [\"BRCR11\", 7]\n")
            .expect("should write invalid array config");

        let err = parse_config_file(&path).expect_err("parsing should fail");
        assert!(err.contains("config parse error"));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn invalid_browser_mode_errors() {
        let path = unique_temp_file("invalid-mode");
        fs::write(&path, "[browser]\nmode = \"invalid\"\n").expect("should write invalid mode");

        let err = parse_config_file(&path).expect_err("parsing should fail");
        assert!(err.contains("browser.mode must be `bundled` or `system`"));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn parse_multiline_tickers() {
        let path = unique_temp_file("multiline");
        fs::write(
            &path,
            "[collector]\ntickers = [\n  \"ALZC11\",\n  \"BRCR11\",\n  \"VISC11\",\n]\n",
        )
        .expect("should write config file");

        let config = parse_config_file(&path).expect("multiline tickers should parse");
        assert_eq!(config.collector.tickers.len(), 3);
        assert_eq!(config.collector.tickers[0], "ALZC11");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn out_of_range_threshold_errors() {
        let path = unique_temp_file("invalid-threshold");
        fs::write(&path, "[analysis.thresholds]\nhigh_impact = 1.2\n")
            .expect("should write invalid threshold");

        let err = parse_config_file(&path).expect_err("parsing should fail");
        assert!(err.contains("analysis.thresholds.high_impact must be between 0.0 and 1.0"));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn doctor_warns_when_config_missing_and_bundled_runtime_absent() {
        let root = unique_temp_dir("doctor-warn");
        fs::create_dir_all(root.join("db")).expect("db dir should be created");
        fs::create_dir_all(root.join("lock")).expect("lock dir should be created");

        let mut config = OmensConfig::default();
        config.browser.mode = "bundled".to_string();
        config.resolved.config_file = root.join("config/omens.toml");
        config.resolved.root_dir = root.clone();
        config.resolved.storage_db_path = root.join("db/omens.db");
        config.resolved.storage_lock_path = root.join("lock/collect.lock");
        config.resolved.reports_output_dir = root.join("reports");

        let report = run_doctor_checks(&config, SystemTime::now());
        assert!(report.warning_count >= 2);
        assert_eq!(report.error_count, 0);
    }

    #[test]
    fn doctor_errors_for_system_mode_without_binary_path() {
        let root = unique_temp_dir("doctor-system");
        fs::create_dir_all(root.join("db")).expect("db dir should be created");
        fs::create_dir_all(root.join("lock")).expect("lock dir should be created");

        let mut config = OmensConfig::default();
        config.browser.mode = "system".to_string();
        config.browser.system_binary_path = Some("/nonexistent/browser".to_string());
        config.resolved.config_file = root.join("config/omens.toml");
        config.resolved.root_dir = root.clone();
        config.resolved.storage_db_path = root.join("db/omens.db");
        config.resolved.storage_lock_path = root.join("lock/collect.lock");
        config.resolved.reports_output_dir = root.join("reports");

        let report = run_doctor_checks(&config, SystemTime::now());
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.severity == DoctorIssueSeverity::Error
                    && issue.message.contains("does not exist"))
        );
    }

    #[test]
    fn doctor_warns_when_bundled_runtime_is_older_than_policy() {
        let root = unique_temp_dir("doctor-age");
        let chromium_dir = root.join("browser/chromium");
        fs::create_dir_all(&chromium_dir).expect("chromium dir should be created");
        fs::create_dir_all(root.join("db")).expect("db dir should be created");
        fs::create_dir_all(root.join("lock")).expect("lock dir should be created");
        fs::write(chromium_dir.join("chromium.lock"), "test").expect("lock file should be written");

        let mut config = OmensConfig::default();
        config.browser.mode = "bundled".to_string();
        config.resolved.config_file = root.join("config/omens.toml");
        config.resolved.root_dir = root.clone();
        config.resolved.storage_db_path = root.join("db/omens.db");
        config.resolved.storage_lock_path = root.join("lock/collect.lock");
        config.resolved.reports_output_dir = root.join("reports");

        let now = SystemTime::now() + Duration::from_secs(91 * 24 * 60 * 60);
        let report = run_doctor_checks(&config, now);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.severity == DoctorIssueSeverity::Warning
                    && issue.message.contains("days old"))
        );
    }

    #[test]
    fn hash_inside_quoted_string_is_preserved() {
        let path = unique_temp_file("hash-in-string");
        fs::write(
            &path,
            "[clubefii]\nbase_url = \"https://example.com/path#frag\"\n",
        )
        .expect("should write config");

        let config = parse_config_file(&path).expect("config should parse");
        assert_eq!(config.clubefii.base_url, "https://example.com/path#frag");

        let _ = fs::remove_file(path);
    }
}
