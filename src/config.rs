use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone)]
pub struct OmensConfig {
    pub clubefii: ClubeFiiConfig,
    pub runtime: RuntimeConfig,
    pub browser: BrowserConfig,
    pub collector: CollectorConfig,
    pub storage: StorageConfig,
    pub analysis: AnalysisConfig,
    pub reports: ReportsConfig,
    pub resolved: ResolvedPaths,
}

#[derive(Debug, Clone)]
pub struct ClubeFiiConfig {
    pub base_url: String,
    pub login_url: String,
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub root_dir: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BrowserConfig {
    pub mode: String,
    pub system_binary_path: Option<String>,
    pub bundled_build: u64,
    pub user_data_dir: Option<String>,
    pub headless_collect: bool,
}

#[derive(Debug, Clone)]
pub struct CollectorConfig {
    pub sections: Vec<String>,
    pub max_pages_per_section: u32,
    pub pagination_mode: String,
    pub detail_open_policy: String,
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub db_path: Option<String>,
    pub lock_path: Option<String>,
    pub retention: RetentionConfig,
}

#[derive(Debug, Clone)]
pub struct RetentionConfig {
    pub keep_runs_days: u32,
    pub keep_versions_per_item: u32,
}

#[derive(Debug, Clone)]
pub struct AnalysisConfig {
    pub lmstudio: LmStudioConfig,
    pub thresholds: AnalysisThresholds,
}

#[derive(Debug, Clone)]
pub struct LmStudioConfig {
    pub enabled: bool,
    pub base_url: String,
    pub model: String,
    pub max_input_chars: u32,
}

#[derive(Debug, Clone)]
pub struct AnalysisThresholds {
    pub high_impact: f64,
    pub low_confidence: f64,
}

#[derive(Debug, Clone)]
pub struct ReportsConfig {
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

impl Default for OmensConfig {
    fn default() -> Self {
        Self {
            clubefii: ClubeFiiConfig {
                base_url: "https://www.clubefii.com.br".to_string(),
                login_url: "https://www.clubefii.com.br/login".to_string(),
            },
            runtime: RuntimeConfig { root_dir: None },
            browser: BrowserConfig {
                mode: "bundled".to_string(),
                system_binary_path: None,
                bundled_build: 0,
                user_data_dir: None,
                headless_collect: true,
            },
            collector: CollectorConfig {
                sections: vec!["news".to_string(), "material-facts".to_string()],
                max_pages_per_section: 20,
                pagination_mode: "next_link".to_string(),
                detail_open_policy: "when_listing_incomplete".to_string(),
            },
            storage: StorageConfig {
                db_path: None,
                lock_path: None,
                retention: RetentionConfig {
                    keep_runs_days: 180,
                    keep_versions_per_item: 20,
                },
            },
            analysis: AnalysisConfig {
                lmstudio: LmStudioConfig {
                    enabled: true,
                    base_url: "http://127.0.0.1:1234/v1".to_string(),
                    model: "".to_string(),
                    max_input_chars: 12000,
                },
                thresholds: AnalysisThresholds {
                    high_impact: 0.8,
                    low_confidence: 0.3,
                },
            },
            reports: ReportsConfig { output_dir: None },
            resolved: ResolvedPaths::default(),
        }
    }
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

    let entries = parse_entries(&text)?;
    let mut config = OmensConfig::default();

    if let Some(value) = entries.get("clubefii.base_url") {
        config.clubefii.base_url = expect_string("clubefii.base_url", value)?;
    }
    if let Some(value) = entries.get("clubefii.login_url") {
        config.clubefii.login_url = expect_string("clubefii.login_url", value)?;
    }

    if let Some(value) = entries.get("runtime.root_dir") {
        config.runtime.root_dir = Some(expect_string("runtime.root_dir", value)?);
    }

    if let Some(value) = entries.get("browser.mode") {
        config.browser.mode = expect_string("browser.mode", value)?;
    }
    if let Some(value) = entries.get("browser.system_binary_path") {
        config.browser.system_binary_path =
            Some(expect_string("browser.system_binary_path", value)?);
    }
    if let Some(value) = entries.get("browser.bundled_build") {
        config.browser.bundled_build = expect_u64("browser.bundled_build", value)?;
    }
    if let Some(value) = entries.get("browser.user_data_dir") {
        config.browser.user_data_dir = Some(expect_string("browser.user_data_dir", value)?);
    }
    if let Some(value) = entries.get("browser.headless_collect") {
        config.browser.headless_collect = expect_bool("browser.headless_collect", value)?;
    }

    if let Some(value) = entries.get("collector.sections") {
        config.collector.sections = expect_string_array("collector.sections", value)?;
    }
    if let Some(value) = entries.get("collector.max_pages_per_section") {
        config.collector.max_pages_per_section =
            expect_u32("collector.max_pages_per_section", value)?;
    }
    if let Some(value) = entries.get("collector.pagination_mode") {
        config.collector.pagination_mode = expect_string("collector.pagination_mode", value)?;
    }
    if let Some(value) = entries.get("collector.detail_open_policy") {
        config.collector.detail_open_policy = expect_string("collector.detail_open_policy", value)?;
    }

    if let Some(value) = entries.get("storage.db_path") {
        config.storage.db_path = Some(expect_string("storage.db_path", value)?);
    }
    if let Some(value) = entries.get("storage.lock_path") {
        config.storage.lock_path = Some(expect_string("storage.lock_path", value)?);
    }
    if let Some(value) = entries.get("storage.retention.keep_runs_days") {
        config.storage.retention.keep_runs_days =
            expect_u32("storage.retention.keep_runs_days", value)?;
    }
    if let Some(value) = entries.get("storage.retention.keep_versions_per_item") {
        config.storage.retention.keep_versions_per_item =
            expect_u32("storage.retention.keep_versions_per_item", value)?;
    }

    if let Some(value) = entries.get("analysis.lmstudio.enabled") {
        config.analysis.lmstudio.enabled = expect_bool("analysis.lmstudio.enabled", value)?;
    }
    if let Some(value) = entries.get("analysis.lmstudio.base_url") {
        config.analysis.lmstudio.base_url = expect_string("analysis.lmstudio.base_url", value)?;
    }
    if let Some(value) = entries.get("analysis.lmstudio.model") {
        config.analysis.lmstudio.model = expect_string("analysis.lmstudio.model", value)?;
    }
    if let Some(value) = entries.get("analysis.lmstudio.max_input_chars") {
        config.analysis.lmstudio.max_input_chars =
            expect_u32("analysis.lmstudio.max_input_chars", value)?;
    }
    if let Some(value) = entries.get("analysis.thresholds.high_impact") {
        config.analysis.thresholds.high_impact =
            expect_f64("analysis.thresholds.high_impact", value)?;
    }
    if let Some(value) = entries.get("analysis.thresholds.low_confidence") {
        config.analysis.thresholds.low_confidence =
            expect_f64("analysis.thresholds.low_confidence", value)?;
    }

    if let Some(value) = entries.get("reports.output_dir") {
        config.reports.output_dir = Some(expect_string("reports.output_dir", value)?);
    }

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

    if config.collector.sections.is_empty() {
        return Err("collector.sections must not be empty".to_string());
    }

    for section in &config.collector.sections {
        if section != "news" && section != "material-facts" {
            return Err(format!(
                "collector.sections contains unsupported section `{section}`"
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
    let path = config
        .browser
        .system_binary_path
        .as_deref()
        .unwrap_or("")
        .trim();
    if path.is_empty() {
        push_issue(
            report,
            DoctorIssueSeverity::Error,
            "browser.mode=system requires browser.system_binary_path".to_string(),
        );
        return;
    }

    let binary = Path::new(path);
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

fn parse_entries(text: &str) -> Result<HashMap<String, TomlValue>, String> {
    let mut section = String::new();
    let mut entries = HashMap::<String, TomlValue>::new();

    for (index, raw_line) in text.lines().enumerate() {
        let line_number = index + 1;
        let line = strip_comment(raw_line).trim();
        if line.is_empty() {
            continue;
        }

        if line.starts_with('[') && line.ends_with(']') {
            section = line[1..line.len() - 1].trim().to_string();
            continue;
        }

        let (key, value) = parse_key_value(line)
            .map_err(|e| format!("config parse error at line {line_number}: {e}"))?;

        let full_key = if section.is_empty() {
            key
        } else {
            format!("{section}.{key}")
        };

        entries.insert(full_key, value);
    }

    Ok(entries)
}

#[derive(Debug, Clone, PartialEq)]
enum TomlValue {
    String(String),
    Bool(bool),
    Integer(u64),
    Float(f64),
    ArrayString(Vec<String>),
}

fn parse_key_value(line: &str) -> Result<(String, TomlValue), String> {
    let parts: Vec<&str> = line.splitn(2, '=').collect();
    if parts.len() != 2 {
        return Err("expected `key = value`".to_string());
    }

    let key = parts[0].trim();
    if key.is_empty() {
        return Err("missing key".to_string());
    }

    let value = parts[1].trim();
    let parsed = parse_value(value)?;

    Ok((key.to_string(), parsed))
}

fn parse_value(value: &str) -> Result<TomlValue, String> {
    if value.starts_with('"') {
        return Ok(TomlValue::String(parse_toml_string(value)?));
    }

    if value == "true" {
        return Ok(TomlValue::Bool(true));
    }

    if value == "false" {
        return Ok(TomlValue::Bool(false));
    }

    if value.starts_with('[') {
        return Ok(TomlValue::ArrayString(parse_string_array(value)?));
    }

    if value.contains('.') {
        let parsed = value
            .parse::<f64>()
            .map_err(|_| format!("invalid float literal: {value}"))?;
        return Ok(TomlValue::Float(parsed));
    }

    let parsed = value
        .parse::<u64>()
        .map_err(|_| format!("unsupported literal `{value}`"))?;
    Ok(TomlValue::Integer(parsed))
}

fn parse_toml_string(value: &str) -> Result<String, String> {
    if !value.starts_with('"') || !value.ends_with('"') || value.len() < 2 {
        return Err("expected quoted string".to_string());
    }

    let inner = &value[1..value.len() - 1];
    if inner.contains('"') {
        return Err("embedded quote is not supported in phase 1 parser".to_string());
    }

    Ok(inner.to_string())
}

fn parse_string_array(value: &str) -> Result<Vec<String>, String> {
    if !value.starts_with('[') || !value.ends_with(']') {
        return Err("array must start with `[` and end with `]`".to_string());
    }

    let inner = value[1..value.len() - 1].trim();
    if inner.is_empty() {
        return Ok(Vec::new());
    }

    inner
        .split(',')
        .map(|entry| parse_toml_string(entry.trim()))
        .collect()
}

fn expect_string(key: &str, value: &TomlValue) -> Result<String, String> {
    match value {
        TomlValue::String(v) => Ok(v.clone()),
        _ => Err(format!("{key} must be a string")),
    }
}

fn expect_bool(key: &str, value: &TomlValue) -> Result<bool, String> {
    match value {
        TomlValue::Bool(v) => Ok(*v),
        _ => Err(format!("{key} must be a boolean")),
    }
}

fn expect_u64(key: &str, value: &TomlValue) -> Result<u64, String> {
    match value {
        TomlValue::Integer(v) => Ok(*v),
        _ => Err(format!("{key} must be an integer")),
    }
}

fn expect_u32(key: &str, value: &TomlValue) -> Result<u32, String> {
    let parsed = expect_u64(key, value)?;
    u32::try_from(parsed).map_err(|_| format!("{key} is out of range for u32"))
}

fn expect_f64(key: &str, value: &TomlValue) -> Result<f64, String> {
    match value {
        TomlValue::Float(v) => Ok(*v),
        TomlValue::Integer(v) => Ok(*v as f64),
        _ => Err(format!("{key} must be numeric")),
    }
}

fn expect_string_array(key: &str, value: &TomlValue) -> Result<Vec<String>, String> {
    match value {
        TomlValue::ArrayString(v) => Ok(v.clone()),
        _ => Err(format!("{key} must be an array of strings")),
    }
}

fn strip_comment(raw_line: &str) -> &str {
    if let Some(pos) = raw_line.find('#') {
        &raw_line[..pos]
    } else {
        raw_line
    }
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
            "[runtime]\nroot_dir = \"~/custom\"\n\
             [browser]\nheadless_collect = false\nbundled_build = 123\n\
             [collector]\nsections = [\"news\",\"material-facts\"]\nmax_pages_per_section = 9\n\
             [analysis.thresholds]\nhigh_impact = 0.9\n",
        )
        .expect("should write config file");

        let config = parse_config_file(&path).expect("config should parse");
        assert_eq!(config.runtime.root_dir.as_deref(), Some("~/custom"));
        assert!(!config.browser.headless_collect);
        assert_eq!(config.browser.bundled_build, 123);
        assert_eq!(
            config.collector.sections,
            vec!["news".to_string(), "material-facts".to_string()]
        );
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
        fs::write(&path, "[browser]\nheadless_collect = \"yes\"\n")
            .expect("should write invalid config");

        let err = parse_config_file(&path).expect_err("parsing should fail");
        assert!(err.contains("browser.headless_collect must be a boolean"));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn invalid_array_item_errors() {
        let path = unique_temp_file("invalid-array");
        fs::write(&path, "[collector]\nsections = [\"news\", 7]\n")
            .expect("should write invalid array config");

        let err = parse_config_file(&path).expect_err("parsing should fail");
        assert!(err.contains("expected quoted string"));

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
    fn invalid_section_name_errors() {
        let path = unique_temp_file("invalid-section");
        fs::write(&path, "[collector]\nsections = [\"news\", \"foo\"]\n")
            .expect("should write invalid sections");

        let err = parse_config_file(&path).expect_err("parsing should fail");
        assert!(err.contains("collector.sections contains unsupported section `foo`"));

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
        config.browser.system_binary_path = None;
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
                    && issue.message.contains("browser.mode=system requires"))
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
}
