use crate::config::OmensConfig;
use std::fs;
use std::path::{Path, PathBuf};

pub const PINNED_CHROMIUM_BUILD: u64 = 1418433;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrowserMode {
    Bundled,
    System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChromiumPlatform {
    Linux64,
    LinuxArm64,
    MacArm64,
    MacX64,
}

impl ChromiumPlatform {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChromiumPlatform::Linux64 => "linux64",
            ChromiumPlatform::LinuxArm64 => "linux-arm64",
            ChromiumPlatform::MacArm64 => "mac-arm64",
            ChromiumPlatform::MacX64 => "mac-x64",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowserInstallState {
    pub mode: BrowserMode,
    pub platform: ChromiumPlatform,
    pub target_build: u64,
    pub active_build: Option<u64>,
    pub is_installed: bool,
    pub current_path: PathBuf,
    pub lock_path: PathBuf,
    pub download_url: String,
}

#[derive(Debug, Clone, Default)]
struct ChromiumLockMetadata {
    build: Option<u64>,
}

pub struct BrowserManager {
    root_dir: PathBuf,
    mode: BrowserMode,
    configured_build: u64,
    platform: ChromiumPlatform,
}

impl BrowserManager {
    pub fn from_config(config: &OmensConfig) -> Result<Self, String> {
        let mode = parse_browser_mode(&config.browser.mode)?;
        let platform = detect_platform(std::env::consts::OS, std::env::consts::ARCH)?;

        Ok(Self {
            root_dir: config.resolved.root_dir.clone(),
            mode,
            configured_build: config.browser.bundled_build,
            platform,
        })
    }

    pub fn status(&self) -> BrowserInstallState {
        let target_build = self.target_build();
        let chromium_dir = self.root_dir.join("browser/chromium");
        let current_path = chromium_dir.join("current");
        let lock_path = chromium_dir.join("chromium.lock");
        let metadata = parse_lock_metadata(&lock_path).unwrap_or_default();

        let is_installed = current_path.exists();
        let active_build = metadata.build;
        let download_url = chromium_download_url(target_build, self.platform);

        BrowserInstallState {
            mode: self.mode,
            platform: self.platform,
            target_build,
            active_build,
            is_installed,
            current_path,
            lock_path,
            download_url,
        }
    }

    fn target_build(&self) -> u64 {
        if self.configured_build == 0 {
            PINNED_CHROMIUM_BUILD
        } else {
            self.configured_build
        }
    }
}

fn parse_browser_mode(value: &str) -> Result<BrowserMode, String> {
    match value {
        "bundled" => Ok(BrowserMode::Bundled),
        "system" => Ok(BrowserMode::System),
        _ => Err(format!("unsupported browser mode `{value}`")),
    }
}

pub fn detect_platform(os: &str, arch: &str) -> Result<ChromiumPlatform, String> {
    match (os, arch) {
        ("linux", "x86_64") => Ok(ChromiumPlatform::Linux64),
        ("linux", "aarch64") => Ok(ChromiumPlatform::LinuxArm64),
        ("macos", "aarch64") => Ok(ChromiumPlatform::MacArm64),
        ("macos", "x86_64") => Ok(ChromiumPlatform::MacX64),
        _ => Err(format!("unsupported platform os={os} arch={arch}")),
    }
}

pub fn chromium_download_url(build: u64, platform: ChromiumPlatform) -> String {
    format!(
        "https://storage.googleapis.com/chrome-for-testing-public/{build}/{}/chrome-{}.zip",
        platform.as_str(),
        platform.as_str()
    )
}

fn parse_lock_metadata(path: &Path) -> Result<ChromiumLockMetadata, String> {
    if !path.exists() {
        return Ok(ChromiumLockMetadata::default());
    }

    let text = fs::read_to_string(path)
        .map_err(|err| format!("failed to read lock metadata {}: {err}", path.display()))?;

    let mut metadata = ChromiumLockMetadata::default();

    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let mut parts = trimmed.splitn(2, '=');
        let key = parts.next().unwrap_or("").trim();
        let value = parts.next().unwrap_or("").trim();

        if key == "build" {
            metadata.build = value.parse::<u64>().ok();
        }
    }

    Ok(metadata)
}

#[cfg(test)]
mod tests {
    use super::{
        BrowserManager, ChromiumPlatform, chromium_download_url, detect_platform,
        parse_lock_metadata,
    };
    use crate::config::OmensConfig;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("omens-browser-{name}-{nanos}"))
    }

    #[test]
    fn build_zero_resolves_to_pinned() {
        let root = unique_temp_dir("pinned");
        fs::create_dir_all(&root).expect("root dir should be created");

        let mut config = OmensConfig::default();
        config.resolved.root_dir = root;
        config.browser.bundled_build = 0;

        let manager = BrowserManager::from_config(&config).expect("manager should build");
        let status = manager.status();
        assert_eq!(status.target_build, super::PINNED_CHROMIUM_BUILD);
    }

    #[test]
    fn explicit_build_is_used_when_set() {
        let root = unique_temp_dir("explicit");
        fs::create_dir_all(&root).expect("root dir should be created");

        let mut config = OmensConfig::default();
        config.resolved.root_dir = root;
        config.browser.bundled_build = 123;

        let manager = BrowserManager::from_config(&config).expect("manager should build");
        let status = manager.status();
        assert_eq!(status.target_build, 123);
    }

    #[test]
    fn url_builder_uses_expected_format() {
        let url = chromium_download_url(1418433, ChromiumPlatform::Linux64);
        assert_eq!(
            url,
            "https://storage.googleapis.com/chrome-for-testing-public/1418433/linux64/chrome-linux64.zip"
        );
    }

    #[test]
    fn platform_mapping_matches_supported_targets() {
        assert_eq!(
            detect_platform("linux", "x86_64").expect("linux should be supported"),
            ChromiumPlatform::Linux64
        );
        assert_eq!(
            detect_platform("macos", "aarch64").expect("mac arm should be supported"),
            ChromiumPlatform::MacArm64
        );
        assert_eq!(
            detect_platform("macos", "x86_64").expect("mac x64 should be supported"),
            ChromiumPlatform::MacX64
        );
        assert_eq!(
            detect_platform("linux", "aarch64").expect("linux arm64 should be supported"),
            ChromiumPlatform::LinuxArm64
        );
    }

    #[test]
    fn lock_metadata_parse_reads_build() {
        let root = unique_temp_dir("lock");
        fs::create_dir_all(&root).expect("root dir should be created");
        let lock_path = root.join("chromium.lock");
        fs::write(&lock_path, "build=222\n").expect("lock file should be written");

        let metadata = parse_lock_metadata(&lock_path).expect("metadata should parse");
        assert_eq!(metadata.build, Some(222));
    }
}
