use crate::config::OmensConfig;
use std::fs;
use std::io::copy;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

pub const PINNED_CHROMIUM_BUILD: u64 = 1418433;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrowserMode {
    Bundled,
    System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChromiumPlatform {
    Linux64,
    MacArm64,
    MacX64,
}

impl ChromiumPlatform {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChromiumPlatform::Linux64 => "linux64",
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
    pub rollback_available: bool,
    pub current_path: PathBuf,
    pub previous_path: PathBuf,
    pub lock_path: PathBuf,
    pub lock_tmp_path: PathBuf,
    pub download_url: String,
}

#[derive(Debug, Clone, Default)]
struct ChromiumLockMetadata {
    build: Option<u64>,
    url: Option<String>,
    checksum_sha256: Option<String>,
    installed_at_unix: Option<u64>,
}

#[derive(Debug, Clone)]
struct ArtifactSource {
    url: String,
    expected_sha256: Option<String>,
    format: ArtifactFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArtifactFormat {
    Zip,
    Deb,
}

pub struct BrowserManager {
    root_dir: PathBuf,
    browser_profile_dir: PathBuf,
    system_binary_path: Option<PathBuf>,
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
            browser_profile_dir: config.resolved.browser_user_data_dir.clone(),
            system_binary_path: config.browser.system_binary_path.clone().map(PathBuf::from),
            mode,
            configured_build: config.browser.bundled_build,
            platform,
        })
    }

    pub fn browser_binary_path(&self) -> Result<PathBuf, String> {
        match self.mode {
            BrowserMode::System => self
                .system_binary_path
                .clone()
                .ok_or_else(|| "browser.system_binary_path is required in system mode".to_string()),
            BrowserMode::Bundled => {
                let current = self.chromium_dir().join("current");
                let candidates = [
                    current.join("chrome"),
                    current.join("chromium"),
                    current.join("chrome-linux64/chrome"),
                    current.join("chrome-mac-arm64/Chromium.app/Contents/MacOS/Chromium"),
                    current.join("chrome-mac-x64/Chromium.app/Contents/MacOS/Chromium"),
                    current.join("Chromium.app/Contents/MacOS/Chromium"),
                ];

                for candidate in candidates {
                    if candidate.exists() {
                        return Ok(candidate);
                    }
                }

                Err(format!(
                    "unable to resolve bundled browser binary under {}. run `omens browser install`",
                    current.display()
                ))
            }
        }
    }

    pub fn default_profile_dir(&self) -> &Path {
        &self.browser_profile_dir
    }

    pub fn status(&self) -> BrowserInstallState {
        let target_build = self.target_build();
        let chromium_dir = self.chromium_dir();
        let current_path = chromium_dir.join("current");
        let previous_path = chromium_dir.join("previous");
        let lock_path = chromium_dir.join("chromium.lock");
        let lock_tmp_path = chromium_dir.join("chromium.lock.tmp");
        let metadata = parse_lock_metadata(&lock_path).unwrap_or_default();

        let is_installed = current_path.exists();
        let active_build = self
            .current_build_from_link(&current_path)
            .or(metadata.build);

        BrowserInstallState {
            mode: self.mode,
            platform: self.platform,
            target_build,
            active_build,
            is_installed,
            rollback_available: previous_path.exists(),
            current_path,
            previous_path,
            lock_path,
            lock_tmp_path,
            download_url: chromium_download_url(target_build, self.platform),
        }
    }

    pub fn install(&self) -> Result<BrowserInstallState, String> {
        self.ensure_bundled_mode()?;
        let build = self.target_build();
        let source = self.resolve_artifact_source(build)?;
        self.finalize_install(build, &source)
    }

    pub fn upgrade(&self) -> Result<BrowserInstallState, String> {
        self.ensure_bundled_mode()?;
        let build = self.target_build();
        let source = self.resolve_artifact_source(build)?;
        self.finalize_install(build, &source)
    }

    pub fn rollback(&self) -> Result<BrowserInstallState, String> {
        self.ensure_bundled_mode()?;
        let status = self.status();
        if !status.rollback_available {
            return Err(
                "rollback is not available; no previous browser build is recorded".to_string(),
            );
        }

        let previous_target = read_link_target(&status.previous_path)?;
        self.set_link_target(&status.current_path, &previous_target)?;

        let build = build_from_path(&previous_target);
        self.write_lock_metadata(build, None, None)?;

        Ok(self.status())
    }

    pub fn reset_profile(&self) -> Result<(), String> {
        if self.browser_profile_dir.exists() {
            fs::remove_dir_all(&self.browser_profile_dir).map_err(|err| {
                format!(
                    "failed to remove profile directory {}: {err}",
                    self.browser_profile_dir.display()
                )
            })?;
        }

        fs::create_dir_all(&self.browser_profile_dir).map_err(|err| {
            format!(
                "failed to recreate profile directory {}: {err}",
                self.browser_profile_dir.display()
            )
        })
    }

    fn finalize_install(
        &self,
        build: u64,
        source: &ArtifactSource,
    ) -> Result<BrowserInstallState, String> {
        let status_before = self.status();
        self.cleanup_stale_tmp(&status_before.lock_tmp_path)?;

        let builds_dir = self.chromium_dir().join("builds");
        fs::create_dir_all(&builds_dir)
            .map_err(|err| format!("failed to create {}: {err}", builds_dir.display()))?;

        let build_dir = builds_dir.join(build.to_string());
        if build_dir.exists() {
            fs::remove_dir_all(&build_dir).map_err(|err| {
                format!("failed to clear previous {}: {err}", build_dir.display())
            })?;
        }
        fs::create_dir_all(&build_dir)
            .map_err(|err| format!("failed to create {}: {err}", build_dir.display()))?;

        let install_result = self.perform_install_steps(build, source, &build_dir);
        if let Err(err) = install_result {
            let _ = fs::remove_dir_all(&build_dir);
            let _ = fs::remove_file(&status_before.lock_tmp_path);
            return Err(err);
        }

        if status_before.current_path.exists() {
            let current_target = read_link_target(&status_before.current_path)?;
            self.set_link_target(&status_before.previous_path, &current_target)?;
        }

        let install_root = resolve_install_root(&build_dir)?;
        self.set_link_target(&status_before.current_path, &install_root)?;
        self.maybe_clear_macos_quarantine(&install_root)?;

        let checksum = compute_sha256(&self.archive_path(build, source.format))?;
        self.write_lock_metadata(
            Some(build),
            Some(source.url.as_str()),
            Some(checksum.as_str()),
        )?;

        Ok(self.status())
    }

    fn perform_install_steps(
        &self,
        build: u64,
        source: &ArtifactSource,
        build_dir: &Path,
    ) -> Result<(), String> {
        let archive_path = self.archive_path(build, source.format);
        self.download_archive(source.url.as_str(), &archive_path)?;

        let actual_checksum = compute_sha256(&archive_path)?;
        if let Some(expected) = source.expected_sha256.as_deref() {
            if !expected.eq_ignore_ascii_case(actual_checksum.as_str()) {
                return Err(format!(
                    "checksum mismatch for {} (expected {}, got {})",
                    archive_path.display(),
                    expected,
                    actual_checksum
                ));
            }
        }

        self.extract_archive(&archive_path, build_dir, source.format)
    }

    fn resolve_artifact_source(&self, build: u64) -> Result<ArtifactSource, String> {
        if let Some(url) = manifest_download_url(build, self.platform)? {
            let expected_sha256 = self.fetch_expected_checksum(&url)?;
            return Ok(ArtifactSource {
                url,
                expected_sha256,
                format: ArtifactFormat::Zip,
            });
        }

        if cfg!(target_os = "linux") && std::env::consts::ARCH == "aarch64" {
            if let Some(url) = debian_trixie_arm64_chromium_url()? {
                return Ok(ArtifactSource {
                    url,
                    expected_sha256: None,
                    format: ArtifactFormat::Deb,
                });
            }
        }

        let url = chromium_download_url(build, self.platform);
        let expected_sha256 = self.fetch_expected_checksum(&url)?;
        Ok(ArtifactSource {
            url,
            expected_sha256,
            format: ArtifactFormat::Zip,
        })
    }

    fn fetch_expected_checksum(&self, url: &str) -> Result<Option<String>, String> {
        let checksum_url = format!("{url}.sha256");
        let response = match http_client()?.get(checksum_url).send() {
            Ok(response) => response,
            Err(_) => return Ok(None),
        };

        if !response.status().is_success() {
            return Ok(None);
        }

        let text = response
            .text()
            .map_err(|err| format!("failed to read checksum body: {err}"))?;
        let checksum = text
            .split_whitespace()
            .next()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        Ok(checksum)
    }

    fn archive_path(&self, build: u64, format: ArtifactFormat) -> PathBuf {
        let extension = match format {
            ArtifactFormat::Zip => "zip",
            ArtifactFormat::Deb => "deb",
        };
        self.chromium_dir().join("cache").join(format!(
            "chrome-{}-{build}.{extension}",
            self.platform.as_str()
        ))
    }

    fn download_archive(&self, url: &str, archive_path: &Path) -> Result<(), String> {
        if let Some(parent) = archive_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("failed to create {}: {err}", parent.display()))?;
        }

        if let Some(local_path) = url.strip_prefix("file://") {
            fs::copy(local_path, archive_path)
                .map_err(|err| format!("failed to copy local archive {}: {err}", local_path))?;
            return Ok(());
        }

        let client = http_client()?;
        let temp_path = archive_path.with_extension("tmp");
        let mut last_error = String::new();

        for _ in 0..4 {
            match client.get(url).send() {
                Ok(mut response) if response.status().is_success() => {
                    let mut file = fs::File::create(&temp_path).map_err(|err| {
                        format!(
                            "failed to create temporary archive {}: {err}",
                            temp_path.display()
                        )
                    })?;
                    copy(&mut response, &mut file).map_err(|err| {
                        format!(
                            "failed to stream download to {}: {err}",
                            temp_path.display()
                        )
                    })?;
                    fs::rename(&temp_path, archive_path).map_err(|err| {
                        format!(
                            "failed to finalize downloaded archive {} -> {}: {err}",
                            temp_path.display(),
                            archive_path.display()
                        )
                    })?;
                    return Ok(());
                }
                Ok(response) => {
                    last_error = format!("http status {}", response.status());
                }
                Err(err) => {
                    last_error = err.to_string();
                }
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
        }

        Err(format!(
            "failed to download chrome archive from {url}: {last_error}"
        ))
    }

    fn extract_archive(
        &self,
        archive_path: &Path,
        destination: &Path,
        format: ArtifactFormat,
    ) -> Result<(), String> {
        if format == ArtifactFormat::Deb {
            let status = Command::new("dpkg-deb")
                .arg("-x")
                .arg(archive_path)
                .arg(destination)
                .status()
                .map_err(|err| {
                    format!(
                        "failed to start dpkg-deb for {}: {err}",
                        archive_path.display()
                    )
                })?;

            if !status.success() {
                return Err(format!(
                    "failed to extract Debian package {} (exit code {:?})",
                    archive_path.display(),
                    status.code()
                ));
            }

            return Ok(());
        }

        let status = Command::new("unzip")
            .arg("-q")
            .arg(archive_path)
            .arg("-d")
            .arg(destination)
            .status()
            .map_err(|err| {
                format!(
                    "failed to start unzip for {}: {err}",
                    archive_path.display()
                )
            })?;

        if !status.success() {
            return Err(format!(
                "failed to extract {} (exit code {:?})",
                archive_path.display(),
                status.code()
            ));
        }

        Ok(())
    }

    fn ensure_bundled_mode(&self) -> Result<(), String> {
        if self.mode != BrowserMode::Bundled {
            return Err("browser runtime commands require browser.mode=bundled".to_string());
        }
        Ok(())
    }

    fn cleanup_stale_tmp(&self, tmp_path: &Path) -> Result<(), String> {
        if tmp_path.exists() {
            fs::remove_file(tmp_path)
                .map_err(|err| format!("failed to clean stale {}: {err}", tmp_path.display()))?;
        }
        Ok(())
    }

    fn write_lock_metadata(
        &self,
        build: Option<u64>,
        url: Option<&str>,
        checksum_sha256: Option<&str>,
    ) -> Result<(), String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "system time is before unix epoch".to_string())?
            .as_secs();

        let lock_tmp_path = self.chromium_dir().join("chromium.lock.tmp");
        let lock_path = self.chromium_dir().join("chromium.lock");

        let build_value = build
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let url_value = url.unwrap_or("unknown");
        let checksum_value = checksum_sha256.unwrap_or("unknown");

        let contents = format!(
            "build={build_value}\nurl={url_value}\nchecksum_sha256={checksum_value}\ninstalled_at_unix={now}\n"
        );

        fs::write(&lock_tmp_path, contents)
            .map_err(|err| format!("failed to write {}: {err}", lock_tmp_path.display()))?;

        fs::rename(&lock_tmp_path, &lock_path).map_err(|err| {
            format!(
                "failed to atomically finalize {} to {}: {err}",
                lock_tmp_path.display(),
                lock_path.display()
            )
        })
    }

    fn set_link_target(&self, link_path: &Path, target: &Path) -> Result<(), String> {
        if link_path.exists() {
            let metadata = fs::symlink_metadata(link_path).map_err(|err| {
                format!(
                    "failed to stat existing link {}: {err}",
                    link_path.display()
                )
            })?;
            if metadata.file_type().is_dir() {
                fs::remove_dir_all(link_path).map_err(|err| {
                    format!(
                        "failed to remove existing dir {}: {err}",
                        link_path.display()
                    )
                })?;
            } else {
                fs::remove_file(link_path).map_err(|err| {
                    format!(
                        "failed to remove existing link {}: {err}",
                        link_path.display()
                    )
                })?;
            }
        }

        create_symlink_dir(target, link_path)
    }

    fn current_build_from_link(&self, current_link: &Path) -> Option<u64> {
        if !current_link.exists() {
            return None;
        }

        read_link_target(current_link)
            .ok()
            .as_deref()
            .and_then(build_from_path)
    }

    fn target_build(&self) -> u64 {
        if self.configured_build == 0 {
            PINNED_CHROMIUM_BUILD
        } else {
            self.configured_build
        }
    }

    fn chromium_dir(&self) -> PathBuf {
        self.root_dir.join("browser/chromium")
    }

    fn maybe_clear_macos_quarantine(&self, install_root: &Path) -> Result<(), String> {
        if !cfg!(target_os = "macos") {
            return Ok(());
        }

        let status = Command::new("xattr")
            .arg("-cr")
            .arg(install_root)
            .status()
            .map_err(|err| format!("failed to run xattr for {}: {err}", install_root.display()))?;
        if !status.success() {
            return Err(format!(
                "failed to clear macOS quarantine attributes for {}; run `xattr -cr {}`",
                install_root.display(),
                install_root.display()
            ));
        }

        Ok(())
    }
}

fn resolve_install_root(build_dir: &Path) -> Result<PathBuf, String> {
    let expected = [
        "chrome-linux64",
        "chrome-mac-arm64",
        "chrome-mac-x64",
        "usr/lib/chromium",
    ];
    for entry in expected {
        let candidate = build_dir.join(entry);
        if candidate.exists() {
            return Ok(candidate);
        }
    }

    let mut dirs = fs::read_dir(build_dir)
        .map_err(|err| format!("failed to read {}: {err}", build_dir.display()))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir());

    dirs.next().ok_or_else(|| {
        format!(
            "no extracted browser directory found in {}",
            build_dir.display()
        )
    })
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
        ("linux", "aarch64") => Ok(ChromiumPlatform::Linux64),
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

fn manifest_download_url(build: u64, platform: ChromiumPlatform) -> Result<Option<String>, String> {
    let response = match http_client()?
        .get("https://googlechromelabs.github.io/chrome-for-testing/known-good-versions-with-downloads.json")
        .send()
    {
        Ok(response) => response,
        Err(_) => return Ok(None),
    };
    if !response.status().is_success() {
        return Ok(None);
    }

    let text = response
        .text()
        .map_err(|err| format!("failed to read chrome-for-testing manifest body: {err}"))?;

    Ok(find_manifest_url_for_revision(
        text.as_str(),
        build,
        platform.as_str(),
    ))
}

fn debian_trixie_arm64_chromium_url() -> Result<Option<String>, String> {
    let response = match http_client()?
        .get("https://packages.debian.org/trixie/arm64/chromium/download")
        .send()
    {
        Ok(response) => response,
        Err(_) => return Ok(None),
    };
    if !response.status().is_success() {
        return Ok(None);
    }

    let text = response
        .text()
        .map_err(|err| format!("failed to read Debian package page body: {err}"))?;
    Ok(find_debian_deb_url(text.as_str()))
}

fn find_debian_deb_url(page: &str) -> Option<String> {
    let marker = "https://deb.debian.org/debian/pool/main/c/chromium/";
    let start = page.find(marker)?;
    let tail = &page[start..];
    if let Some(end) = tail.find("_arm64.deb") {
        return Some(format!("{}{}", &tail[..end], "_arm64.deb"));
    }
    let end = tail.find(".deb\"")?;
    Some(tail[..end + 4].to_string())
}

fn find_manifest_url_for_revision(manifest: &str, revision: u64, platform: &str) -> Option<String> {
    let compact: String = manifest.chars().filter(|c| !c.is_whitespace()).collect();
    let revision_marker = format!("\"revision\":\"{revision}\"");
    let start = compact.find(&revision_marker)?;
    let tail = &compact[start..];

    let platform_marker = format!("\"platform\":\"{platform}\"");
    let platform_index = tail.find(&platform_marker)?;

    if let Some(next_revision_rel) = tail[revision_marker.len()..].find("\"revision\":\"") {
        let next_revision_index = next_revision_rel + revision_marker.len();
        if next_revision_index < platform_index {
            return None;
        }
    }

    let platform_tail = &tail[platform_index..];

    let key = "\"url\":\"";
    let url_start = platform_tail.find(key)? + key.len();
    let url_tail = &platform_tail[url_start..];
    let url_end = url_tail.find('\"')?;
    Some(url_tail[..url_end].to_string())
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

        match key {
            "build" => metadata.build = value.parse::<u64>().ok(),
            "url" => metadata.url = Some(value.to_string()),
            "checksum_sha256" => metadata.checksum_sha256 = Some(value.to_string()),
            "installed_at_unix" => metadata.installed_at_unix = value.parse::<u64>().ok(),
            _ => {}
        }
    }

    Ok(metadata)
}

fn build_from_path(path: &Path) -> Option<u64> {
    for ancestor in path.ancestors() {
        let parsed = ancestor
            .file_name()
            .and_then(|value| value.to_str())
            .and_then(|value| value.parse::<u64>().ok());
        if parsed.is_some() {
            return parsed;
        }
    }
    None
}

fn read_link_target(path: &Path) -> Result<PathBuf, String> {
    fs::read_link(path).map_err(|err| format!("failed to read link {}: {err}", path.display()))
}

fn compute_sha256(path: &Path) -> Result<String, String> {
    let sha256sum = Command::new("sha256sum").arg(path).output();
    if let Ok(output) = sha256sum {
        if output.status.success() {
            return parse_checksum_output("sha256sum", &output.stdout);
        }
    }

    let shasum = Command::new("shasum")
        .arg("-a")
        .arg("256")
        .arg(path)
        .output()
        .map_err(|err| {
            format!(
                "failed to run checksum command for {}: {err}",
                path.display()
            )
        })?;
    if !shasum.status.success() {
        return Err(format!(
            "checksum command failed for {}: exit status {:?}",
            path.display(),
            shasum.status.code()
        ));
    }

    parse_checksum_output("shasum", &shasum.stdout)
}

fn parse_checksum_output(tool: &str, stdout: &[u8]) -> Result<String, String> {
    let text = String::from_utf8(stdout.to_vec())
        .map_err(|_| format!("{tool} output is not valid utf-8"))?;
    let first = text
        .split_whitespace()
        .next()
        .ok_or_else(|| format!("{tool} produced empty output"))?;
    Ok(first.to_string())
}

fn http_client() -> Result<reqwest::blocking::Client, String> {
    reqwest::blocking::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .map_err(|err| format!("failed to build http client: {err}"))
}

#[cfg(unix)]
fn create_symlink_dir(target: &Path, link_path: &Path) -> Result<(), String> {
    std::os::unix::fs::symlink(target, link_path).map_err(|err| {
        format!(
            "failed to create symlink {} -> {}: {err}",
            link_path.display(),
            target.display()
        )
    })
}

#[cfg(not(unix))]
fn create_symlink_dir(target: &Path, link_path: &Path) -> Result<(), String> {
    let _ = target;
    let _ = link_path;
    Err("symlink creation is only supported on unix in this build".to_string())
}

#[cfg(test)]
mod tests {
    use super::{
        ArtifactFormat, ArtifactSource, BrowserManager, BrowserMode, ChromiumPlatform,
        chromium_download_url, detect_platform, find_debian_deb_url,
        find_manifest_url_for_revision, parse_lock_metadata,
    };
    use crate::config::OmensConfig;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        let base = std::env::current_dir()
            .expect("cwd should exist")
            .join(".test-tmp");
        let _ = fs::create_dir_all(&base);
        base.join(format!("omens-browser-{name}-{nanos}"))
    }

    fn bundled_manager(root: PathBuf) -> BrowserManager {
        let mut config = OmensConfig::default();
        config.resolved.root_dir = root.clone();
        config.resolved.browser_user_data_dir = root.join("browser/profiles/default");
        BrowserManager::from_config(&config).expect("manager should build")
    }

    fn maybe_create_fake_archive(
        root: &Path,
        platform_dir: &str,
        file_name: &str,
    ) -> Option<PathBuf> {
        let zip_check = Command::new("zip").arg("-v").output();
        if zip_check.is_err() {
            return None;
        }

        let src_root = root.join("archive-src");
        let browser_dir = src_root.join(platform_dir);
        fs::create_dir_all(&browser_dir).ok()?;
        fs::write(browser_dir.join("chrome"), "binary").ok()?;

        let archive_path = root.join(file_name);
        let status = Command::new("zip")
            .arg("-qr")
            .arg(&archive_path)
            .arg(platform_dir)
            .current_dir(&src_root)
            .status()
            .ok()?;
        if !status.success() {
            return None;
        }

        Some(archive_path)
    }

    fn install_with_local_archive(
        manager: &BrowserManager,
        build: u64,
        archive_path: &Path,
    ) -> Result<(), String> {
        let source = ArtifactSource {
            url: format!("file://{}", archive_path.display()),
            expected_sha256: None,
            format: ArtifactFormat::Zip,
        };
        manager.finalize_install(build, &source).map(|_| ())
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
            detect_platform("linux", "aarch64").expect("linux arm should map to linux64"),
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

    #[test]
    fn install_cleans_stale_tmp_and_sets_current() {
        let root = unique_temp_dir("install");
        let chromium_dir = root.join("browser/chromium");
        fs::create_dir_all(&chromium_dir).expect("chromium dir should be created");
        fs::write(chromium_dir.join("chromium.lock.tmp"), "stale").expect("tmp should be created");

        let manager = bundled_manager(root.clone());
        let archive = match maybe_create_fake_archive(&root, "chrome-linux64", "test.zip") {
            Some(path) => path,
            None => return,
        };
        install_with_local_archive(&manager, 333, &archive).expect("install should succeed");

        let status = manager.status();
        assert!(status.is_installed);
        assert!(!status.lock_tmp_path.exists());
        assert!(status.current_path.exists());
        assert!(status.lock_path.exists());
    }

    #[test]
    fn rollback_switches_to_previous_build() {
        let root = unique_temp_dir("rollback");
        let manager = bundled_manager(root.clone());
        let archive = match maybe_create_fake_archive(&root, "chrome-linux64", "test.zip") {
            Some(path) => path,
            None => return,
        };

        install_with_local_archive(&manager, 111, &archive)
            .expect("initial install should succeed");
        install_with_local_archive(&manager, 999, &archive).expect("upgrade should succeed");
        let post_upgrade = manager.status();
        assert_eq!(post_upgrade.active_build, Some(999));
        assert!(post_upgrade.rollback_available);

        let rolled_back = manager.rollback().expect("rollback should succeed");
        assert_eq!(rolled_back.active_build, Some(111));
    }

    #[test]
    fn interrupted_install_cleans_partial_state() {
        let root = unique_temp_dir("interrupted");
        let manager = bundled_manager(root.clone());
        let source = ArtifactSource {
            url: "https://example.invalid/not-found.zip".to_string(),
            expected_sha256: None,
            format: ArtifactFormat::Zip,
        };

        let err = manager
            .finalize_install(444, &source)
            .expect_err("install should fail for invalid source");
        assert!(err.contains("failed to download") || err.contains("resolve"));

        let build_dir = root.join("browser/chromium/builds/444");
        assert!(!build_dir.exists());
        assert!(!root.join("browser/chromium/chromium.lock.tmp").exists());
    }

    #[test]
    fn checksum_mismatch_refuses_install_and_preserves_previous_state() {
        let root = unique_temp_dir("checksum-mismatch");
        let manager = bundled_manager(root.clone());
        let archive = match maybe_create_fake_archive(&root, "chrome-linux64", "test.zip") {
            Some(path) => path,
            None => return,
        };

        install_with_local_archive(&manager, 111, &archive)
            .expect("initial install should succeed");

        let source = ArtifactSource {
            url: format!("file://{}", archive.display()),
            expected_sha256: Some("deadbeef".to_string()),
            format: ArtifactFormat::Zip,
        };

        let err = manager
            .finalize_install(222, &source)
            .expect_err("install should fail on checksum mismatch");
        assert!(err.contains("checksum mismatch"));

        let status = manager.status();
        assert_eq!(status.active_build, Some(111));
    }

    #[test]
    fn reset_profile_clears_directory_contents() {
        let root = unique_temp_dir("profile");
        let profile_dir = root.join("browser/profiles/default");
        fs::create_dir_all(&profile_dir).expect("profile dir should be created");
        fs::write(profile_dir.join("cookies.db"), "data").expect("profile file should exist");

        let manager = bundled_manager(root.clone());
        manager.reset_profile().expect("reset should succeed");

        assert!(profile_dir.exists());
        assert!(!profile_dir.join("cookies.db").exists());
    }

    #[test]
    fn bundled_commands_refuse_system_mode() {
        let root = unique_temp_dir("system-mode");
        let mut config = OmensConfig::default();
        config.browser.mode = "system".to_string();
        config.resolved.root_dir = root.clone();
        config.resolved.browser_user_data_dir = root.join("browser/profiles/default");

        let manager = BrowserManager::from_config(&config).expect("manager should build");
        assert_eq!(manager.status().mode, BrowserMode::System);
        assert!(manager.install().is_err());
        assert!(manager.upgrade().is_err());
        assert!(manager.rollback().is_err());
    }

    #[test]
    fn manifest_url_resolution_finds_platform_url() {
        let manifest = r#"{
          "versions": [
            {
              "version": "133.0.6943.126",
              "revision": "1418433",
              "downloads": {
                "chrome": [
                  {"platform": "linux64", "url": "https://example/chrome-linux64.zip"},
                  {"platform": "mac-x64", "url": "https://example/chrome-mac-x64.zip"}
                ]
              }
            }
          ]
        }"#;

        let url = find_manifest_url_for_revision(manifest, 1418433, "linux64")
            .expect("url should be found");
        assert_eq!(url, "https://example/chrome-linux64.zip");
    }

    #[test]
    fn debian_fallback_url_parser_finds_arm64_deb_link() {
        let page = r#"
            <html><body>
              <a href="https://deb.debian.org/debian/pool/main/c/chromium/chromium_142.0.7444.162-1_arm64.deb">mirror</a>
            </body></html>
        "#;

        let url = find_debian_deb_url(page).expect("debian .deb url should be found");
        assert_eq!(
            url,
            "https://deb.debian.org/debian/pool/main/c/chromium/chromium_142.0.7444.162-1_arm64.deb"
        );
    }
}
