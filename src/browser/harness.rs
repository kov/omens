use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

pub trait BrowserHarness {
    fn launch(&mut self, url: &str) -> Result<(), String>;
    fn current_url(&self) -> Result<String, String>;
    fn has_marker(&self, marker: &str) -> Result<bool, String>;
    fn probe_authenticated(&self, probe_url: &str) -> Result<bool, String>;
    fn shutdown(&mut self) -> Result<(), String>;
}

pub struct CommandBrowserHarness {
    browser_binary: PathBuf,
    profile_dir: PathBuf,
    launched_url: Option<String>,
    child: Option<Child>,
}

impl CommandBrowserHarness {
    pub fn new(browser_binary: PathBuf, profile_dir: PathBuf) -> Self {
        Self {
            browser_binary,
            profile_dir,
            launched_url: None,
            child: None,
        }
    }

    fn state_dir(&self) -> PathBuf {
        self.profile_dir.join("auth_state")
    }

    fn current_url_file(&self) -> PathBuf {
        self.state_dir().join("current_url.txt")
    }

    fn marker_file(&self, marker: &str) -> PathBuf {
        self.state_dir().join("markers").join(marker)
    }
}

impl BrowserHarness for CommandBrowserHarness {
    fn launch(&mut self, url: &str) -> Result<(), String> {
        fs::create_dir_all(&self.profile_dir).map_err(|err| {
            format!(
                "failed to create profile directory {}: {err}",
                self.profile_dir.display()
            )
        })?;

        let child = Command::new(&self.browser_binary)
            .arg(format!("--user-data-dir={}", self.profile_dir.display()))
            .arg("--no-first-run")
            .arg("--no-default-browser-check")
            .arg(url)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|err| {
                format!(
                    "failed to launch browser {}: {err}",
                    self.browser_binary.display()
                )
            })?;

        self.launched_url = Some(url.to_string());
        self.child = Some(child);
        Ok(())
    }

    fn current_url(&self) -> Result<String, String> {
        let current_url_file = self.current_url_file();
        if current_url_file.exists() {
            let value = fs::read_to_string(&current_url_file)
                .map_err(|err| format!("failed to read {}: {err}", current_url_file.display()))?;
            return Ok(value.trim().to_string());
        }

        self.launched_url
            .clone()
            .ok_or_else(|| "browser was not launched before session checks".to_string())
    }

    fn has_marker(&self, marker: &str) -> Result<bool, String> {
        Ok(self.marker_file(marker).exists())
    }

    fn probe_authenticated(&self, probe_url: &str) -> Result<bool, String> {
        let response = reqwest::blocking::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(20))
            .build()
            .map_err(|err| format!("failed to construct probe client: {err}"))?
            .get(probe_url)
            .send()
            .map_err(|err| format!("probe request failed: {err}"))?;

        Ok(response.status().is_success())
    }

    fn shutdown(&mut self) -> Result<(), String> {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        Ok(())
    }
}

pub fn write_mock_current_url(profile_dir: &Path, url: &str) -> Result<(), String> {
    let state_dir = profile_dir.join("auth_state");
    fs::create_dir_all(&state_dir)
        .map_err(|err| format!("failed to create {}: {err}", state_dir.display()))?;
    fs::write(state_dir.join("current_url.txt"), url)
        .map_err(|err| format!("failed to write current url marker: {err}"))
}

pub fn write_mock_marker(profile_dir: &Path, marker: &str) -> Result<(), String> {
    let marker_dir = profile_dir.join("auth_state").join("markers");
    fs::create_dir_all(&marker_dir)
        .map_err(|err| format!("failed to create {}: {err}", marker_dir.display()))?;
    fs::write(marker_dir.join(marker), "ok")
        .map_err(|err| format!("failed to write marker {}: {err}", marker))
}
