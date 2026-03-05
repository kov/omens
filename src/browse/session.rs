use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct BrowseSession {
    pub pid: u32,
    pub port: u16,
    pub profile_dir: PathBuf,
}

pub struct BrowseSessionManager {
    browse_dir: PathBuf,
    state_file: PathBuf,
}

impl BrowseSessionManager {
    pub fn new(root_dir: &Path) -> Self {
        let browse_dir = root_dir.join("browse");
        Self {
            state_file: browse_dir.join("session.state"),
            browse_dir,
        }
    }

    pub fn start(
        &self,
        binary: &Path,
        profile_dir: &Path,
        port: u16,
        launch_env: &[(String, String)],
        extra_args: &[String],
    ) -> Result<BrowseSession, String> {
        self.ensure_browse_dir()?;

        if let Some(existing) = self.read_state()?
            && self.is_alive(existing.pid)
        {
            return Err(format!(
                "browse session already running (pid={}, port={})",
                existing.pid, existing.port
            ));
        }

        fs::create_dir_all(profile_dir).map_err(|err| {
            format!(
                "failed to create browser profile {}: {err}",
                profile_dir.display()
            )
        })?;

        let mut cmd = Command::new(binary);
        cmd.arg(format!("--user-data-dir={}", profile_dir.display()))
            .arg(format!("--remote-debugging-port={port}"))
            .arg("--no-first-run")
            .arg("--no-default-browser-check");

        for (key, value) in launch_env {
            cmd.env(key, value);
        }
        for arg in extra_args {
            cmd.arg(arg);
        }

        cmd.arg("about:blank");
        cmd.stdout(Stdio::null()).stderr(Stdio::null());

        let child = cmd
            .spawn()
            .map_err(|err| format!("failed to launch browser {}: {err}", binary.display()))?;

        let pid = child.id();

        // Poll CDP endpoint until it responds (up to 5s)
        let cdp_url = format!("http://127.0.0.1:{port}/json/version");
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .map_err(|err| format!("failed to create HTTP client: {err}"))?;

        let deadline = Instant::now() + Duration::from_secs(5);
        let mut cdp_ok = false;
        while Instant::now() < deadline {
            if !self.is_alive(pid) {
                return Err("browser exited before CDP became available".to_string());
            }
            if client.get(&cdp_url).send().is_ok() {
                cdp_ok = true;
                break;
            }
            thread::sleep(Duration::from_millis(200));
        }

        if !cdp_ok {
            let _ = Command::new("kill")
                .arg("-TERM")
                .arg(pid.to_string())
                .status();
            return Err(format!(
                "CDP endpoint {cdp_url} did not become available in time"
            ));
        }

        let session = BrowseSession {
            pid,
            port,
            profile_dir: profile_dir.to_path_buf(),
        };
        self.write_state(&session)?;

        Ok(session)
    }

    pub fn stop(&self) -> Result<(), String> {
        let Some(session) = self.read_state()? else {
            return Ok(());
        };

        let _ = self.kill_pid(session.pid);
        let _ = fs::remove_file(&self.state_file);
        Ok(())
    }

    pub fn status(&self) -> Result<Option<BrowseSession>, String> {
        let Some(session) = self.read_state()? else {
            return Ok(None);
        };

        if !self.is_alive(session.pid) {
            let _ = fs::remove_file(&self.state_file);
            return Ok(None);
        }

        Ok(Some(session))
    }

    pub fn read_state(&self) -> Result<Option<BrowseSession>, String> {
        let text = match fs::read_to_string(&self.state_file) {
            Ok(text) => text,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(format!(
                    "failed to read {}: {err}",
                    self.state_file.display()
                ));
            }
        };

        let mut pid = None;
        let mut port = None;
        let mut profile_dir = None;

        for line in text.lines() {
            let mut parts = line.splitn(2, '=');
            let key = parts.next().unwrap_or("").trim();
            let value = parts.next().unwrap_or("").trim();
            match key {
                "pid" => pid = value.parse::<u32>().ok(),
                "port" => port = value.parse::<u16>().ok(),
                "profile_dir" => profile_dir = Some(PathBuf::from(value)),
                _ => {}
            }
        }

        let session = BrowseSession {
            pid: pid.ok_or_else(|| "state missing pid".to_string())?,
            port: port.ok_or_else(|| "state missing port".to_string())?,
            profile_dir: profile_dir.ok_or_else(|| "state missing profile_dir".to_string())?,
        };

        Ok(Some(session))
    }

    fn ensure_browse_dir(&self) -> Result<(), String> {
        fs::create_dir_all(&self.browse_dir)
            .map_err(|err| format!("failed to create {}: {err}", self.browse_dir.display()))
    }

    fn write_state(&self, session: &BrowseSession) -> Result<(), String> {
        let body = format!(
            "pid={}\nport={}\nprofile_dir={}\n",
            session.pid,
            session.port,
            session.profile_dir.display(),
        );

        fs::write(&self.state_file, body)
            .map_err(|err| format!("failed to write {}: {err}", self.state_file.display()))
    }

    fn kill_pid(&self, pid: u32) -> Result<(), String> {
        let status = Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status()
            .map_err(|err| format!("failed to execute kill for pid {pid}: {err}"))?;
        if !status.success() {
            return Err(format!("kill failed for pid {pid} with status {status}"));
        }
        Ok(())
    }

    fn is_alive(&self, pid: u32) -> bool {
        PathBuf::from(format!("/proc/{pid}")).exists()
    }
}

#[cfg(test)]
mod tests {
    use super::BrowseSessionManager;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::current_dir()
            .expect("cwd should exist")
            .join(".test-tmp")
            .join(format!("browse-{name}-{nanos}"))
    }

    #[test]
    fn status_returns_none_without_state() {
        let root = unique_temp_dir("status-empty");
        fs::create_dir_all(&root).expect("root should exist");
        let manager = BrowseSessionManager::new(&root);
        let status = manager.status().expect("status should work");
        assert!(status.is_none());
    }

    #[test]
    fn stop_without_state_is_ok() {
        let root = unique_temp_dir("stop-empty");
        fs::create_dir_all(&root).expect("root should exist");
        let manager = BrowseSessionManager::new(&root);
        manager.stop().expect("stop should not fail");
    }

    #[test]
    fn read_state_parses_written_state() {
        let root = unique_temp_dir("read-state");
        fs::create_dir_all(root.join("browse")).expect("browse dir should exist");
        let manager = BrowseSessionManager::new(&root);

        let state_content = "pid=12345\nport=9222\nprofile_dir=/tmp/test-profile\n";
        fs::write(&manager.state_file, state_content).expect("state should be written");

        let session = manager
            .read_state()
            .expect("read_state should succeed")
            .expect("session should exist");
        assert_eq!(session.pid, 12345);
        assert_eq!(session.port, 9222);
        assert_eq!(session.profile_dir, PathBuf::from("/tmp/test-profile"));
    }

    #[test]
    fn status_cleans_stale_state() {
        let root = unique_temp_dir("stale-state");
        fs::create_dir_all(root.join("browse")).expect("browse dir should exist");
        let manager = BrowseSessionManager::new(&root);

        // Write state with a PID that definitely doesn't exist
        let state_content = "pid=999999999\nport=9222\nprofile_dir=/tmp/test\n";
        fs::write(&manager.state_file, state_content).expect("state should be written");

        let status = manager.status().expect("status should work");
        assert!(status.is_none());
        assert!(!manager.state_file.exists());
    }
}
