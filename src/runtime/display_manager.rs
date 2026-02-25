use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct DisplaySession {
    pub weston_pid: u32,
    pub wayvnc_pid: u32,
    pub runtime_dir: PathBuf,
    pub wayland_socket: String,
    pub listen_addr: String,
}

#[derive(Debug, Clone)]
pub struct DisplayStatus {
    pub running: bool,
    pub session: Option<DisplaySession>,
}

pub struct DisplayManager {
    display_dir: PathBuf,
    state_file: PathBuf,
    weston_log: PathBuf,
    wayvnc_log: PathBuf,
}

impl DisplayManager {
    pub fn new(root_dir: &Path) -> Self {
        let display_dir = root_dir.join("display");
        Self {
            state_file: display_dir.join("session.state"),
            weston_log: display_dir.join("weston.log"),
            wayvnc_log: display_dir.join("wayvnc.log"),
            display_dir,
        }
    }

    pub fn start(
        &self,
        listen_addr: &str,
        password_file: Option<&str>,
    ) -> Result<DisplaySession, String> {
        self.ensure_display_dir()?;

        if let Some(existing) = self.read_state()? {
            if self.is_alive(existing.weston_pid) && self.is_alive(existing.wayvnc_pid) {
                return Err("display session already running".to_string());
            }
        }

        if !listen_addr.starts_with("127.0.0.1:") && password_file.is_none() {
            return Err("non-local display bind requires --password-file for wayvnc".to_string());
        }

        let runtime_dir = self.display_dir.join("runtime");
        fs::create_dir_all(&runtime_dir)
            .map_err(|err| format!("failed to create {}: {err}", runtime_dir.display()))?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&runtime_dir, fs::Permissions::from_mode(0o700)).map_err(
                |err| {
                    format!(
                        "failed setting permissions on {}: {err}",
                        runtime_dir.display()
                    )
                },
            )?;
        }

        let wayland_socket = "omens-wayland-0".to_string();

        let weston_log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.weston_log)
            .map_err(|err| format!("failed to open {}: {err}", self.weston_log.display()))?;
        let weston_log_err = weston_log
            .try_clone()
            .map_err(|err| format!("failed to clone weston log handle: {err}"))?;

        let mut weston_cmd = Command::new("weston");
        weston_cmd
            .env("XDG_RUNTIME_DIR", &runtime_dir)
            .arg("--backend=headless")
            .arg(format!("--socket={wayland_socket}"))
            .arg("--idle-time=0")
            .stdout(Stdio::from(weston_log))
            .stderr(Stdio::from(weston_log_err));

        let weston_child = weston_cmd
            .spawn()
            .map_err(|err| format!("failed to launch weston: {err}"))?;

        let wayland_socket_path = runtime_dir.join(&wayland_socket);
        let deadline = Instant::now() + Duration::from_secs(8);
        while !wayland_socket_path.exists() {
            if Instant::now() >= deadline {
                let _ = Command::new("kill")
                    .arg("-TERM")
                    .arg(weston_child.id().to_string())
                    .status();
                return Err(format!(
                    "weston socket {} was not created in time",
                    wayland_socket_path.display()
                ));
            }
            thread::sleep(Duration::from_millis(100));
        }

        let wayvnc_log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.wayvnc_log)
            .map_err(|err| format!("failed to open {}: {err}", self.wayvnc_log.display()))?;
        let wayvnc_log_err = wayvnc_log
            .try_clone()
            .map_err(|err| format!("failed to clone wayvnc log handle: {err}"))?;

        let mut wayvnc_cmd = Command::new("wayvnc");
        wayvnc_cmd
            .env("XDG_RUNTIME_DIR", &runtime_dir)
            .arg("--socket")
            .arg(&wayland_socket);
        if let Some(password_file) = password_file {
            wayvnc_cmd.arg("--password-file").arg(password_file);
        }
        wayvnc_cmd
            .arg(listen_addr)
            .stdout(Stdio::from(wayvnc_log))
            .stderr(Stdio::from(wayvnc_log_err));

        let wayvnc_child = wayvnc_cmd
            .spawn()
            .map_err(|err| format!("failed to launch wayvnc: {err}"))?;

        thread::sleep(Duration::from_millis(250));
        if !self.is_alive(wayvnc_child.id()) {
            let _ = Command::new("kill")
                .arg("-TERM")
                .arg(weston_child.id().to_string())
                .status();
            return Err("wayvnc exited immediately; check display logs".to_string());
        }

        let session = DisplaySession {
            weston_pid: weston_child.id(),
            wayvnc_pid: wayvnc_child.id(),
            runtime_dir,
            wayland_socket,
            listen_addr: listen_addr.to_string(),
        };
        self.write_state(&session)?;
        Ok(session)
    }

    pub fn stop(&self) -> Result<(), String> {
        let Some(session) = self.read_state()? else {
            return Ok(());
        };

        let _ = self.kill_pid(session.wayvnc_pid);
        let _ = self.kill_pid(session.weston_pid);
        let _ = fs::remove_file(&self.state_file);
        Ok(())
    }

    pub fn status(&self) -> Result<DisplayStatus, String> {
        let Some(session) = self.read_state()? else {
            return Ok(DisplayStatus {
                running: false,
                session: None,
            });
        };

        let running = self.is_alive(session.weston_pid) && self.is_alive(session.wayvnc_pid);
        if !running {
            let _ = fs::remove_file(&self.state_file);
            return Ok(DisplayStatus {
                running: false,
                session: None,
            });
        }

        Ok(DisplayStatus {
            running: true,
            session: Some(session),
        })
    }

    fn ensure_display_dir(&self) -> Result<(), String> {
        fs::create_dir_all(&self.display_dir)
            .map_err(|err| format!("failed to create {}: {err}", self.display_dir.display()))
    }

    fn read_state(&self) -> Result<Option<DisplaySession>, String> {
        if !self.state_file.exists() {
            return Ok(None);
        }

        let text = fs::read_to_string(&self.state_file)
            .map_err(|err| format!("failed to read {}: {err}", self.state_file.display()))?;

        let mut weston_pid = None;
        let mut wayvnc_pid = None;
        let mut runtime_dir = None;
        let mut wayland_socket = None;
        let mut listen_addr = None;

        for line in text.lines() {
            let mut parts = line.splitn(2, '=');
            let key = parts.next().unwrap_or("").trim();
            let value = parts.next().unwrap_or("").trim();
            match key {
                "weston_pid" => weston_pid = value.parse::<u32>().ok(),
                "wayvnc_pid" => wayvnc_pid = value.parse::<u32>().ok(),
                "runtime_dir" => runtime_dir = Some(PathBuf::from(value)),
                "wayland_socket" => wayland_socket = Some(value.to_string()),
                "listen_addr" => listen_addr = Some(value.to_string()),
                _ => {}
            }
        }

        let session = DisplaySession {
            weston_pid: weston_pid.ok_or_else(|| "state missing weston_pid".to_string())?,
            wayvnc_pid: wayvnc_pid.ok_or_else(|| "state missing wayvnc_pid".to_string())?,
            runtime_dir: runtime_dir.ok_or_else(|| "state missing runtime_dir".to_string())?,
            wayland_socket: wayland_socket
                .ok_or_else(|| "state missing wayland_socket".to_string())?,
            listen_addr: listen_addr.ok_or_else(|| "state missing listen_addr".to_string())?,
        };

        Ok(Some(session))
    }

    fn write_state(&self, session: &DisplaySession) -> Result<(), String> {
        let body = format!(
            "weston_pid={}\nwayvnc_pid={}\nruntime_dir={}\nwayland_socket={}\nlisten_addr={}\n",
            session.weston_pid,
            session.wayvnc_pid,
            session.runtime_dir.display(),
            session.wayland_socket,
            session.listen_addr,
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
    use super::DisplayManager;
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
            .join(format!("display-{name}-{nanos}"))
    }

    #[test]
    fn status_returns_stopped_without_state() {
        let root = unique_temp_dir("status-empty");
        fs::create_dir_all(&root).expect("root should exist");
        let manager = DisplayManager::new(&root);
        let status = manager.status().expect("status should work");
        assert!(!status.running);
        assert!(status.session.is_none());
    }

    #[test]
    fn stop_without_state_is_ok() {
        let root = unique_temp_dir("stop-empty");
        fs::create_dir_all(&root).expect("root should exist");
        let manager = DisplayManager::new(&root);
        manager.stop().expect("stop should not fail");
    }
}
