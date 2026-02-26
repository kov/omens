use crate::browser::harness::BrowserHarness;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct AuthValidationConfig {
    pub base_url: String,
    pub login_url: String,
    pub required_marker: Option<String>,
    pub protected_probe_url: Option<String>,
    pub login_timeout: Duration,
    pub poll_interval: Duration,
}

#[derive(Debug, Clone)]
pub enum AuthError {
    AuthRequired(String),
    Runtime(String),
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::AuthRequired(msg) => write!(f, "{msg}"),
            AuthError::Runtime(msg) => write!(f, "{msg}"),
        }
    }
}

pub fn validate_session(
    browser: &dyn BrowserHarness,
    config: &AuthValidationConfig,
) -> Result<(), AuthError> {
    let current_url = browser.current_url().map_err(AuthError::Runtime)?;
    if current_url.starts_with(config.login_url.as_str()) {
        return Err(AuthError::AuthRequired(format!(
            "session redirected to login URL {}; run `omens auth bootstrap`",
            config.login_url
        )));
    }
    if !current_url.starts_with(config.base_url.as_str()) {
        return Err(AuthError::AuthRequired(format!(
            "session URL `{current_url}` is outside expected base URL {}; run `omens auth bootstrap`",
            config.base_url
        )));
    }

    if let Some(marker) = &config.required_marker {
        let present = browser.has_marker(marker).map_err(AuthError::Runtime)?;
        if !present {
            return Err(AuthError::AuthRequired(format!(
                "required authenticated marker `{marker}` is missing"
            )));
        }
    }

    if let Some(probe_url) = &config.protected_probe_url {
        let ok = browser
            .probe_authenticated(probe_url)
            .map_err(AuthError::Runtime)?;
        if !ok {
            return Err(AuthError::AuthRequired(format!(
                "protected probe failed at {}; run `omens auth bootstrap`",
                probe_url
            )));
        }
    }

    Ok(())
}

pub fn wait_for_login(
    browser: &dyn BrowserHarness,
    config: &AuthValidationConfig,
) -> Result<(), AuthError> {
    let deadline = Instant::now() + config.login_timeout;

    loop {
        match validate_session(browser, config) {
            Ok(()) => return Ok(()),
            Err(AuthError::Runtime(err)) => return Err(AuthError::Runtime(err)),
            Err(AuthError::AuthRequired(_)) => {
                if Instant::now() >= deadline {
                    return Err(AuthError::AuthRequired(
                        "login was not validated before timeout".to_string(),
                    ));
                }
                thread::sleep(config.poll_interval);
            }
        }
    }
}

pub struct EphemeralProfile {
    path: PathBuf,
}

impl EphemeralProfile {
    pub fn create(ephemeral_root: &Path) -> Result<Self, AuthError> {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AuthError::Runtime("system time before unix epoch".to_string()))?
            .as_nanos();

        let path = ephemeral_root.join(format!("run-{stamp}"));
        fs::create_dir_all(&path).map_err(|err| {
            AuthError::Runtime(format!(
                "failed to create ephemeral profile {}: {err}",
                path.display()
            ))
        })?;

        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for EphemeralProfile {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AuthError, AuthValidationConfig, EphemeralProfile, validate_session, wait_for_login,
    };
    use crate::browser::harness::BrowserHarness;
    use std::cell::RefCell;
    use std::fs;
    use std::path::PathBuf;
    use std::rc::Rc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[derive(Clone)]
    struct MockHarness {
        urls: Rc<RefCell<Vec<String>>>,
        marker_present: bool,
        probe_ok: bool,
    }

    impl MockHarness {
        fn new(urls: Vec<&str>, marker_present: bool, probe_ok: bool) -> Self {
            Self {
                urls: Rc::new(RefCell::new(
                    urls.into_iter().map(|v| v.to_string()).collect(),
                )),
                marker_present,
                probe_ok,
            }
        }
    }

    impl BrowserHarness for MockHarness {
        fn launch(&mut self, _url: &str) -> Result<(), String> {
            Ok(())
        }

        fn current_url(&self) -> Result<String, String> {
            let mut urls = self.urls.borrow_mut();
            if urls.len() > 1 {
                Ok(urls.remove(0))
            } else {
                Ok(urls
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "https://www.clubefii.com.br".to_string()))
            }
        }

        fn has_marker(&self, _marker: &str) -> Result<bool, String> {
            Ok(self.marker_present)
        }

        fn probe_authenticated(&self, _probe_url: &str) -> Result<bool, String> {
            Ok(self.probe_ok)
        }

        fn page_source(&self) -> Result<String, String> {
            Ok("<html>mock</html>".to_string())
        }

        fn capture_page_fingerprint(
            &self,
        ) -> Result<crate::browser::harness::PageFingerprint, String> {
            Ok(crate::browser::harness::PageFingerprint {
                url: self.current_url()?,
                title: "Mock Page".to_string(),
                candidate_selectors: Vec::new(),
            })
        }

        fn shutdown(&mut self) -> Result<(), String> {
            Ok(())
        }
    }

    fn base_config() -> AuthValidationConfig {
        AuthValidationConfig {
            base_url: "https://www.clubefii.com.br".to_string(),
            login_url: "https://www.clubefii.com.br/login".to_string(),
            required_marker: None,
            protected_probe_url: None,
            login_timeout: Duration::from_millis(50),
            poll_interval: Duration::from_millis(5),
        }
    }

    fn unique_temp_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::current_dir()
            .expect("cwd should exist")
            .join(".test-tmp")
            .join(format!("auth-{name}-{nanos}"))
    }

    #[test]
    fn validate_session_rejects_login_redirect() {
        let harness = MockHarness::new(vec!["https://www.clubefii.com.br/login"], true, true);
        let err = validate_session(&harness, &base_config()).expect_err("should fail");
        assert!(matches!(err, AuthError::AuthRequired(_)));
    }

    #[test]
    fn validate_session_accepts_marker_and_probe() {
        let mut config = base_config();
        config.required_marker = Some("dashboard".to_string());
        config.protected_probe_url = Some("https://www.clubefii.com.br/probe".to_string());

        let harness = MockHarness::new(vec!["https://www.clubefii.com.br/home"], true, true);
        validate_session(&harness, &config).expect("session should be valid");
    }

    #[test]
    fn wait_for_login_succeeds_after_redirect_clears() {
        let harness = MockHarness::new(
            vec![
                "https://www.clubefii.com.br/login",
                "https://www.clubefii.com.br/login",
                "https://www.clubefii.com.br/home",
            ],
            true,
            true,
        );

        wait_for_login(&harness, &base_config()).expect("login should eventually validate");
    }

    #[test]
    fn wait_for_login_times_out() {
        let harness = MockHarness::new(vec!["https://www.clubefii.com.br/login"], true, true);
        let err = wait_for_login(&harness, &base_config()).expect_err("should timeout");
        assert!(matches!(err, AuthError::AuthRequired(_)));
    }

    #[test]
    fn ephemeral_profile_is_cleaned_on_drop() {
        let root = unique_temp_dir("ephemeral-cleanup");
        fs::create_dir_all(&root).expect("root should be created");

        let profile_path = {
            let profile = EphemeralProfile::create(&root).expect("profile should be created");
            let marker = profile.path().join("marker.txt");
            fs::write(&marker, "ok").expect("marker should be written");
            profile.path().to_path_buf()
        };

        assert!(!profile_path.exists());
    }
}
