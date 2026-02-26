use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct FixtureWriter {
    fixtures_dir: PathBuf,
}

impl FixtureWriter {
    pub fn new(fixtures_dir: &Path) -> Self {
        Self {
            fixtures_dir: fixtures_dir.to_path_buf(),
        }
    }

    pub fn save_page(&self, section: &str, url: &str, html: &str) -> Result<PathBuf, String> {
        let section_dir = self.fixtures_dir.join(section);
        fs::create_dir_all(&section_dir)
            .map_err(|err| format!("failed to create {}: {err}", section_dir.display()))?;

        let stamp = epoch_millis()?;
        let filename = format!("{stamp}.html");
        let path = section_dir.join(&filename);

        let header = format!("<!-- url: {url} -->\n");
        let content = format!("{header}{html}");
        fs::write(&path, content)
            .map_err(|err| format!("failed to write fixture {}: {err}", path.display()))?;

        Ok(path)
    }
}

pub struct FailureBundleWriter {
    bundles_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct FailureBundleDiagnostics {
    pub section: String,
    pub url: String,
    pub error: String,
    pub recipe_id: Option<i64>,
}

impl FailureBundleWriter {
    pub fn new(bundles_dir: &Path) -> Self {
        Self {
            bundles_dir: bundles_dir.to_path_buf(),
        }
    }

    pub fn save_bundle(
        &self,
        diagnostics: &FailureBundleDiagnostics,
        page_source: Option<&str>,
    ) -> Result<PathBuf, String> {
        let stamp = epoch_millis()?;
        let bundle_dir = self
            .bundles_dir
            .join(format!("{}-{stamp}", diagnostics.section));
        fs::create_dir_all(&bundle_dir)
            .map_err(|err| format!("failed to create {}: {err}", bundle_dir.display()))?;

        let diag_json = format!(
            "{{\"section\":\"{}\",\"url\":\"{}\",\"error\":\"{}\",\"recipe_id\":{}}}",
            diagnostics.section,
            diagnostics.url,
            diagnostics.error.replace('\"', "\\\""),
            diagnostics
                .recipe_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "null".to_string()),
        );
        fs::write(bundle_dir.join("diagnostics.json"), &diag_json)
            .map_err(|err| format!("failed to write diagnostics: {err}"))?;

        if let Some(source) = page_source {
            fs::write(bundle_dir.join("page.html"), source)
                .map_err(|err| format!("failed to write page source: {err}"))?;
        }

        Ok(bundle_dir)
    }
}

fn epoch_millis() -> Result<u128, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .map_err(|err| format!("system clock before UNIX epoch: {err}"))
}

#[cfg(test)]
mod tests {
    use super::{FailureBundleDiagnostics, FailureBundleWriter, FixtureWriter};
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
            .join(format!("fixtures-{name}-{nanos}"))
    }

    #[test]
    fn fixture_writer_creates_section_file() {
        let root = unique_temp_dir("fixture-write");
        let writer = FixtureWriter::new(&root);
        let path = writer
            .save_page("news", "https://example.com/news", "<html>content</html>")
            .expect("should write fixture");

        assert!(path.exists());
        assert!(path.extension().is_some_and(|ext| ext == "html"));

        let content = fs::read_to_string(&path).expect("should read");
        assert!(content.contains("<!-- url: https://example.com/news -->"));
        assert!(content.contains("<html>content</html>"));

        // Verify it's under the section subdirectory
        assert!(path.parent().unwrap().ends_with("news"));
    }

    #[test]
    fn failure_bundle_creates_diagnostics_and_page() {
        let root = unique_temp_dir("bundle-write");
        let writer = FailureBundleWriter::new(&root);
        let diag = FailureBundleDiagnostics {
            section: "news".to_string(),
            url: "https://example.com/fail".to_string(),
            error: "selector not found".to_string(),
            recipe_id: Some(42),
        };

        let bundle_path = writer
            .save_bundle(&diag, Some("<html>broken</html>"))
            .expect("should write bundle");

        assert!(bundle_path.is_dir());
        let diag_path = bundle_path.join("diagnostics.json");
        assert!(diag_path.exists());
        let diag_content = fs::read_to_string(&diag_path).expect("should read diagnostics");
        assert!(diag_content.contains("\"section\":\"news\""));
        assert!(diag_content.contains("\"recipe_id\":42"));

        let page_path = bundle_path.join("page.html");
        assert!(page_path.exists());
    }

    #[test]
    fn failure_bundle_without_page_source() {
        let root = unique_temp_dir("bundle-no-page");
        let writer = FailureBundleWriter::new(&root);
        let diag = FailureBundleDiagnostics {
            section: "material-facts".to_string(),
            url: "https://example.com".to_string(),
            error: "timeout".to_string(),
            recipe_id: None,
        };

        let bundle_path = writer
            .save_bundle(&diag, None)
            .expect("should write bundle");

        assert!(bundle_path.join("diagnostics.json").exists());
        assert!(!bundle_path.join("page.html").exists());
    }
}
