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

fn epoch_millis() -> Result<u128, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .map_err(|err| format!("system clock before UNIX epoch: {err}"))
}

#[cfg(test)]
mod tests {
    use super::FixtureWriter;
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
}
