use futures_util::StreamExt;
use std::path::PathBuf;

use chromiumoxide::{Browser, BrowserConfig, Page};

pub trait BrowserHarness {
    fn launch(&mut self, url: &str) -> Result<(), String>;
    fn current_url(&self) -> Result<String, String>;
    fn has_marker(&self, marker: &str) -> Result<bool, String>;
    fn probe_authenticated(&self, probe_url: &str) -> Result<bool, String>;
    fn shutdown(&mut self) -> Result<(), String>;
}

pub struct ChromiumoxideHarness {
    browser_binary: PathBuf,
    profile_dir: PathBuf,
    runtime: tokio::runtime::Runtime,
    browser: Option<Browser>,
    page: Option<Page>,
    handler_task: Option<tokio::task::JoinHandle<()>>,
}

impl ChromiumoxideHarness {
    pub fn new(browser_binary: PathBuf, profile_dir: PathBuf) -> Result<Self, String> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|err| format!("failed to create tokio runtime for browser harness: {err}"))?;

        Ok(Self {
            browser_binary,
            profile_dir,
            runtime,
            browser: None,
            page: None,
            handler_task: None,
        })
    }

    fn page(&self) -> Result<&Page, String> {
        self.page
            .as_ref()
            .ok_or_else(|| "browser page is not initialized".to_string())
    }
}

impl BrowserHarness for ChromiumoxideHarness {
    fn launch(&mut self, url: &str) -> Result<(), String> {
        let config = BrowserConfig::builder()
            .chrome_executable(&self.browser_binary)
            .user_data_dir(&self.profile_dir)
            .with_head()
            .build()
            .map_err(|err| format!("failed to build browser config: {err}"))?;

        let (browser, page, handler_task) = self.runtime.block_on(async {
            let (browser, mut handler) = Browser::launch(config).await.map_err(|err| {
                let raw = err.to_string();
                if raw.contains("ld-linux-x86-64.so.2") {
                    return "failed to launch bundled browser on this host architecture; set `browser.mode=\"system\"` and configure `browser.system_binary_path` to a native browser binary".to_string();
                }
                format!("failed to launch chromiumoxide browser: {raw}")
            })?;

            let handler_task = tokio::spawn(async move {
                while let Some(next) = handler.next().await {
                    if next.is_err() {
                        break;
                    }
                }
            });

            let page = browser
                .new_page(url)
                .await
                .map_err(|err| format!("failed to open page {url}: {err}"))?;

            Ok::<_, String>((browser, page, handler_task))
        })?;

        self.browser = Some(browser);
        self.page = Some(page);
        self.handler_task = Some(handler_task);
        Ok(())
    }

    fn current_url(&self) -> Result<String, String> {
        let page = self.page()?.clone();
        self.runtime
            .block_on(async {
                page.url()
                    .await
                    .map_err(|err| format!("failed to read current URL: {err}"))
            })
            .and_then(|value| value.ok_or_else(|| "browser page URL is not available".to_string()))
    }

    fn has_marker(&self, marker: &str) -> Result<bool, String> {
        let page = self.page()?.clone();
        let selector = marker.to_string();
        self.runtime.block_on(async move {
            let found = page
                .find_elements(selector)
                .await
                .map_err(|err| format!("failed to search marker selector: {err}"))?;
            Ok(!found.is_empty())
        })
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
        if let Some(mut browser) = self.browser.take() {
            let _ = self.runtime.block_on(async { browser.close().await });
        }

        if let Some(task) = self.handler_task.take() {
            task.abort();
        }

        self.page = None;
        Ok(())
    }
}
