use futures_util::StreamExt;
use serde::Deserialize;
use std::path::PathBuf;

use chromiumoxide::{Browser, BrowserConfig, Page};

pub trait BrowserHarness {
    fn launch(&mut self, url: &str) -> Result<(), String>;
    fn current_url(&self) -> Result<String, String>;
    fn has_marker(&self, marker: &str) -> Result<bool, String>;
    fn probe_authenticated(&self, probe_url: &str) -> Result<bool, String>;
    fn page_source(&self) -> Result<String, String>;
    fn capture_page_fingerprint(&self) -> Result<PageFingerprint, String>;
    fn shutdown(&mut self) -> Result<(), String>;
}

#[derive(Debug, Clone)]
pub struct PageFingerprint {
    pub url: String,
    pub title: String,
    pub candidate_selectors: Vec<CandidateSelector>,
}

#[derive(Debug, Clone)]
pub struct CandidateSelector {
    pub kind: SelectorKind,
    pub selector: String,
    pub count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectorKind {
    ListingItem,
    PaginationLink,
    DetailLink,
}

pub struct ChromiumoxideHarness {
    browser_binary: PathBuf,
    profile_dir: PathBuf,
    launch_env: Vec<(String, String)>,
    runtime: tokio::runtime::Runtime,
    browser: Option<Browser>,
    page: Option<Page>,
    handler_task: Option<tokio::task::JoinHandle<()>>,
}

impl ChromiumoxideHarness {
    pub fn new(
        browser_binary: PathBuf,
        profile_dir: PathBuf,
        launch_env: Vec<(String, String)>,
    ) -> Result<Self, String> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|err| format!("failed to create tokio runtime for browser harness: {err}"))?;

        Ok(Self {
            browser_binary,
            profile_dir,
            launch_env,
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

#[derive(Deserialize, Default)]
struct JsCandidateSelector {
    kind: String,
    selector: String,
    count: usize,
}

impl BrowserHarness for ChromiumoxideHarness {
    fn launch(&mut self, url: &str) -> Result<(), String> {
        let mut builder = BrowserConfig::builder()
            .chrome_executable(&self.browser_binary)
            .user_data_dir(&self.profile_dir)
            .with_head();
        for (key, value) in &self.launch_env {
            builder = builder.env(key.clone(), value.clone());
        }
        let config = builder
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
        let page = self.page()?.clone();
        let url = probe_url.to_string();
        self.runtime.block_on(async move {
            page.goto(&url)
                .await
                .map_err(|err| format!("probe navigation to {url} failed: {err}"))?;
            let nav = page
                .wait_for_navigation_response()
                .await
                .map_err(|err| format!("probe wait for response failed: {err}"))?;
            match nav {
                Some(req) => match &req.response {
                    Some(resp) => Ok(resp.status == 200),
                    None => Ok(false),
                },
                None => Ok(false),
            }
        })
    }

    fn page_source(&self) -> Result<String, String> {
        let page = self.page()?.clone();
        self.runtime.block_on(async move {
            page.content()
                .await
                .map_err(|err| format!("failed to get page content: {err}"))
        })
    }

    fn capture_page_fingerprint(&self) -> Result<PageFingerprint, String> {
        let url = self.current_url()?;
        let page = self.page()?.clone();

        let title: String = self
            .runtime
            .block_on(async {
                page.evaluate("document.title")
                    .await
                    .map_err(|err| format!("failed to evaluate document.title: {err}"))
            })?
            .into_value()
            .map_err(|err| format!("failed to deserialize title: {err}"))?;

        let candidates_js = r#"
            (function() {
                var results = [];
                // Look for common listing patterns
                var listingSelectors = [
                    'table tbody tr',
                    '.card', '.list-item', '.item',
                    'article', '[class*="news"]', '[class*="fato"]',
                    '.table-responsive tr', '.data-table tr'
                ];
                for (var i = 0; i < listingSelectors.length; i++) {
                    var sel = listingSelectors[i];
                    var count = document.querySelectorAll(sel).length;
                    if (count >= 2) {
                        results.push({kind: 'listing_item', selector: sel, count: count});
                    }
                }
                // Look for pagination
                var paginationSelectors = [
                    'a[rel="next"]', '.pagination a', '.next-page',
                    '[class*="paginat"] a', 'nav a[href*="page"]'
                ];
                for (var i = 0; i < paginationSelectors.length; i++) {
                    var sel = paginationSelectors[i];
                    var count = document.querySelectorAll(sel).length;
                    if (count >= 1) {
                        results.push({kind: 'pagination_link', selector: sel, count: count});
                    }
                }
                // Look for detail links
                var detailSelectors = [
                    'a[href*="detalhe"]', 'a[href*="detail"]',
                    'a[href*="noticia"]', 'a[href*="fato"]',
                    'td a[href]', '.item a[href]'
                ];
                for (var i = 0; i < detailSelectors.length; i++) {
                    var sel = detailSelectors[i];
                    var count = document.querySelectorAll(sel).length;
                    if (count >= 1) {
                        results.push({kind: 'detail_link', selector: sel, count: count});
                    }
                }
                return results;
            })()
        "#;

        let candidates: Vec<JsCandidateSelector> = self
            .runtime
            .block_on(async {
                page.evaluate(candidates_js)
                    .await
                    .map_err(|err| format!("failed to evaluate selector scan: {err}"))
            })?
            .into_value()
            .unwrap_or_default();

        let candidate_selectors = candidates
            .into_iter()
            .map(|c| CandidateSelector {
                kind: match c.kind.as_str() {
                    "listing_item" => SelectorKind::ListingItem,
                    "pagination_link" => SelectorKind::PaginationLink,
                    "detail_link" => SelectorKind::DetailLink,
                    _ => SelectorKind::ListingItem,
                },
                selector: c.selector,
                count: c.count,
            })
            .collect();

        Ok(PageFingerprint {
            url,
            title,
            candidate_selectors,
        })
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
