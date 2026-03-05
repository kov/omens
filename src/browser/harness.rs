use futures_util::StreamExt;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use chromiumoxide::cdp::browser_protocol::network::{
    EventLoadingFailed, EventLoadingFinished, EventRequestWillBeSent,
};
use chromiumoxide::{Browser, BrowserConfig, Page};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScrollDirection {
    Up,
    Down,
}

impl ScrollDirection {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "up" => Ok(Self::Up),
            "down" => Ok(Self::Down),
            _ => Err(format!("invalid scroll direction: {s} (use up or down)")),
        }
    }

    pub fn dy(self, pixels: u32) -> i64 {
        match self {
            Self::Up => -(pixels as i64),
            Self::Down => pixels as i64,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Up => "up",
            Self::Down => "down",
        }
    }
}

pub trait BrowserHarness {
    fn launch(&mut self, url: &str) -> Result<(), String>;
    fn current_url(&self) -> Result<String, String>;
    fn has_marker(&self, marker: &str) -> Result<bool, String>;
    fn probe_authenticated(&self, probe_url: &str) -> Result<bool, String>;
    fn page_source(&self) -> Result<String, String>;
    fn navigate(&self, url: &str) -> Result<(), String>;
    fn click_and_wait(&self, selector: &str, settle_ms: u64) -> Result<(), String>;
    /// Best-effort: dismiss any blocking overlays/popups (e.g. ad modals).
    /// Errors are suppressed — the page is usable even if no overlay was found.
    fn dismiss_overlays(&self);
    fn discover_tab_anchors(&self) -> Result<Vec<TabAnchor>, String>;
    fn capture_tab_summary(&self) -> Result<TabSummary, String>;
    fn extract_table_rows(
        &self,
        selector_hint: &str,
        max_rows: usize,
    ) -> Result<Vec<Vec<String>>, String>;
    fn extract_repeating_group_rows(
        &self,
        container_hint: &str,
        child_selector: &str,
        field_ids: &[&str],
        max_rows: usize,
    ) -> Result<Vec<HashMap<String, String>>, String>;
    fn shutdown(&mut self) -> Result<(), String>;
    /// Find the href of the first element matching `selector`.
    fn find_link_href(&self, selector: &str) -> Result<Option<String>, String>;
    /// Find the href of the first link in a table row whose text contains `search_text`.
    /// Also looks for onclick-embedded URLs (window.open patterns).
    fn find_row_link_by_text(&self, search_text: &str) -> Result<Option<String>, String>;
    /// Focus an element and type text into it.
    fn type_text(&self, selector: &str, text: &str) -> Result<(), String>;
    /// Scroll the page up or down by the given number of pixels.
    fn scroll(&self, direction: ScrollDirection, pixels: u32) -> Result<(), String>;
    /// Evaluate a raw JS expression and return the JSON-stringified result.
    fn evaluate_js(&self, expression: &str) -> Result<String, String>;
}

#[derive(Debug, Clone)]
pub struct TabAnchor {
    pub anchor: String,
    pub label: String,
}

#[derive(Debug, Clone)]
pub struct TabSummary {
    pub tables: Vec<TableInfo>,
    pub link_patterns: Vec<LinkPattern>,
    pub repeating_groups: Vec<RepeatingGroup>,
    pub text_blocks: usize,
}

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub selector_hint: String,
    pub row_count: usize,
    pub column_count: usize,
    pub headers: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct LinkPattern {
    pub pattern: String,
    pub count: usize,
    pub sample_text: String,
}

#[derive(Debug, Clone)]
pub struct RepeatingGroup {
    pub container_hint: String,
    pub child_selector: String,
    pub count: usize,
    pub sample_fields: Vec<String>,
}

pub struct ChromiumoxideHarness {
    browser_binary: PathBuf,
    profile_dir: PathBuf,
    launch_env: Vec<(String, String)>,
    extra_args: Vec<String>,
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
        extra_args: Vec<String>,
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
            extra_args,
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
struct JsTabAnchor {
    anchor: String,
    label: String,
}

#[derive(Deserialize, Default)]
struct JsTableInfo {
    selector_hint: String,
    row_count: usize,
    column_count: usize,
    headers: Vec<String>,
}

#[derive(Deserialize, Default)]
struct JsLinkPattern {
    pattern: String,
    count: usize,
    sample_text: String,
}

#[derive(Deserialize, Default)]
struct JsRepeatingGroup {
    container_hint: String,
    child_selector: String,
    count: usize,
    sample_fields: Vec<String>,
}

impl BrowserHarness for ChromiumoxideHarness {
    fn launch(&mut self, url: &str) -> Result<(), String> {
        let mut builder = BrowserConfig::builder()
            .chrome_executable(&self.browser_binary)
            .user_data_dir(&self.profile_dir)
            .viewport(None)
            .with_head();
        let has_wayland = self.launch_env.iter().any(|(k, _)| k == "WAYLAND_DISPLAY");
        if has_wayland {
            builder = builder
                .arg(("ozone-platform", "wayland"))
                .arg(("force-device-scale-factor", "1"));
        }
        for arg in &self.extra_args {
            let stripped = arg.strip_prefix("--").unwrap_or(arg);
            if let Some((key, value)) = stripped.split_once('=') {
                builder = builder.arg((key, value));
            } else {
                builder = builder.arg((stripped, ""));
            }
        }
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

    fn navigate(&self, url: &str) -> Result<(), String> {
        let page = self.page()?.clone();
        let url = url.to_string();
        self.runtime.block_on(async move {
            page.goto(&url)
                .await
                .map_err(|err| format!("navigation to {url} failed: {err}"))?;
            page.wait_for_navigation()
                .await
                .map_err(|err| format!("wait for navigation failed: {err}"))?;
            Ok(())
        })
    }

    fn click_and_wait(&self, selector: &str, settle_ms: u64) -> Result<(), String> {
        let page = self.page()?.clone();
        let js = format!(
            r#"document.querySelector('{}').click()"#,
            selector.replace('\'', "\\'")
        );
        self.runtime.block_on(async move {
            let in_flight = Arc::new(AtomicUsize::new(0));

            let mut req_stream = page
                .event_listener::<EventRequestWillBeSent>()
                .await
                .map_err(|e| format!("listen request events: {e}"))?;
            let mut fin_stream = page
                .event_listener::<EventLoadingFinished>()
                .await
                .map_err(|e| format!("listen loading-finished events: {e}"))?;
            let mut fail_stream = page
                .event_listener::<EventLoadingFailed>()
                .await
                .map_err(|e| format!("listen loading-failed events: {e}"))?;

            // Drain events from request/finish/fail streams in a background task
            let counter = in_flight.clone();
            let drain_handle = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        Some(_) = req_stream.next() => {
                            counter.fetch_add(1, Ordering::SeqCst);
                        }
                        Some(_) = fin_stream.next() => {
                            counter.fetch_sub(1, Ordering::SeqCst);
                        }
                        Some(_) = fail_stream.next() => {
                            counter.fetch_sub(1, Ordering::SeqCst);
                        }
                        else => break,
                    }
                }
            });

            page.evaluate(js)
                .await
                .map_err(|err| format!("click on '{selector}' failed: {err}"))?;

            // Wait for network idle: no in-flight requests for 500ms, up to settle_ms max
            let idle_threshold = std::time::Duration::from_millis(500);
            let deadline =
                tokio::time::Instant::now() + std::time::Duration::from_millis(settle_ms);
            let mut idle_since = Option::<tokio::time::Instant>::None;

            loop {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    break;
                }

                let count = in_flight.load(Ordering::SeqCst);
                if count == 0 {
                    match idle_since {
                        Some(since) if now.duration_since(since) >= idle_threshold => break,
                        None => idle_since = Some(now),
                        _ => {}
                    }
                } else {
                    idle_since = None;
                }

                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }

            drain_handle.abort();
            Ok(())
        })
    }

    fn dismiss_overlays(&self) {
        // Best-effort: hide #modal_masterpage (ad popup + mask) and any other
        // visible blocking overlays. Silently ignores missing elements.
        let Ok(page) = self.page() else { return };
        let page = page.clone();
        let js = r#"
            (function() {
                var modal = document.getElementById('modal_masterpage');
                if (modal) modal.style.display = 'none';
                var mask = document.getElementById('mask');
                if (mask) mask.style.display = 'none';
                return true;
            })()
        "#;
        let _ = self
            .runtime
            .block_on(async move { page.evaluate(js).await });
    }

    fn discover_tab_anchors(&self) -> Result<Vec<TabAnchor>, String> {
        let page = self.page()?.clone();
        let js = r##"
            (function() {
                var results = [];
                var seen = {};
                var links = document.querySelectorAll('a[href^="#"]');
                for (var i = 0; i < links.length; i++) {
                    var a = links[i];
                    var anchor = a.getAttribute('href');
                    if (!anchor || anchor === '#' || seen[anchor]) continue;
                    var label = (a.textContent || '').trim().substring(0, 60);
                    if (!label) continue;
                    seen[anchor] = true;
                    results.push({anchor: anchor, label: label});
                }
                return results;
            })()
        "##;

        let js_tabs: Vec<JsTabAnchor> = self
            .runtime
            .block_on(async {
                page.evaluate(js)
                    .await
                    .map_err(|err| format!("tab discovery failed: {err}"))
            })?
            .into_value()
            .unwrap_or_default();

        Ok(js_tabs
            .into_iter()
            .map(|t| TabAnchor {
                anchor: t.anchor,
                label: t.label,
            })
            .collect())
    }

    fn capture_tab_summary(&self) -> Result<TabSummary, String> {
        let page = self.page()?.clone();

        let tables_js = r#"
            (function() {
                var results = [];
                var tables = document.querySelectorAll('table');
                for (var i = 0; i < tables.length; i++) {
                    var t = tables[i];
                    var rows = t.querySelectorAll('tbody tr');
                    if (rows.length === 0) continue;
                    var cols = 0;
                    var headers = [];
                    var ths = t.querySelectorAll('thead th, thead td');
                    for (var j = 0; j < ths.length; j++) {
                        headers.push((ths[j].textContent || '').trim().substring(0, 40));
                    }
                    var firstRow = rows[0];
                    cols = firstRow ? firstRow.querySelectorAll('td, th').length : 0;
                    var hint = '';
                    if (t.id) {
                        hint = '#' + t.id;
                    } else {
                        // Walk ancestors to find the nearest element with an id
                        var anc = t.parentElement;
                        var ancId = '';
                        while (anc && anc !== document.body) {
                            if (anc.id) { ancId = anc.id; break; }
                            anc = anc.parentElement;
                        }
                        var localSel = t.className ? '.' + t.className.trim().split(/\s+/)[0] : 'table';
                        if (ancId) hint = '#' + ancId + ' ' + localSel;
                        else if (t.className) hint = localSel;
                        else hint = 'table:nth-of-type(' + (i+1) + ')';
                    }
                    results.push({
                        selector_hint: hint,
                        row_count: rows.length,
                        column_count: cols,
                        headers: headers
                    });
                }
                return results;
            })()
        "#;

        let tables: Vec<JsTableInfo> = self
            .runtime
            .block_on(async {
                page.evaluate(tables_js)
                    .await
                    .map_err(|err| format!("table scan failed: {err}"))
            })?
            .into_value()
            .unwrap_or_default();

        let links_js = r#"
            (function() {
                var patterns = {};
                var samples = {};
                var links = document.querySelectorAll('a[href]');
                for (var i = 0; i < links.length; i++) {
                    var href = links[i].getAttribute('href') || '';
                    if (!href || href === '#' || href.startsWith('javascript')) continue;
                    // Extract pattern: replace IDs/numbers with {id}
                    var pattern = href.replace(/[?].*$/, '')
                                      .replace(/\d{4,}/g, '{id}')
                                      .replace(/\/[A-Z]{4}\d{2}/g, '/{ticker}');
                    if (!patterns[pattern]) {
                        patterns[pattern] = 0;
                        samples[pattern] = (links[i].textContent || '').trim().substring(0, 60);
                    }
                    patterns[pattern]++;
                }
                var results = [];
                for (var p in patterns) {
                    if (patterns[p] >= 2) {
                        results.push({pattern: p, count: patterns[p], sample_text: samples[p]});
                    }
                }
                results.sort(function(a,b) { return b.count - a.count; });
                return results.slice(0, 20);
            })()
        "#;

        let link_patterns: Vec<JsLinkPattern> = self
            .runtime
            .block_on(async {
                page.evaluate(links_js)
                    .await
                    .map_err(|err| format!("link pattern scan failed: {err}"))
            })?
            .into_value()
            .unwrap_or_default();

        // Detect repeating div-based structures (non-tabular data like court cases)
        let repeating_js = r##"
            (function() {
                var results = [];
                var containers = document.querySelectorAll('[id], [class]');
                for (var c = 0; c < containers.length; c++) {
                    var el = containers[c];
                    if (el.tagName === 'TABLE' || el.tagName === 'THEAD' || el.tagName === 'TBODY') continue;
                    var children = el.children;
                    if (children.length < 3) continue;
                    // Count children by their className
                    var classCounts = {};
                    for (var i = 0; i < children.length; i++) {
                        var raw = children[i].className;
                        var cls = (typeof raw === 'string') ? raw.trim() : '';
                        if (!cls) continue;
                        var key = children[i].tagName + '.' + cls.split(/\s+/)[0];
                        classCounts[key] = (classCounts[key] || 0) + 1;
                    }
                    for (var key in classCounts) {
                        if (classCounts[key] < 3) continue;
                        // Found a repeating group — extract sample fields from the first child
                        var hint = el.id ? '#' + el.id : (el.className ? '.' + el.className.split(/\s+/)[0] : '');
                        if (!hint) continue;
                        var childSel = key.replace('.', '.');
                        var firstChild = el.querySelector(childSel.replace(/^(\w+)\./, '$1.'));
                        var fields = [];
                        if (firstChild) {
                            var inner = firstChild.querySelectorAll('[id]');
                            for (var j = 0; j < inner.length && j < 8; j++) {
                                var fid = inner[j].id;
                                var ftxt = (inner[j].textContent || '').trim().substring(0, 40);
                                if (fid && ftxt) fields.push(fid + ': ' + ftxt);
                            }
                        }
                        results.push({
                            container_hint: hint,
                            child_selector: childSel,
                            count: classCounts[key],
                            sample_fields: fields
                        });
                    }
                }
                // Deduplicate: keep the group with most children per container
                var seen = {};
                var deduped = [];
                results.sort(function(a,b) { return b.count - a.count; });
                for (var r = 0; r < results.length; r++) {
                    if (!seen[results[r].container_hint]) {
                        seen[results[r].container_hint] = true;
                        deduped.push(results[r]);
                    }
                }
                return deduped.slice(0, 10);
            })()
        "##;

        let repeating_groups: Vec<JsRepeatingGroup> = self
            .runtime
            .block_on(async {
                page.evaluate(repeating_js)
                    .await
                    .map_err(|err| format!("repeating group scan failed: {err}"))
            })?
            .into_value()
            .unwrap_or_default();

        let text_blocks: usize = self
            .runtime
            .block_on(async {
                page.evaluate(
                    "document.querySelectorAll('p, article, .content, .description, .text').length",
                )
                .await
                .map_err(|err| format!("text block count failed: {err}"))
            })?
            .into_value()
            .unwrap_or(0);

        Ok(TabSummary {
            tables: tables
                .into_iter()
                .map(|t| TableInfo {
                    selector_hint: t.selector_hint,
                    row_count: t.row_count,
                    column_count: t.column_count,
                    headers: t.headers,
                })
                .collect(),
            link_patterns: link_patterns
                .into_iter()
                .map(|l| LinkPattern {
                    pattern: l.pattern,
                    count: l.count,
                    sample_text: l.sample_text,
                })
                .collect(),
            repeating_groups: repeating_groups
                .into_iter()
                .map(|g| RepeatingGroup {
                    container_hint: g.container_hint,
                    child_selector: g.child_selector,
                    count: g.count,
                    sample_fields: g.sample_fields,
                })
                .collect(),
            text_blocks,
        })
    }

    fn extract_table_rows(
        &self,
        selector_hint: &str,
        max_rows: usize,
    ) -> Result<Vec<Vec<String>>, String> {
        let page = self.page()?.clone();
        let sel = selector_hint.replace('\'', "\\'");
        let js = format!(
            r#"(function() {{
                var table = document.querySelector('{sel}');
                if (!table) return [];
                var rows = table.querySelectorAll('tbody tr');
                var results = [];
                var limit = {max_rows};
                for (var i = 0; i < rows.length && results.length < limit; i++) {{
                    var cells = rows[i].querySelectorAll('td');
                    if (cells.length === 0) continue;
                    var row = [];
                    for (var j = 0; j < cells.length; j++) {{
                        row.push((cells[j].textContent || '').trim().substring(0, 500));
                    }}
                    results.push(row);
                }}
                return results;
            }})()"#
        );
        let rows: Vec<Vec<String>> = self
            .runtime
            .block_on(async {
                page.evaluate(js)
                    .await
                    .map_err(|err| format!("table row extraction failed: {err}"))
            })?
            .into_value()
            .unwrap_or_default();
        Ok(rows)
    }

    fn extract_repeating_group_rows(
        &self,
        container_hint: &str,
        child_selector: &str,
        field_ids: &[&str],
        max_rows: usize,
    ) -> Result<Vec<HashMap<String, String>>, String> {
        let page = self.page()?.clone();
        let field_ids_json = serde_json::to_string(field_ids).unwrap_or_else(|_| "[]".to_string());
        let container = container_hint.replace('\'', "\\'");
        let child = child_selector.replace('\'', "\\'");
        let js = format!(
            r#"(function() {{
                var container = document.querySelector('{container}');
                if (!container) return [];
                var children = container.querySelectorAll('{child}');
                var fieldIds = {field_ids_json};
                var results = [];
                var limit = {max_rows};
                for (var i = 0; i < children.length && results.length < limit; i++) {{
                    var child = children[i];
                    var inner = child.querySelectorAll('[id]');
                    var idMap = {{}};
                    for (var k = 0; k < inner.length; k++) {{
                        var fid = inner[k].getAttribute('id');
                        if (fid) idMap[fid] = (inner[k].textContent || '').trim().substring(0, 500);
                    }}
                    var row = {{}};
                    for (var j = 0; j < fieldIds.length; j++) {{
                        var fid = fieldIds[j];
                        if (idMap[fid] !== undefined) row[fid] = idMap[fid];
                    }}
                    if (Object.keys(row).length > 0) results.push(row);
                }}
                return results;
            }})()"#
        );
        let rows: Vec<HashMap<String, String>> = self
            .runtime
            .block_on(async {
                page.evaluate(js)
                    .await
                    .map_err(|err| format!("repeating group extraction failed: {err}"))
            })?
            .into_value()
            .unwrap_or_default();
        Ok(rows)
    }

    fn find_link_href(&self, selector: &str) -> Result<Option<String>, String> {
        let page = self.page()?.clone();
        let sel_json = serde_json::to_string(selector).unwrap_or_else(|_| "\"\"".to_string());
        let js = format!(
            "(function() {{ \
                var el = document.querySelector({sel_json}); \
                if (!el) return null; \
                return el.href || el.getAttribute('href') || null; \
            }})()"
        );
        let result: Option<String> = self
            .runtime
            .block_on(async {
                page.evaluate(js)
                    .await
                    .map_err(|e| format!("find_link_href: {e}"))
            })?
            .into_value()
            .unwrap_or(None);
        Ok(result)
    }

    fn type_text(&self, selector: &str, text: &str) -> Result<(), String> {
        let page = self.page()?.clone();
        let sel_json = serde_json::to_string(selector).unwrap_or_else(|_| "\"\"".to_string());
        let text_json = serde_json::to_string(text).unwrap_or_else(|_| "\"\"".to_string());
        let js = format!(
            r#"(function() {{
                var el = document.querySelector({sel_json});
                if (!el) throw new Error('element not found: ' + {sel_json});
                el.focus();
                el.value = {text_json};
                el.dispatchEvent(new Event('input', {{bubbles: true}}));
                el.dispatchEvent(new Event('change', {{bubbles: true}}));
                return true;
            }})()"#
        );
        self.runtime
            .block_on(async {
                page.evaluate(js)
                    .await
                    .map_err(|e| format!("type_text failed: {e}"))
            })
            .map(|_| ())
    }

    fn scroll(&self, direction: ScrollDirection, pixels: u32) -> Result<(), String> {
        let page = self.page()?.clone();
        let dy = direction.dy(pixels);
        let js = format!("window.scrollBy(0, {dy})");
        self.runtime
            .block_on(async {
                page.evaluate(js)
                    .await
                    .map_err(|e| format!("scroll failed: {e}"))
            })
            .map(|_| ())
    }

    fn evaluate_js(&self, expression: &str) -> Result<String, String> {
        let page = self.page()?.clone();
        let expr = expression.to_string();
        self.runtime.block_on(async {
            let result = page
                .evaluate(format!("JSON.stringify({expr})"))
                .await
                .map_err(|e| format!("evaluate_js failed: {e}"))?;
            let value: serde_json::Value = result.into_value().unwrap_or(serde_json::Value::Null);
            match value {
                serde_json::Value::String(s) => Ok(s),
                other => Ok(other.to_string()),
            }
        })
    }

    fn find_row_link_by_text(&self, search_text: &str) -> Result<Option<String>, String> {
        let page = self.page()?.clone();
        let text_json = serde_json::to_string(search_text).unwrap_or_else(|_| "\"\"".to_string());
        let js = format!(
            r#"(function() {{
                var searchText = {text_json};
                var rows = document.querySelectorAll('table tbody tr');
                for (var i = 0; i < rows.length; i++) {{
                    if (!(rows[i].textContent || '').includes(searchText)) continue;
                    var links = rows[i].querySelectorAll('a');
                    for (var j = 0; j < links.length; j++) {{
                        var a = links[j];
                        var href = a.getAttribute('href') || '';
                        if (href.startsWith('http')) return href;
                        // onclick: window.open('URL')
                        var onclick = a.getAttribute('onclick') || '';
                        var m = onclick.match(/window\.open\(['"]([^'"]+)['"]/);
                        if (m) return m[1];
                    }}
                }}
                return null;
            }})()"#
        );
        let result: Option<String> = self
            .runtime
            .block_on(async {
                page.evaluate(js)
                    .await
                    .map_err(|e| format!("find_row_link_by_text: {e}"))
            })?
            .into_value()
            .unwrap_or(None);
        Ok(result)
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
