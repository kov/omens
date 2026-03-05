pub mod commands;
pub mod session;

use chromiumoxide::{Browser, Page};
use futures_util::StreamExt;
use std::future::Future;

/// Connect to a running browser via CDP, get the active page, run the async closure, disconnect.
pub fn with_page<F, Fut, T>(port: u16, f: F) -> Result<T, String>
where
    F: FnOnce(Page) -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(|err| format!("failed to create tokio runtime: {err}"))?;

    runtime.block_on(async {
        let url = format!("http://127.0.0.1:{port}");
        let (browser, mut handler) = Browser::connect(&url)
            .await
            .map_err(|err| format!("failed to connect to browser at {url}: {err}"))?;

        let handler_task = tokio::spawn(async move {
            while let Some(next) = handler.next().await {
                if next.is_err() {
                    break;
                }
            }
        });

        // The handler needs time to discover existing targets after connect.
        // Poll pages() with a short retry loop.
        let mut page = None;
        for _ in 0..20 {
            if let Ok(pages) = browser.pages().await {
                if let Some(p) = pages.into_iter().next() {
                    page = Some(p);
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let page = page.ok_or_else(|| "no pages found in browser".to_string())?;

        let result = f(page).await;

        handler_task.abort();
        result
    })
}

/// Poll `document.readyState` until it reaches `target` (or beyond) or timeout.
/// State ordering: loading < interactive < complete.
/// Best-effort: returns Ok even on timeout.
pub async fn wait_for_ready_state(
    page: &Page,
    target: &str,
    timeout_ms: u64,
) -> Result<(), String> {
    let target_rank = match target {
        "interactive" => 1,
        "complete" => 2,
        _ => 1,
    };
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
    loop {
        let value = eval_on_page(page, "document.readyState").await?;
        let state = value.as_str().unwrap_or("loading");
        let rank = match state {
            "interactive" => 1,
            "complete" => 2,
            _ => 0,
        };
        if rank >= target_rank {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// Evaluate JS on a page. Must be called from within an async context.
pub async fn eval_on_page(page: &Page, js: &str) -> Result<serde_json::Value, String> {
    let result = page
        .evaluate(js.to_string())
        .await
        .map_err(|err| format!("JS evaluation failed: {err}"))?;
    let value: serde_json::Value = result.into_value().unwrap_or(serde_json::Value::Null);
    Ok(value)
}

/// Collapse runs of blank lines into a single blank line.
pub fn collapse_blank_lines(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut prev_blank = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            if !prev_blank {
                result.push('\n');
                prev_blank = true;
            }
        } else {
            result.push_str(line);
            result.push('\n');
            prev_blank = false;
        }
    }
    result
}

/// Build JS to find elements matching a selector and return their attributes.
pub fn find_elements_js(selector: &str, max_results: usize) -> String {
    let sel_json = serde_json::to_string(selector).unwrap_or_else(|_| "\"\"".to_string());
    format!(
        r#"(function() {{
            var els = document.querySelectorAll({sel_json});
            var results = [];
            var limit = {max_results};
            for (var i = 0; i < els.length && results.length < limit; i++) {{
                var el = els[i];
                results.push({{
                    tag: el.tagName.toLowerCase(),
                    text: (el.textContent || '').trim().substring(0, 200),
                    href: el.getAttribute('href') || '',
                    id: el.id || '',
                    class: el.className || '',
                    name: el.getAttribute('name') || '',
                    value: el.value || '',
                    type: el.getAttribute('type') || ''
                }});
            }}
            return results;
        }})()"#
    )
}

/// Truncate a string to at most `max` bytes, without splitting a UTF-8 character.
pub fn truncate_str(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}
