use crate::browser::harness::ScrollDirection;

use super::{
    collapse_blank_lines, eval_on_page, find_elements_js, truncate_str, wait_for_ready_state,
    with_page,
};

pub fn navigate(port: u16, url: &str) -> Result<(), String> {
    let url = url.to_string();
    with_page(port, |page| async move {
        // Try page.goto() first for proper error detection (bad URLs, network errors).
        // Use a 10s timeout — if the page loads slowly due to subresources, fall back.
        let goto_result =
            tokio::time::timeout(std::time::Duration::from_secs(10), page.goto(&url)).await;

        match goto_result {
            Ok(Ok(_)) => {
                // Navigation completed normally
            }
            Ok(Err(err)) => {
                return Err(format!("navigation to {url} failed: {err}"));
            }
            Err(_timeout) => {
                // Timeout — check if the page actually navigated but has slow subresources
                let state = eval_on_page(&page, "document.readyState").await?;
                let ready = state.as_str().unwrap_or("loading");
                if ready != "interactive" && ready != "complete" {
                    // Page didn't reach interactive — fall back to JS navigation
                    let url_json =
                        serde_json::to_string(&url).unwrap_or_else(|_| "\"\"".to_string());
                    eval_on_page(&page, &format!("window.location.href = {url_json}")).await?;
                    wait_for_ready_state(&page, "interactive", 8000).await?;
                }
            }
        }

        let value = eval_on_page(&page, "window.location.href").await?;
        let final_url = value.as_str().unwrap_or(&url);
        println!("{final_url}");
        Ok(())
    })
}

pub fn content(port: u16, max_chars: u32, full: bool) -> Result<(), String> {
    with_page(port, |page| async move {
        let js = if full {
            "document.body.innerText"
        } else {
            "(document.querySelector('main, article, [role=\"main\"], #content, .content') || document.body).innerText"
        };
        let value = eval_on_page(&page, js).await?;
        let text = value.as_str().unwrap_or("");
        let cleaned = collapse_blank_lines(text);
        let max = max_chars as usize;
        if cleaned.len() > max {
            print!("{}", truncate_str(&cleaned, max));
            println!("\n[truncated at {max} chars]");
        } else {
            print!("{cleaned}");
        }
        Ok(())
    })
}

pub fn click(port: u16, selector: &str) -> Result<(), String> {
    let sel_json = serde_json::to_string(selector).unwrap_or_else(|_| "\"\"".to_string());
    let js = format!(
        r#"(function() {{
            var el = document.querySelector({sel_json});
            if (!el) throw new Error('element not found: ' + {sel_json});
            el.scrollIntoView({{block: 'center'}});
            el.click();
            return 'ok';
        }})()"#
    );
    with_page(port, |page| async move {
        eval_on_page(&page, &js).await?;
        // Wait for potential navigation/XHR to settle
        wait_for_ready_state(&page, "interactive", 3000).await?;
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        println!("ok");
        Ok(())
    })
}

pub fn type_text(port: u16, selector: &str, text: &str) -> Result<(), String> {
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
            return 'ok';
        }})()"#
    );
    with_page(port, |page| async move {
        eval_on_page(&page, &js).await?;
        println!("ok");
        Ok(())
    })
}

pub fn find(port: u16, selector: &str, max_results: usize) -> Result<(), String> {
    let js = find_elements_js(selector, max_results);
    with_page(port, |page| async move {
        let value = eval_on_page(&page, &js).await?;
        let output = serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string());
        println!("{output}");
        Ok(())
    })
}

pub fn scroll(port: u16, direction: ScrollDirection, pixels: u32) -> Result<(), String> {
    let js = format!("window.scrollBy(0, {})", direction.dy(pixels));
    with_page(port, |page| async move {
        eval_on_page(&page, &js).await?;
        println!("ok");
        Ok(())
    })
}

pub fn eval(port: u16, expression: &str) -> Result<(), String> {
    let expression = expression.to_string();
    with_page(port, |page| async move {
        let value = eval_on_page(&page, &expression).await?;
        match value {
            serde_json::Value::String(s) => println!("{s}"),
            serde_json::Value::Null => println!("null"),
            serde_json::Value::Bool(b) => println!("{b}"),
            serde_json::Value::Number(n) => println!("{n}"),
            other => {
                let pretty =
                    serde_json::to_string_pretty(&other).unwrap_or_else(|_| other.to_string());
                println!("{pretty}");
            }
        }
        Ok(())
    })
}

pub fn source(port: u16) -> Result<(), String> {
    with_page(port, |page| async move {
        let value = eval_on_page(&page, "document.documentElement.outerHTML").await?;
        let html = value.as_str().unwrap_or("");
        println!("{html}");
        Ok(())
    })
}

pub fn url(port: u16) -> Result<(), String> {
    with_page(port, |page| async move {
        let value = eval_on_page(&page, "window.location.href").await?;
        let url = value.as_str().unwrap_or("");
        println!("{url}");
        Ok(())
    })
}

pub fn links(port: u16, contains: Option<&str>, max_results: usize) -> Result<(), String> {
    let contains = contains.map(|s| s.to_lowercase());
    with_page(port, |page| async move {
        let js = r#"(function() {
            var seen = {};
            var results = [];
            var regions = 'nav, header, main, footer, aside, [role=navigation], [role=main], [role=banner], [role=contentinfo]';

            function getRegion(el) {
                var node = el;
                while (node && node !== document.body) {
                    if (node.matches && node.matches(regions)) {
                        var tag = node.tagName.toLowerCase();
                        var role = node.getAttribute('role') || '';
                        if (role === 'navigation' || tag === 'nav') return 'nav';
                        if (role === 'banner' || tag === 'header') return 'header';
                        if (role === 'main' || tag === 'main') return 'main';
                        if (role === 'contentinfo' || tag === 'footer') return 'footer';
                        if (tag === 'aside') return 'aside';
                        return tag;
                    }
                    node = node.parentElement;
                }
                return 'page';
            }

            var anchors = document.querySelectorAll('a[href]');
            for (var i = 0; i < anchors.length; i++) {
                var a = anchors[i];
                var text = (a.textContent || '').trim().substring(0, 80);
                if (!text) continue;
                var href = a.getAttribute('href') || '';
                if (href === '#' || href.indexOf('javascript:') === 0) continue;
                var key = text + '\t' + href;
                if (seen[key]) continue;
                seen[key] = true;
                results.push({text: text, href: href, region: getRegion(a)});
            }

            var buttons = document.querySelectorAll('button');
            for (var j = 0; j < buttons.length; j++) {
                var btn = buttons[j];
                var btxt = (btn.textContent || '').trim().substring(0, 80);
                if (!btxt) continue;
                var bkey = btxt + '\t(button)';
                if (seen[bkey]) continue;
                seen[bkey] = true;
                results.push({text: btxt, href: '(button)', region: getRegion(btn)});
            }

            return results;
        })()"#;

        let value = eval_on_page(&page, js).await?;
        let entries = value.as_array().ok_or("unexpected JS result for links")?;

        let mut count = 0;
        for entry in entries {
            if count >= max_results {
                break;
            }
            let text = entry["text"].as_str().unwrap_or("");
            let href = entry["href"].as_str().unwrap_or("");
            let region = entry["region"].as_str().unwrap_or("page");

            if let Some(ref filter) = contains {
                let text_lower = text.to_lowercase();
                let href_lower = href.to_lowercase();
                if !text_lower.contains(filter.as_str()) && !href_lower.contains(filter.as_str()) {
                    continue;
                }
            }

            println!("[{region}] {text} → {href}");
            count += 1;
        }
        Ok(())
    })
}
