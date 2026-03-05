use crate::browse::{collapse_blank_lines, truncate_str};
use crate::browser::harness::BrowserHarness;
use serde_json::{Value, json};

pub fn tool_definitions() -> Vec<Value> {
    vec![
        json!({
            "type": "function",
            "name": "navigate",
            "description": "Navigate the browser to a URL",
            "parameters": {
                "type": "object",
                "properties": {
                    "url": { "type": "string", "description": "The URL to navigate to" }
                },
                "required": ["url"]
            }
        }),
        json!({
            "type": "function",
            "name": "page_content",
            "description": "Get the visible text content of the current page",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }),
        json!({
            "type": "function",
            "name": "click",
            "description": "Click an element matching a CSS selector",
            "parameters": {
                "type": "object",
                "properties": {
                    "selector": { "type": "string", "description": "CSS selector of the element to click" }
                },
                "required": ["selector"]
            }
        }),
        json!({
            "type": "function",
            "name": "type_text",
            "description": "Type text into an input element matching a CSS selector",
            "parameters": {
                "type": "object",
                "properties": {
                    "selector": { "type": "string", "description": "CSS selector of the input element" },
                    "text": { "type": "string", "description": "Text to type into the element" }
                },
                "required": ["selector", "text"]
            }
        }),
        json!({
            "type": "function",
            "name": "find_elements",
            "description": "Find elements matching a CSS selector and return their attributes",
            "parameters": {
                "type": "object",
                "properties": {
                    "selector": { "type": "string", "description": "CSS selector to search for" },
                    "max_results": { "type": "integer", "description": "Maximum number of results (default 20)" }
                },
                "required": ["selector"]
            }
        }),
        json!({
            "type": "function",
            "name": "scroll",
            "description": "Scroll the page up or down",
            "parameters": {
                "type": "object",
                "properties": {
                    "direction": { "type": "string", "enum": ["up", "down"], "description": "Scroll direction" },
                    "amount": { "type": "integer", "description": "Pixels to scroll (default 600)" }
                },
                "required": ["direction"]
            }
        }),
        json!({
            "type": "function",
            "name": "eval_js",
            "description": "Evaluate a JavaScript expression in the browser and return the result",
            "parameters": {
                "type": "object",
                "properties": {
                    "expression": { "type": "string", "description": "JavaScript expression to evaluate" }
                },
                "required": ["expression"]
            }
        }),
    ]
}

pub fn dispatch(
    name: &str,
    arguments: &str,
    harness: &dyn BrowserHarness,
    max_page_chars: u32,
) -> String {
    let args: Value = serde_json::from_str(arguments).unwrap_or(Value::Object(Default::default()));

    let result = match name {
        "navigate" => exec_navigate(&args, harness),
        "page_content" => exec_page_content(harness, max_page_chars),
        "click" => exec_click(&args, harness),
        "type_text" => exec_type_text(&args, harness),
        "find_elements" => exec_find_elements(&args, harness),
        "scroll" => exec_scroll(&args, harness),
        "eval_js" => exec_eval_js(&args, harness),
        _ => Err(format!("unknown tool: {name}")),
    };

    match result {
        Ok(output) => output,
        Err(e) => json!({"error": e}).to_string(),
    }
}

fn exec_navigate(args: &Value, harness: &dyn BrowserHarness) -> Result<String, String> {
    let url = args["url"].as_str().ok_or("missing 'url' parameter")?;
    harness.navigate(url)?;
    let final_url = harness.current_url().unwrap_or_else(|_| url.to_string());
    Ok(json!({"url": final_url}).to_string())
}

fn exec_page_content(harness: &dyn BrowserHarness, max_chars: u32) -> Result<String, String> {
    let js = "document.body.innerText";
    let text = harness.evaluate_js(js)?;

    // Clean up: collapse multiple blank lines
    let cleaned: String = collapse_blank_lines(&text);

    let max = max_chars as usize;
    if cleaned.len() > max {
        let truncated = truncate_str(&cleaned, max);
        Ok(format!("{truncated}\n[truncated at {max} chars]"))
    } else {
        Ok(cleaned)
    }
}

fn exec_click(args: &Value, harness: &dyn BrowserHarness) -> Result<String, String> {
    let selector = args["selector"]
        .as_str()
        .ok_or("missing 'selector' parameter")?;
    harness.click_and_wait(selector, 5000)?;
    Ok(json!({"clicked": selector}).to_string())
}

fn exec_type_text(args: &Value, harness: &dyn BrowserHarness) -> Result<String, String> {
    let selector = args["selector"]
        .as_str()
        .ok_or("missing 'selector' parameter")?;
    let text = args["text"].as_str().ok_or("missing 'text' parameter")?;
    harness.type_text(selector, text)?;
    Ok(json!({"typed": text, "into": selector}).to_string())
}

fn exec_find_elements(args: &Value, harness: &dyn BrowserHarness) -> Result<String, String> {
    let selector = args["selector"]
        .as_str()
        .ok_or("missing 'selector' parameter")?;
    let max_results = args["max_results"].as_u64().unwrap_or(20) as u32;

    let sel_json = serde_json::to_string(selector).unwrap_or_else(|_| "\"\"".to_string());
    let js = format!(
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
    );
    harness.evaluate_js(&js)
}

fn exec_scroll(args: &Value, harness: &dyn BrowserHarness) -> Result<String, String> {
    let direction = args["direction"]
        .as_str()
        .ok_or("missing 'direction' parameter")?;
    let amount = args["amount"].as_u64().unwrap_or(600) as u32;
    harness.scroll(direction, amount)?;
    Ok(json!({"scrolled": direction, "pixels": amount}).to_string())
}

fn exec_eval_js(args: &Value, harness: &dyn BrowserHarness) -> Result<String, String> {
    let expression = args["expression"]
        .as_str()
        .ok_or("missing 'expression' parameter")?;
    harness.evaluate_js(expression)
}
