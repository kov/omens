use crate::browser::harness::BrowserHarness;
use crate::config::ChatConfig;
use serde_json::{Value, json};
use std::io::{self, BufRead, Write};

use super::tools;

const SYSTEM_INSTRUCTIONS: &str = "\
You are a browser automation agent. You have access to a headless browser and \
can navigate web pages, read content, click elements, type text, and run JavaScript.

When the user asks you to find information on the web, use the browser tools to:
1. Navigate to relevant URLs
2. Read page content
3. Click links and buttons to explore
4. Report your findings back to the user

Always describe what you're doing as you work. If a tool call fails, try an \
alternative approach (different selector, different URL, etc).";

pub fn run_chat_loop(harness: &mut dyn BrowserHarness, config: &ChatConfig) -> Result<(), String> {
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .map_err(|e| format!("failed to create HTTP client: {e}"))?;

    let tool_defs = tools::tool_definitions();
    let mut input: Vec<Value> = Vec::new();

    println!("omens chat — type a message, or /quit to exit");
    println!();

    let stdin = io::stdin();
    let mut reader = stdin.lock();

    loop {
        eprint!("you> ");
        io::stderr().flush().ok();

        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => break, // EOF
            Err(e) => {
                eprintln!("read error: {e}");
                break;
            }
            _ => {}
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed == "/quit" || trimmed == "/exit" {
            break;
        }

        input.push(json!({
            "role": "user",
            "content": trimmed
        }));

        // Tool-call loop
        loop {
            let body = json!({
                "model": &config.model,
                "instructions": SYSTEM_INSTRUCTIONS,
                "input": &input,
                "tools": &tool_defs,
            });

            let mut request = client
                .post(format!("{}/responses", config.base_url))
                .header("Content-Type", "application/json")
                .body(
                    serde_json::to_string(&body)
                        .map_err(|e| format!("JSON serialize error: {e}"))?,
                );

            if !config.api_key.is_empty() {
                request = request.header("Authorization", format!("Bearer {}", config.api_key));
            }

            let response = match request.send() {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("  API error: {e}");
                    // Pop the last user message so they can retry
                    if let Some(last) = input.last()
                        && last.get("role").and_then(|r| r.as_str()) == Some("user")
                    {
                        input.pop();
                    }
                    break;
                }
            };

            let status = response.status();
            let resp_text = response
                .text()
                .unwrap_or_else(|e| format!("{{\"error\": \"failed to read response: {e}\"}}"));

            if !status.is_success() {
                eprintln!("  API error ({}): {}", status, truncate(&resp_text, 500));
                if let Some(last) = input.last()
                    && last.get("role").and_then(|r| r.as_str()) == Some("user")
                {
                    input.pop();
                }
                break;
            }

            let resp: Value = match serde_json::from_str(&resp_text) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("  failed to parse API response: {e}");
                    break;
                }
            };

            let output = match resp.get("output").and_then(|o| o.as_array()) {
                Some(arr) => arr.clone(),
                None => {
                    eprintln!("  unexpected API response format (no 'output' array)");
                    break;
                }
            };

            // Separate function calls from messages
            let mut function_calls = Vec::new();
            let mut messages = Vec::new();

            for item in &output {
                match item.get("type").and_then(|t| t.as_str()) {
                    Some("function_call") => function_calls.push(item.clone()),
                    Some("message") => messages.push(item.clone()),
                    _ => {}
                }
            }

            // Append all output items to input for next turn
            input.extend(output);

            if function_calls.is_empty() {
                // Print message text
                for msg in &messages {
                    if let Some(content) = msg.get("content").and_then(|c| c.as_array()) {
                        for part in content {
                            if part.get("type").and_then(|t| t.as_str()) == Some("output_text")
                                && let Some(text) = part.get("text").and_then(|t| t.as_str())
                            {
                                println!();
                                println!("{text}");
                                println!();
                            }
                        }
                    }
                }
                break;
            }

            // Execute tools and append results
            for call in &function_calls {
                let name = call
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("unknown");
                let arguments = call
                    .get("arguments")
                    .and_then(|a| a.as_str())
                    .unwrap_or("{}");
                let call_id = call.get("call_id").and_then(|c| c.as_str()).unwrap_or("");

                eprintln!("  [tool] {name}({args})", args = truncate(arguments, 120));

                let result = tools::dispatch(name, arguments, harness, config.max_page_chars);

                input.push(json!({
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": result
                }));
            }
            // Loop back to call API with tool results
        }

        // Trim history to avoid unbounded growth
        trim_history(&mut input, 80);
    }

    Ok(())
}

/// Keep the last `max_items` items in the input array.
/// Adjusts the cut point backward so function_call/function_call_output
/// pairs are never split.
fn trim_history(input: &mut Vec<Value>, max_items: usize) {
    if input.len() <= max_items {
        return;
    }

    let excess = input.len() - max_items;
    let mut cut = excess;

    // If the kept portion starts with function_call_output, walk backward
    // to include the function_call that produced it, keeping the pair intact.
    while cut > 0 {
        let item_type = input[cut]
            .get("type")
            .and_then(|t| t.as_str())
            .unwrap_or("");
        if item_type == "function_call_output" {
            cut -= 1;
        } else {
            break;
        }
    }

    if cut > 0 {
        input.drain(..cut);
    }
}

fn truncate(s: &str, max: usize) -> &str {
    crate::browse::truncate_str(s, max)
}
