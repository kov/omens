---
name: browse
description: Interactive CDP-controlled browser session for navigating websites, extracting content, and interacting with dynamic pages. Use when you need to explore an unfamiliar site, scrape data, fill forms, or click through JS-heavy UIs.
---

# Skill: omens browse

`browse` provides a persistent CDP-controlled browser session. Start once,
run many commands against it. Works with any website.

**Always invoke via `cargo run --`:**

```bash
cargo run -- browse <subcommand>
```

---

## Session lifecycle

```bash
cargo run -- browse start --display      # start (needs display running)
cargo run -- browse status               # check if running + port/pid
cargo run -- browse stop                 # kill session
```

The session persists across commands. The display must be running first
(`cargo run -- display start`).

---

## Navigation

```bash
cargo run -- browse navigate <url>       # goto with error detection
cargo run -- browse url                  # print current URL
```

`navigate` uses `page.goto()` with a 10s timeout for proper error detection
(catches bad URLs, network errors). If the timeout fires but the page reached
`interactive`/`complete` readyState (slow subresources), it treats that as
success. Otherwise falls back to JS `window.location.href` assignment with
an 8s readyState wait.

---

## Content extraction

```bash
cargo run -- browse content              # main-content text (skips nav/footer)
cargo run -- browse content --full       # full document.body text
cargo run -- browse content --max-chars 5000
cargo run -- browse source               # raw HTML
```

`content` defaults to semantic extraction — it looks for `main`, `article`,
`[role="main"]`, `#content`, `.content`, falling back to `document.body`.
Use `--full` when the page lacks semantic landmarks (most sites outside of
blogs/articles).

---

## Discovery

### links — find what's on the page

```bash
cargo run -- browse links                        # all links + buttons
cargo run -- browse links --contains "keyword"   # filter by text or href
cargo run -- browse links --max 50               # limit results (default 200)
```

Extracts all `<a>` and `<button>` elements with their semantic region
(`nav`, `header`, `main`, `footer`, `aside`, `page`). Output:

```
[nav] Filtro de Ações → https://br.investing.com/stock-screener/
[main] Indicadores → /equities/anima-on-ratios
[main] Entrar → (button)
[footer] Ajuda → /about/help
```

Filtering is case-insensitive on both text and href. Skips empty-text
elements, `href="#"`, and `javascript:` links. Deduplicates by (text, href).

**This is the primary discovery tool for unfamiliar sites.** Use it before
navigating or clicking to find targets.

### find — DOM element search

```bash
cargo run -- browse find "css-selector" --max 20
```

Returns JSON with tag, text, href, id, class, name, value, type for each
matching element. Useful for inspecting form fields, buttons, etc.

---

## Interaction

```bash
cargo run -- browse click "css-selector"         # click + wait for settle
cargo run -- browse type "css-selector" "text"   # fill input field
cargo run -- browse scroll down 600              # scroll (up|down) [pixels]
```

`click` scrolls the element into view, clicks it, then waits for
`document.readyState` to reach interactive (up to 3s) plus a 300ms settle.

`type` sets the value and dispatches `input` + `change` events. This works
for simple HTML inputs but **not for React/controlled inputs** — see the
eval pattern below.

---

## JavaScript evaluation

```bash
cargo run -- browse eval "document.title"                          # → My Page
cargo run -- browse eval "document.querySelectorAll('a').length"   # → 42
cargo run -- browse eval "({foo: 1, bar: 2})"                      # → pretty JSON
```

Strings print without quotes. Objects/arrays print as pretty JSON. Null,
booleans, and numbers print as-is.

### React/controlled input pattern

For inputs managed by React or similar frameworks, `type` won't trigger
state updates. Use the native value setter:

```bash
cargo run -- browse eval "(function() {
  var input = document.querySelectorAll('input[type=search]')[1];
  input.focus();
  var setter = Object.getOwnPropertyDescriptor(
    window.HTMLInputElement.prototype, 'value').set;
  setter.call(input, 'search term');
  input.dispatchEvent(new Event('input', {bubbles: true}));
  return 'ok';
})()"
```

### Clicking elements by text content

When CSS selectors aren't specific enough, find and click by text:

```bash
cargo run -- browse eval "(function() {
  var items = document.querySelectorAll('li, button, a, div');
  for (var i = 0; i < items.length; i++) {
    var t = (items[i].textContent || '').trim();
    if (t === 'Exact Button Text') {
      items[i].click();
      return 'clicked';
    }
  }
  return 'not found';
})()"
```

**Shell escaping note:** `!==` gets mangled by bash/fish. Use `>= 0` or
`> -1` instead of `!== -1` for `indexOf` checks.

### Extracting a specific text region

```bash
cargo run -- browse eval "(function() {
  var text = document.body.innerText;
  var idx = text.indexOf('Section Header');
  if (idx >= 0) return text.substring(idx, idx + 800);
  return 'not found';
})()"
```

---

## Typical workflow

```bash
# 1. Start session
cargo run -- browse start --display

# 2. Navigate to a site
cargo run -- browse navigate "https://example.com"

# 3. Discover what's available
cargo run -- browse links --contains "data"

# 4. Navigate to a target
cargo run -- browse navigate "https://example.com/data-page"

# 5. Extract content
cargo run -- browse content --max-chars 5000

# 6. Interact with dynamic elements
cargo run -- browse type "input[name=q]" "search query"
cargo run -- browse click "button[type=submit]"

# 7. Read results
cargo run -- browse content

# 8. Done
cargo run -- browse stop
```
