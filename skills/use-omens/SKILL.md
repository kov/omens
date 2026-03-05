---
name: use-omens
description: Run the full omens pipeline for Brazilian FII (Fundo de Investimento Imobiliário) data collection, analysis, and reporting — from explore to report. Use when asked to collect data, run a ticker, analyse signals, or produce a report.
---

# Skill: Using `omens`

`omens` is a Rust CLI that scrapes Brazilian FII data from clubefii.com.br, stores
it in SQLite, scores new/changed items for actionable signals, and produces reports.

**Always invoke via `cargo run --`** (never a compiled binary):

```bash
cargo run -- <command>
```

All commands below assume this prefix.

---

## Quick Start (existing ticker, display already running)

```bash
cargo run -- collect run --tickers BRCR11
cargo run -- report latest          # signals from this run
cargo run -- report since 30d       # or: all signals from the last 30 days
```

If the display isn't running yet:

```bash
cargo run -- display start
cargo run -- collect run --tickers BRCR11
cargo run -- report latest
```

For a brand-new ticker you haven't explored before, see
[Typical Full Run](#typical-full-run-new-ticker) at the bottom.

---

## Prerequisites

### 1. Display (Wayland/Weston)

The browser needs a Wayland compositor. Check and start it once per session:

```bash
cargo run -- display status
cargo run -- display start          # default: listens on 127.0.0.1:3389
cargo run -- display stop
```

`display start` persists state to `~/.omens/display/session.state`. It survives
across `collect run` calls; you only need to stop/restart if the process dies.

### 2. Browser

Check and install the pinned Chromium build once:

```bash
cargo run -- browser status
cargo run -- browser install        # downloads pinned build (~300 MB)
cargo run -- browser upgrade        # upgrade to newer pinned build
cargo run -- browser rollback       # revert to previous build
cargo run -- browser reset-profile  # wipe browser profile (login state etc.)
```

### 3. Authentication

Bootstrap a logged-in session on clubefii.com.br. This opens an interactive
browser window — the user must log in manually:

```bash
cargo run -- auth bootstrap          # persistent login (saved to profile)
cargo run -- auth bootstrap --ephemeral   # temporary profile, discarded after
cargo run -- auth bootstrap --display     # also starts display if not running
```

Auth state is preserved in the browser profile across `collect run` calls.

### 4. Config

Config file (optional): `~/.omens/config/omens.toml`
Root data dir: `~/.omens/`

Key config sections (all have defaults, file may not exist):

```toml
[collector]
tickers = ["BRCR11", "HGLG11"]   # used when --tickers not passed

[analysis.thresholds]
high_impact    = 0.8   # medium signals below this are hidden in reports
low_confidence = 0.3   # signals below this are dropped entirely

[analysis.lmstudio]
enabled = false
base_url = "http://127.0.0.1:1234/v1"
model = ""             # set to enable LM Studio enhancement
max_input_chars = 12000

[browser]
mode = "bundled"       # or "system"
```

---

## Pipeline

### Step 1 — Explore (one-time setup, NOT per-ticker)

**Recipes are per-section, not per-ticker.** All clubefii.com.br FII pages share
the same HTML structure, so a single explore run against any ticker creates recipes
that work for every ticker in your config. **BRCR11 is the recommended reference
ticker** — it has all four main sections well-populated.

You only need to re-run explore if:
- The database was wiped (`omens.db` deleted)
- The site's HTML structure changed and collect starts returning 0 items
- You want to improve recipe quality for a specific section

```bash
cargo run -- explore start BRCR11
# or pass a full URL:
cargo run -- explore start https://www.clubefii.com.br/fiis/BRCR11
```

This opens Chromium, crawls all 25 tabs, discovers tables and repeating-group
fields, and saves one candidate recipe per section to the DB.

Review candidates (one row per section, all confidence=1.00 when auto-detected):

```bash
cargo run -- explore review
# id=1   section=informacoes_basicas  status=pending_review  confidence=1.00  name=...
# id=3   section=cotacoes             status=pending_review  confidence=1.00  name=...
# id=8   section=comunicados          status=pending_review  confidence=1.00  name=...
# id=10  section=proventos            status=pending_review  confidence=1.00  name=...
# ... (25 sections total, only 4 are used for scoring)
```

Promote only the 4 sections used for collection and scoring:

```bash
cargo run -- explore promote <id_for_informacoes_basicas>
cargo run -- explore promote <id_for_comunicados>
cargo run -- explore promote <id_for_proventos>
cargo run -- explore promote <id_for_cotacoes>
```

After promoting these 4, all 51 configured tickers will collect successfully.
The other 21 sections (videos, comparativo, etc.) can be ignored.

Only one recipe per section is `active` at a time. Promoting a new one
automatically demotes the previous.

### Step 2 — Collect

Run data collection for one or more tickers:

```bash
cargo run -- collect run --tickers BRCR11
cargo run -- collect run --tickers BRCR11,HGLG11
cargo run -- collect run                          # uses config tickers
cargo run -- collect run --tickers BRCR11 --sections comunicados,proventos
```

Output summary:

```
collect run
  run_id: 12
  tickers: BRCR11
  status: success
  items_seen: 387
  items_new: 0
  items_changed: 2
  signals: 0 (0 high, 0 medium)
  retention: runs_deleted=0, versions_deleted=0
```

When signals are generated they print inline:

```
signals: 3 (2 high, 1 medium)

Signals:
  [HIGH     0.90] new announcement: ...
  [HIGH     0.85] management report (relatório gerencial): ...
  [MEDIUM   0.80] contains 'assembleia': ...
```

### Step 2b — Fetch Document Text (on demand)

To read the full text of a comunicado or other document stored in the DB:

```bash
cargo run -- fetch-doc 'external_id:TICKER/comunicados/...'   # by stable_key
cargo run -- fetch-doc 'https://bvmf.bmfbovespa.com.br/...'  # by URL
```

- Accepts a **stable_key** (from DB) or a **direct URL**
- Stable-key mode: navigates to the ticker's list page, finds the document link
  by matching the row text (Categoria, Data Referência, etc.), then fetches it
- Handles two document types automatically:
  - **B3/FNET** (`bvmf.bmfbovespa.com.br`, `fnet.bmfbovespa.com.br`): public PDF,
    downloaded with reqwest and converted via `pdftotext -layout`
  - **Clubefii embed** (`fundo_comunicados_embed`): follows the download link to the PDF
  - **HTML pages**: tag-stripped to plain text
- Results are **cached** in `~/.cache/omens/docs/` — subsequent calls are instant
- Output goes to stdout; useful for piping or reading inline

Requires the display to be running (same as `collect run`).

### Step 3 — Report

Two report subcommands, serving different purposes:

```bash
cargo run -- report latest          # signals from the most recent collect run
cargo run -- report since 30d       # cross-run: items with published_at in last 30 days
cargo run -- report since 2025-01-01  # same with an absolute date
```

#### `report latest`

Shows signals produced by the **most recent `collect run`** — i.e. what changed
or appeared since the previous collection. Use this right after collecting.

```
report latest
  run_id: 12
  total_signals: 5
  shown: 3 (critical/high + medium >= 80% confidence)

--- HIGH ---
  [HIGH     0.90] new announcement: external_id:BRCR11/comunicados/...
    section: comunicados | key: ...
    reasons: contains 'fato relevante'
--- MEDIUM ---
  [MEDIUM   0.80] contains 'assembleia': ...

  reports:
    /home/user/.omens/reports/latest.json
    /home/user/.omens/reports/latest.md
```

#### `report since DATE|Nd`

Cross-run query: returns the most recent signal **per item** where the item's
`published_at` falls within the requested window, across **all** collect runs.
Use this to review what happened over a time period regardless of when you ran
collection.

```
report latest
  since: 2026-01-31
  total_signals: 21
  shown: 11 (critical/high + medium >= 80% confidence)

--- HIGH ---
  ...
```

Items without a `published_at` date (e.g. `cotacoes`) are excluded from
`report since` results.

#### Display filter (both subcommands)

- **critical / high**: always shown
- **medium**: shown only if `confidence >= high_impact` (default 0.8)
- **low / ignore**: never shown in terminal output

Writes two files after each report:
- `~/.omens/reports/latest.json` — machine-readable, full signal list
- `~/.omens/reports/latest.md` — markdown summary

---

## Signal Scoring Reference

Signals are produced by `src/analyze.rs` during `collect run`. Only
**new or changed** items are scored (stable items produce no signals).

### Sections and rules

| Section | Trigger | Severity | Confidence |
|---|---|---|---|
| `comunicados` | contains "fato relevante" | High | 0.90 |
| `comunicados` | contains "relatório gerencial" | High | 0.85 |
| `comunicados` | contains "assembleia" or "alteração" | Medium | 0.80 |
| `comunicados` | any other new/changed item | Medium | 0.75 |
| `proventos` | dividend amount changed | High | 0.90 |
| `proventos` | new positive dividend | Medium | 0.85 |
| `proventos` | NÃO DISTRIBUIÇÃO | Low | 0.70 |
| `informacoes_basicas` | changed (not new) | Medium | 0.70 |
| `cotacoes` | (always ignored — historical price data) | — | — |

### Severity ranks

`critical=4 > high=3 > medium=2 > low=1 > ignore=0`

Signals are sorted by rank descending, then confidence descending,
then published_at descending.

---

## Browse — Interactive Web Exploration

For interactive browser automation (navigating sites, extracting content,
clicking elements), use `/browse`. The browse session shares the same display
and browser infrastructure as the collect pipeline.

---

## Investigating the Database

DB path: `~/.omens/db/omens.db`

```bash
# All runs
sqlite3 ~/.omens/db/omens.db \
  "SELECT id, started_at, status FROM runs ORDER BY id;"

# Signals from the latest run
sqlite3 ~/.omens/db/omens.db "
  SELECT s.severity, s.confidence, s.summary
  FROM signals s
  JOIN (SELECT MAX(id) id FROM runs) r ON s.run_id = r.id
  ORDER BY s.confidence DESC LIMIT 20;"

# Items from a section
sqlite3 ~/.omens/db/omens.db "
  SELECT stable_key, (SELECT COUNT(*) FROM item_versions WHERE item_id=i.id) versions
  FROM items i WHERE section='comunicados' AND ticker='BRCR11'
  ORDER BY stable_key;"

# Payload of a specific item
sqlite3 ~/.omens/db/omens.db "
  SELECT iv.payload_json FROM item_versions iv
  JOIN items i ON i.id=iv.item_id
  WHERE i.stable_key LIKE '%Fato Relevante%'
  ORDER BY iv.id DESC LIMIT 1;"
```

---

## Stable Key Format

Items are deduplicated by `ticker/section/stable_key`. Stable keys appear in
signal summaries from `collect run` and in `report latest` output (`key:` field).

They are built from a multi-pass compound key algorithm:

1. Seed: first cell of each row (or row index if empty)
2. Repeatedly extend non-unique keys using preferred headers in order:
   `Assunto` → `Data Referência` → `Data Entrega` → `MÊS REF.` → `DATA COM` → `Status / Modalidade Envio`
3. Placeholder values (`N/D`, `N/A`, `-`, `--`, empty) are skipped
4. Last resort: append row index as tiebreaker

Example stable keys:
```
Fato Relevante|31/08/2023                   ← first_cell|Data Referência
Informe Mensal Estruturado|01/01/2026|26/02/2026 18:17:00   ← 3-part: added Data Entrega
Aviso ao Mercado|2                          ← first_cell|row_index (last resort)
```

---

## Typical Full Run (fresh database or new machine)

```bash
# 1. Start display (if not running)
cargo run -- display start

# 2. Check browser
cargo run -- browser status
# If not installed:
cargo run -- browser install

# 3. Auth (user logs in interactively)
cargo run -- auth bootstrap

# 4. Explore BRCR11 (creates recipes for ALL tickers — only needed once)
cargo run -- explore start BRCR11

# 5. Review candidates and promote the 4 scoring sections
cargo run -- explore review
# Note the IDs for: informacoes_basicas, comunicados, proventos, cotacoes
cargo run -- explore promote <id_for_informacoes_basicas>
cargo run -- explore promote <id_for_comunicados>
cargo run -- explore promote <id_for_proventos>
cargo run -- explore promote <id_for_cotacoes>

# 6. Full pipeline run across all configured tickers
cargo run -- run

# 7. Review signals from the last 30 days (cross-run)
cargo run -- report since 30d
```

## Adding a new ticker

Just add the ticker to `collector.tickers` in `~/.omens/config/omens.toml`.
No explore needed — existing recipes cover all clubefii.com.br tickers.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Missing X server or $DISPLAY` | Display not running | `cargo run -- display start` |
| `display session already running` | Stale state | `cargo run -- display stop && cargo run -- display start` |
| `no tickers specified` | No `--tickers` and no config | Add `--tickers TICKER` |
| `no recipes found` | Explore not run yet | Run `explore start` then `explore promote` |
| `lock contended` | Another collect running | Wait for it to finish |
| Browser login fails / session lost | Stale or corrupt profile | `cargo run -- browser reset-profile` then `auth bootstrap` again |
| Many `items_changed` on re-run | Key collision from past run | Check `stable_key` uniqueness in DB |
| Signals not showing in report | Low confidence / below threshold | Check `high_impact` in config; or signal is `medium < 0.8` |
| `fetch-doc`: no document link found | Assunto is N/D and row text doesn't match | Try passing the B3/FNET URL directly instead of the stable_key |
| `fetch-doc`: empty output | B3 URL served HTML (not PDF) or page timed out | Delete `~/.cache/omens/docs/<hash>.txt` and retry |
