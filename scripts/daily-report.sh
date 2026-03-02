#!/usr/bin/env bash
# daily-report.sh — collect FII data, generate signal report, run Claude analysis

set -euo pipefail

OMENS="$HOME/.local/bin/omens"
CLAUDE="/home/kov/.local/bin/claude"
DATE=$(date +%Y-%m-%d)
OUTPUT_DIR="$HOME/Documents/omens"
OUTPUT_FILE="$OUTPUT_DIR/$DATE.md"
PROMPT_FILE="$HOME/.cache/omens/prompt.txt"

mkdir -p "$OUTPUT_DIR" "$HOME/.cache/omens"

# ---------------------------------------------------------------------------
# Phase 1 — Collect (runs outside bwrap, needs full display/browser access)
# ---------------------------------------------------------------------------

DISPLAY_STARTED=false
if "$OMENS" display start 2>/dev/null; then
    DISPLAY_STARTED=true
    echo "[$(date -Iseconds)] Display started."
else
    echo "[$(date -Iseconds)] Display already running, skipping start."
fi
trap 'if $DISPLAY_STARTED; then "$OMENS" display stop && echo "[$(date -Iseconds)] Display stopped."; fi' EXIT

echo "[$(date -Iseconds)] Running full pipeline (collect + report)..."
"$OMENS" run

echo "[$(date -Iseconds)] Collect complete."

# ---------------------------------------------------------------------------
# Phase 2 — Build prompt file (written directly to avoid variable size limits)
# ---------------------------------------------------------------------------

REPORT_FILE="$HOME/.omens/reports/latest.md"
if [[ ! -f "$REPORT_FILE" ]]; then
    echo "[$(date -Iseconds)] ERROR: $REPORT_FILE not found after run. Aborting." >&2
    exit 1
fi

{
    cat <<EOF
You are analyzing FII (Fundo de Investimento Imobiliário — Brazilian Real
Estate Investment Trust) signals collected by the omens monitoring system.

Date: $DATE

## Today's signal report

EOF
    head -c 50000 "$REPORT_FILE"
    if [[ $(wc -c < "$REPORT_FILE") -gt 50000 ]]; then
        echo ""
        echo "(Report truncated at 50 KB — use the available tools to query additional signals.)"
    fi
    cat <<'EOF'

## Your task

Investigate and report — do not escalate.

For every HIGH or CRITICAL signal, exhaust the available data before writing a
finding. If something looks anomalous, query the database to answer the question
yourself. The investor reading this report wants conclusions, not a list of things
to check manually. Do not write "verificar X" or "checar Y" unless you have
already queried for X and Y and the data is genuinely absent from the DB.

**Investigation workflow for each notable signal:**
1. Look up the raw payload: all scraped fields for that item across every run
   (item_versions.payload_json). This is the ground truth.
2. Check the full history for that ticker/section: how has the value changed
   across runs? Is the current version consistent with prior ones?
3. If a comunicado or relatório gerencial exists for that ticker in the DB,
   read its payload — it often directly answers "why did this change?"
4. Only after exhausting the DB should you write your finding.

**Key queries:**
  # All versions of an item (see how data changed across runs)
  sqlite3 ~/.omens/db/omens.db "
    SELECT r.id AS run, iv.payload_json
    FROM item_versions iv
    JOIN items i ON iv.item_id = i.id
    JOIN runs r ON iv.run_id = r.id
    WHERE i.stable_key = 'external_id:TICKER/section/key'
    ORDER BY r.id"

  # All items for a ticker in a section (find related comunicados, etc.)
  sqlite3 ~/.omens/db/omens.db "
    SELECT i.stable_key, iv.payload_json
    FROM items i JOIN item_versions iv ON iv.item_id = i.id
    WHERE i.external_id LIKE '%TICKER/comunicados%'
    ORDER BY i.published_at DESC LIMIT 10"

  # Cross-run signals for a ticker (see what the system flagged and when)
  sqlite3 ~/.omens/db/omens.db "
    SELECT r.id AS run, s.severity, s.summary
    FROM signals s JOIN items i ON s.item_id = i.id JOIN runs r ON s.run_id = r.id
    WHERE i.external_id LIKE '%TICKER%'
    ORDER BY r.id"

  ~/.local/bin/omens report since 7d   # compact view of recent signals
  ~/.local/bin/omens report since 30d  # broader context

**Output:** Escreva em português (pt-BR). Relatório conciso em Markdown cobrindo:
- O que aconteceu de fato (não apenas o rótulo do sinal)
- Quais tickers merecem atenção e por quê — com sua conclusão, não dúvidas abertas
- Se após esgotar os dados algo ainda é inconclusivo, diga explicitamente o que
  está faltando no banco e por que não foi possível resolver

## Database schema

  items(id, section, external_id, stable_key, published_at, normalized_json)
  item_versions(id, item_id, run_id, payload_json)   <- full scraped data per run
  signals(id, item_id, run_id, severity, confidence, summary, reasons_json)
  runs(id, started_at, ended_at, status)

payload_json format: [["key","value"], ...]  (sorted key-value pairs)

Do not write files or modify anything. Output your analysis to stdout.
EOF
} > "$PROMPT_FILE"

# ---------------------------------------------------------------------------
# Phase 3 — Run Claude inside bwrap (read-only FS except /tmp and ~/.claude)
# ---------------------------------------------------------------------------

echo "[$(date -Iseconds)] Running Claude analysis..."

bwrap \
    --ro-bind / / \
    --proc /proc \
    --dev /dev \
    --tmpfs /tmp \
    --bind "$HOME/.claude" "$HOME/.claude" \
    --bind "$HOME/.omens/db" "$HOME/.omens/db" \
    -- \
    env -u CLAUDECODE \
    "$CLAUDE" \
        --print \
        --dangerously-skip-permissions \
        --allowedTools Bash \
    < "$PROMPT_FILE" \
    > "$OUTPUT_FILE"

echo "[$(date -Iseconds)] Report saved: $OUTPUT_FILE"
