#!/usr/bin/env bash
# daily-report.sh — collect FII data, generate signal report, run Claude analysis

set -euo pipefail

OMENS="$HOME/.local/bin/omens"
CLAUDE="/home/kov/.local/bin/claude"
DATE=$(date +%Y-%m-%d)
OUTPUT_DIR="$HOME/Documents/omens"
OUTPUT_FILE="$OUTPUT_DIR/$DATE.md"
PROMPT_FILE="$HOME/.cache/omens/prompt.txt"

mkdir -p "$OUTPUT_DIR" "$HOME/.cache/omens/docs"

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

Investigate and conclude — never escalate to the investor.

The investor reading this report wants **conclusions**, not a list of things to
check. For every HIGH or CRITICAL signal, fully resolve it here using the tools
available. Do not write "verificar X", "checar Y", or "consultar o comunicado"
unless you have already done so and the answer is not there.

**Investigation workflow (follow in order for every HIGH/CRITICAL signal):**

1. **Payload history** — look up all scraped versions of the item to understand
   what exactly changed and when:
     sqlite3 ~/.omens/db/omens.db "
       SELECT r.id AS run, iv.payload_json
       FROM item_versions iv
       JOIN items i ON iv.item_id = i.id
       JOIN runs r ON iv.run_id = r.id
       WHERE i.stable_key = 'external_id:TICKER/section/key'
       ORDER BY r.id"

2. **Historical context** — compare with prior values for the same ticker/section:
     sqlite3 ~/.omens/db/omens.db "
       SELECT r.id AS run, s.severity, s.confidence, s.summary
       FROM signals s
       JOIN items i ON s.item_id = i.id
       JOIN runs r ON s.run_id = r.id
       WHERE i.external_id LIKE '%TICKER%'
       ORDER BY r.id"

3. **Fetch the document** — for every HIGH/CRITICAL comunicado signal, and for
   every proventos signal where the reason isn't obvious, fetch the full text of
   the related comunicado. This is mandatory, not optional.

   Step 3a — find the right stable_key:
     sqlite3 ~/.omens/db/omens.db "
       SELECT i.stable_key, i.published_at, iv.payload_json
       FROM items i JOIN item_versions iv ON iv.item_id = i.id
       WHERE i.external_id LIKE '%TICKER/comunicados%'
       ORDER BY i.published_at DESC LIMIT 10"

   Step 3b — fetch the document (takes the stable_key exactly as it appears):
     ~/.local/bin/omens fetch-doc 'external_id:TICKER/comunicados/...'

   fetch-doc outputs the full document text (PDF converted to plain text, or HTML
   stripped). Results are cached — subsequent calls for the same document are
   instant. The display session is already running.

4. **Write your finding** — only after steps 1–3. If after fetching the document
   something is still unclear, say exactly what is missing and why it could not
   be resolved.

**Additional queries:**
  ~/.local/bin/omens report since 7d    # compact cross-run signal view
  ~/.local/bin/omens report since 30d   # broader historical context

**Output:** Escreva em português (pt-BR). Relatório conciso em Markdown cobrindo:
- O que aconteceu de fato (com base nos dados e no texto do documento, não no rótulo do sinal)
- Para cada ticker com sinal HIGH/CRITICAL: sua conclusão objetiva sobre o que fazer ou monitorar
- Se algo genuinamente não pôde ser resolvido: o que falta e por quê

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
    --bind "$HOME/.omens" "$HOME/.omens" \
    --bind "$HOME/.cache/omens" "$HOME/.cache/omens" \
    -- \
    env -u CLAUDECODE \
    "$CLAUDE" \
        --print \
        --dangerously-skip-permissions \
        --allowedTools Bash \
    < "$PROMPT_FILE" \
    > "$OUTPUT_FILE"

echo "[$(date -Iseconds)] Report saved: $OUTPUT_FILE"
