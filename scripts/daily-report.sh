#!/usr/bin/env bash
# daily-report.sh — collect FII data, generate signal report, run Claude analysis

set -euo pipefail

OMENS="$HOME/.local/bin/omens"
CLAUDE="/home/kov/.local/bin/claude"
DATE=$(date +%Y-%m-%d)
OUTPUT_DIR="$HOME/Documents/omens"
OUTPUT_FILE="$OUTPUT_DIR/$DATE.md"
PROMPT_FILE="$HOME/.cache/omens/prompt.txt"
export RUST_BACKTRACE=1

EX_AUTH_REQUIRED=20

# Refresh token outside sandbox (quick throwaway call)
"$CLAUDE" --print "Good morning, Claude!"

mkdir -p "$OUTPUT_DIR" "$HOME/.cache/omens/docs"

# ---------------------------------------------------------------------------
# Phase 1 — Collect (display auto-starts as needed)
# ---------------------------------------------------------------------------

RUN_TODAY=$(sqlite3 ~/.omens/db/omens.db \
    "SELECT COUNT(*) FROM runs WHERE date(started_at, 'unixepoch', 'localtime') = '$DATE' AND status = 'success'" 2>/dev/null || echo 0)

if [[ "$RUN_TODAY" -gt 0 ]]; then
    echo "[$(date -Iseconds)] Collect already succeeded today, skipping."
else
    echo "[$(date -Iseconds)] Running full pipeline (collect + report)..."
    rc=0
    "$OMENS" run || rc=$?

    if [[ $rc -eq $EX_AUTH_REQUIRED ]]; then
        echo "[$(date -Iseconds)] Auth expired — sending alert email."
        printf '# omens — sessão expirada (%s)\n\nA sessão do clubefii.com.br expirou.\nExecute `omens auth bootstrap` para re-autenticar.\n' "$DATE" \
            > "$OUTPUT_FILE"
        "$OMENS" send-email "$OUTPUT_FILE"
        exit $EX_AUTH_REQUIRED
    elif [[ $rc -ne 0 ]]; then
        echo "[$(date -Iseconds)] Pipeline failed (exit $rc)." >&2
        exit $rc
    fi

    echo "[$(date -Iseconds)] Collect complete."
fi

# ---------------------------------------------------------------------------
# Phase 2+3 — Build prompt and run Claude analysis (skip if report exists)
# ---------------------------------------------------------------------------

is_valid_report() { [[ -f "$1" ]] && grep -q '^#' "$1" && ! grep -q '^# omens — análise falhou' "$1"; }

if is_valid_report "$OUTPUT_FILE"; then
    echo "[$(date -Iseconds)] Claude analysis already exists, skipping."
else

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

**Critical output rule:** Your analysis will be captured via --print, which only
records your final text response. Therefore you MUST accumulate all findings
internally and produce the COMPLETE analysis as a SINGLE final markdown message.
Do not output partial findings between tool calls — gather everything first,
then write the full report at the end. The report MUST start with a markdown
heading: # Análise omens — YYYY-MM-DD
EOF
} > "$PROMPT_FILE"

    rm -f "$OUTPUT_FILE"
    MAX_RETRIES=3
    for attempt in $(seq 1 "$MAX_RETRIES"); do
        echo "[$(date -Iseconds)] Running Claude analysis (attempt $attempt/$MAX_RETRIES)..."
        rc=0
        bwrap \
            --ro-bind / / \
            --proc /proc \
            --dev /dev \
            --tmpfs /tmp \
            --bind "$HOME/.claude" "$HOME/.claude" \
            --bind "$HOME/.claude.json" "$HOME/.claude.json" \
            --bind "$HOME/.omens" "$HOME/.omens" \
            --bind "$HOME/.cache/omens" "$HOME/.cache/omens" \
            -- \
            env -u CLAUDECODE \
            "$CLAUDE" \
                --print \
                --dangerously-skip-permissions \
                --allowedTools Bash \
            < "$PROMPT_FILE" \
            > "$OUTPUT_FILE.tmp" 2>"$OUTPUT_FILE.stderr" || rc=$?

        if [[ $rc -eq 0 ]] && is_valid_report "$OUTPUT_FILE.tmp"; then
            mv "$OUTPUT_FILE.tmp" "$OUTPUT_FILE"
            rm -f "$OUTPUT_FILE.stderr"
            break
        fi

        echo "[$(date -Iseconds)] Claude analysis failed (exit $rc), attempt $attempt/$MAX_RETRIES." >&2
        if [[ -s "$OUTPUT_FILE.stderr" ]]; then
            echo "--- stderr (first 2000 bytes) ---" >&2
            head -c 2000 "$OUTPUT_FILE.stderr" >&2
            echo "" >&2
            echo "--- end stderr ---" >&2
        fi
        if [[ -s "$OUTPUT_FILE.tmp" ]]; then
            echo "--- stdout (first 500 bytes) ---" >&2
            head -c 500 "$OUTPUT_FILE.tmp" >&2
            echo "" >&2
            echo "--- end stdout ---" >&2
        fi
        if [[ ! -s "$OUTPUT_FILE.tmp" ]] && [[ ! -s "$OUTPUT_FILE.stderr" ]]; then
            echo "(no output on stdout or stderr)" >&2
        fi
        # Keep last failure for the error email
        cat "$OUTPUT_FILE.stderr" "$OUTPUT_FILE.tmp" > "$OUTPUT_FILE.lastfail" 2>/dev/null
        rm -f "$OUTPUT_FILE.tmp" "$OUTPUT_FILE.stderr"
        if [[ $attempt -lt $MAX_RETRIES ]]; then
            sleep 30
        fi
    done

    if ! is_valid_report "$OUTPUT_FILE"; then
        echo "[$(date -Iseconds)] Claude analysis failed after $MAX_RETRIES attempts." >&2
        # Send failure notification with last error output
        {
            printf '# omens — análise falhou (%s)\n\n' "$DATE"
            printf 'Claude falhou em todas as %d tentativas.\n\n' "$MAX_RETRIES"
            if [[ -s "$OUTPUT_FILE.lastfail" ]]; then
                printf '## Última saída (truncada)\n\n```\n'
                head -c 2000 "$OUTPUT_FILE.lastfail"
                printf '\n```\n'
            else
                printf 'Nenhuma saída produzida.\n'
            fi
        } > "$OUTPUT_FILE"
        "$OMENS" send-email "$OUTPUT_FILE"
        exit 1
    fi

    echo "[$(date -Iseconds)] Report saved: $OUTPUT_FILE"
fi

# ---------------------------------------------------------------------------
# Phase 4 — Email report
# ---------------------------------------------------------------------------

SENT_MARKER="$OUTPUT_DIR/$DATE.sent"
if [[ -f "$SENT_MARKER" ]]; then
    echo "[$(date -Iseconds)] Email already sent today, skipping."
else
    echo "[$(date -Iseconds)] Emailing report..."
    "$OMENS" send-email "$OUTPUT_FILE"
    touch "$SENT_MARKER"
fi
