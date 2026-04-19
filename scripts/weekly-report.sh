#!/usr/bin/env bash
# weekly-report.sh — Saturday weekly FII analysis
#   - Runs collect + report over the last 7 days
#   - Injects 90d per-ticker history
#   - Consolidates pendências ("fetch falhou") from the last 7 daily reports
#   - Asks Claude for cross-ticker trends, not just per-signal resolution

set -euo pipefail

OMENS="$HOME/.local/bin/omens"
CLAUDE="/home/kov/.local/bin/claude"
DATE=$(date +%Y-%m-%d)
OUTPUT_DIR="$HOME/Documents/omens/weekly"
OUTPUT_FILE="$OUTPUT_DIR/$DATE-semanal.md"
PROMPT_FILE="$HOME/.cache/omens/prompt-weekly.txt"
DAILY_DIR="$HOME/Documents/omens"
export RUST_BACKTRACE=1

EX_AUTH_REQUIRED=20

# Refresh token outside sandbox (quick throwaway call)
"$CLAUDE" --print "Good morning, Claude!"

mkdir -p "$OUTPUT_DIR" "$HOME/.cache/omens/docs"

# ---------------------------------------------------------------------------
# Phase 1 — Collect (same-day skip) and build a 7-day signal report
# ---------------------------------------------------------------------------

RUN_TODAY=$(sqlite3 ~/.omens/db/omens.db \
    "SELECT COUNT(*) FROM runs WHERE date(started_at, 'unixepoch', 'localtime') = '$DATE' AND status = 'success'" 2>/dev/null || echo 0)

if [[ "$RUN_TODAY" -gt 0 ]]; then
    echo "[$(date -Iseconds)] Collect already succeeded today, skipping."
else
    echo "[$(date -Iseconds)] Running collect..."
    rc=0
    "$OMENS" collect run || rc=$?

    if [[ $rc -eq $EX_AUTH_REQUIRED ]]; then
        echo "[$(date -Iseconds)] Auth expired — sending alert email."
        printf '# omens — sessão expirada (%s)\n\nA sessão do clubefii.com.br expirou.\nExecute `omens auth bootstrap` para re-autenticar.\n' "$DATE" \
            > "$OUTPUT_FILE"
        "$OMENS" send-email "$OUTPUT_FILE"
        exit $EX_AUTH_REQUIRED
    elif [[ $rc -ne 0 ]]; then
        echo "[$(date -Iseconds)] Collect failed (exit $rc)." >&2
        exit $rc
    fi
fi

echo "[$(date -Iseconds)] Rebuilding 7-day signal report..."
"$OMENS" report since 7d > /dev/null

# ---------------------------------------------------------------------------
# Phase 2+3 — Build weekly prompt and run Claude analysis
# ---------------------------------------------------------------------------

is_valid_report() { [[ -f "$1" ]] && grep -q '^#' "$1" && ! grep -q '^# omens — análise falhou' "$1"; }

if is_valid_report "$OUTPUT_FILE"; then
    echo "[$(date -Iseconds)] Weekly Claude analysis already exists, skipping."
else

REPORT_FILE="$HOME/.omens/reports/latest.md"
if [[ ! -f "$REPORT_FILE" ]]; then
    echo "[$(date -Iseconds)] ERROR: $REPORT_FILE not found after report since 7d. Aborting." >&2
    exit 1
fi

{
    cat <<EOF
You are analyzing FII (Fundo de Investimento Imobiliário — Brazilian Real
Estate Investment Trust) signals collected by the omens monitoring system,
producing a **weekly** synthesis over the last 7 days.

Date (semana encerra em): $DATE

## Signal report — últimos 7 dias

EOF
    head -c 80000 "$REPORT_FILE"
    if [[ $(wc -c < "$REPORT_FILE") -gt 80000 ]]; then
        echo ""
        echo "(Report truncated at 80 KB — use the available tools to query additional signals.)"
    fi

    # -------------------------------------------------------------------------
    # Inject per-ticker 90-day history for every ticker with any MEDIUM+ signal
    # in the last 7 days. Wider window than daily to expose multi-month trends.
    # -------------------------------------------------------------------------
    # Only fan out to tickers that had HIGH/CRITICAL this week; for each, the
    # 20 most recent signals in the 90d window — enough to see multi-month
    # trends without blowing up the prompt.
    HISTORY_SQL="
      WITH active_tickers AS (
        SELECT DISTINCT substr(i.external_id, 1, instr(i.external_id, '/') - 1) AS ticker
        FROM signals s
        JOIN items i ON s.item_id = i.id
        WHERE s.created_at >= strftime('%s', 'now', '-7 days')
          AND s.severity IN ('critical', 'high')
      ),
      ranked AS (
        SELECT
          substr(i.external_id, 1, instr(i.external_id, '/') - 1) AS ticker,
          COALESCE(date(i.published_at, 'unixepoch', 'localtime'), '?') AS dt,
          upper(substr(s.severity, 1, 4)) AS sev,
          printf('%.2f', s.confidence) AS conf,
          i.section,
          replace(replace(s.summary, char(10), ' '), char(13), ' ') AS summary,
          i.published_at AS pub_at,
          row_number() OVER (
            PARTITION BY substr(i.external_id, 1, instr(i.external_id, '/') - 1)
            ORDER BY i.published_at DESC
          ) AS rn
        FROM signals s
        JOIN items i ON s.item_id = i.id
        WHERE substr(i.external_id, 1, instr(i.external_id, '/') - 1) IN (SELECT ticker FROM active_tickers)
          AND i.published_at IS NOT NULL
          AND i.published_at >= strftime('%s', 'now', '-90 days')
      )
      SELECT ticker, dt, sev, conf, section, summary
      FROM ranked
      WHERE rn <= 20
      ORDER BY ticker, pub_at DESC;
    "
    HISTORY=$(sqlite3 -separator $'\t' ~/.omens/db/omens.db "$HISTORY_SQL" 2>/dev/null || true)
    if [[ -n "$HISTORY" ]]; then
        echo ""
        echo "## Contexto histórico (90d por ticker ativo na semana)"
        echo ""
        echo "$HISTORY" | awk -F'\t' '
            BEGIN { prev = "" }
            {
                if ($1 != prev) {
                    if (prev != "") print ""
                    print "### " $1
                    prev = $1
                }
                printf "- %s %-4s %s %-20s %s\n", $2, $3, $4, $5, $6
            }
        '
    fi

    # -------------------------------------------------------------------------
    # Consolidate fetch-doc pendências from the last 7 daily reports.
    # The daily prompt forbids "verificar" unless the fetch actually failed, in
    # which case it writes "(fetch falhou: <razão>)". We scan for those.
    # -------------------------------------------------------------------------
    PEND=""
    for d in $(seq 1 7); do
        DATE_PAST=$(date -d "-$d days" +%Y-%m-%d)
        F="$DAILY_DIR/$DATE_PAST.md"
        if [[ -f "$F" ]]; then
            while IFS= read -r line; do
                trimmed="${line#"${line%%[![:space:]]*}"}"
                PEND+="- **$DATE_PAST** — ${trimmed}"$'\n'
            done < <(grep -F 'fetch falhou' "$F" 2>/dev/null || true)
        fi
    done

    if [[ -n "$PEND" ]]; then
        echo ""
        echo "## Pendências dos últimos 7 dias"
        echo ""
        echo "Linhas dos relatórios diários em que o fetch-doc falhou. Tente resolver de novo agora (o cache é por URL — falhas passadas podem voltar a funcionar):"
        echo ""
        printf '%s' "$PEND"
    fi

    cat <<'EOF'

## Your task

Este é o relatório **semanal**. O foco é consolidar, não repetir o diário:
encontrar tendências que só aparecem com mais dias, resolver pendências que
ficaram de pé, e dar ao investidor uma visão do estado atual da carteira.

**Workflow:**

1. **Pendências primeiro.** Para cada item em "Pendências dos últimos 7 dias",
   tente `omens fetch-doc` de novo. Se resolver, trate-o como um sinal normal.
   Se continuar falhando, mantenha na seção final com o motivo atualizado.

2. **Tendências cross-ticker.** Olhando o histórico 90d, identifique padrões
   que abrangem múltiplos tickers — p.ex.:
   - 2+ FIIs de shopping com vacância subindo
   - Vários FIIs de papel cortando rendimento no mesmo mês
   - Cluster de emissões / alterações regulamentares
   - Setores inteiros com NÃO DISTRIBUIÇÃO recorrente
   Não force: se a semana não tem tendência cross-ticker nítida, diga isso.

3. **Eventos únicos HIGH/CRITICAL.** Use a mesma disciplina do relatório
   diário: fetch-doc obrigatório para assembleia/alteração/fato relevante/
   NÃO DISTRIBUIÇÃO, nunca "verificar".

**Always-fetch triggers.** Mesmos gatilhos do relatório diário: assembleia,
alteração, fusão, incorporação, liquidação, destituição, fato relevante,
NÃO DISTRIBUIÇÃO. O histórico 90d ajuda a contextualizar, mas não substitui
o texto do documento.

**Forbidden phrasings.** Nunca escreva "verificar X", "checar Y",
"consultar o comunicado", "acompanhar a publicação do documento" — exceto
no formato exato `(fetch falhou: <razão curta>)` após tentativa real.

**Queries úteis:**
  ~/.local/bin/omens report since 30d    # visão de um mês
  sqlite3 ~/.omens/db/omens.db "SELECT ..."

**Output format (obrigatório).** Escreva em português (pt-BR) e siga
**exatamente** esta estrutura:

```
# Análise omens semanal — YYYY-MM-DD

## Resumo executivo

- 3 a 8 bullets com o que importa da semana, priorizado. Cada bullet com
  conclusão objetiva; nunca "verificar"/"checar".
- Se a semana foi genuinamente tranquila, diga "Semana sem eventos materiais."
  e liste em uma linha os tickers mais ativos que foram revisados.

## Tendências cross-ticker

Bullets descrevendo padrões que envolvem ≥ 2 tickers ou um setor. Se nenhum
padrão for nítido, escreva "Nenhuma tendência cross-ticker nítida esta semana."
— não invente correlações.

## Eventos da semana

Uma subseção `### TICKER — título do evento` por ticker com sinal HIGH/CRITICAL
genuíno na semana. Cubra o que aconteceu (com base no documento), sua
conclusão objetiva, e referência à data do evento.

## Sinais rotineiros

Uma linha por sinal MEDIUM ou HIGH da semana que é rotineiro (relatório
gerencial sem surpresas, provento estável, assembleia de rotina). Formato:

- **TICKER** — descrição curta em ≤ 1 linha, com data do evento

Se nenhum, escreva "Nenhum.".

## Pendências persistentes

Itens onde `fetch-doc` continuou falhando mesmo com nova tentativa, ou que
dependem de informação externa ao sistema. Formato:

- **TICKER** — o que falta — `(fetch falhou: <razão>)` ou justificativa

Se nenhuma, escreva "Nenhuma." e confirme que todas as pendências da semana
foram resolvidas nesta análise.
```

## Database schema

  items(id, section, external_id, stable_key, published_at, normalized_json)
  item_versions(id, item_id, run_id, payload_json)   <- full scraped data per run
  signals(id, item_id, run_id, severity, confidence, summary, reasons_json, created_at)
  runs(id, started_at, ended_at, status)

payload_json format: [["key","value"], ...]  (sorted key-value pairs)

Do not write files or modify anything. Output your analysis to stdout.

**Critical output rule:** Your analysis will be captured via --print, which only
records your LAST text response. Therefore:
1. Do NOT run any background tasks and do NOT let commands auto-background on
   timeout — set timeout: 600000 (10 minutes) on EVERY Bash tool call.
2. Accumulate all findings internally and produce the COMPLETE analysis as a
   SINGLE final markdown message.
3. The report MUST start with a markdown heading: `# Análise omens semanal — YYYY-MM-DD`
   (exact format, inclui a palavra "semanal").
4. The report MUST follow the Output format above: "Resumo executivo",
   "Tendências cross-ticker", "Eventos da semana", "Sinais rotineiros",
   "Pendências persistentes" — nessa ordem, sem renomear nem mesclar.
5. Your VERY LAST message must be the report itself — do not add any follow-up
   commentary, summary, or acknowledgment after it.
EOF
} > "$PROMPT_FILE"

    rm -f "$OUTPUT_FILE"
    MAX_RETRIES=3
    for attempt in $(seq 1 "$MAX_RETRIES"); do
        echo "[$(date -Iseconds)] Running weekly Claude analysis (attempt $attempt/$MAX_RETRIES)..."
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
                --append-system-prompt "CRITICAL: Set timeout: 600000 on EVERY Bash tool call. Never use run_in_background. Commands that exceed the default 2-minute timeout get auto-backgrounded, and the completion notification will replace your report in --print output." \
            < "$PROMPT_FILE" \
            > "$OUTPUT_FILE.tmp" 2>"$OUTPUT_FILE.stderr" || rc=$?

        if [[ $rc -eq 0 ]] && is_valid_report "$OUTPUT_FILE.tmp"; then
            mv "$OUTPUT_FILE.tmp" "$OUTPUT_FILE"
            rm -f "$OUTPUT_FILE.stderr"
            break
        fi

        echo "[$(date -Iseconds)] Weekly Claude analysis failed (exit $rc), attempt $attempt/$MAX_RETRIES." >&2
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
        cat "$OUTPUT_FILE.stderr" "$OUTPUT_FILE.tmp" > "$OUTPUT_FILE.lastfail" 2>/dev/null
        rm -f "$OUTPUT_FILE.tmp" "$OUTPUT_FILE.stderr"
        if [[ $attempt -lt $MAX_RETRIES ]]; then
            sleep 30
        fi
    done

    if ! is_valid_report "$OUTPUT_FILE"; then
        echo "[$(date -Iseconds)] Weekly Claude analysis failed after $MAX_RETRIES attempts." >&2
        {
            printf '# omens — análise semanal falhou (%s)\n\n' "$DATE"
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

    echo "[$(date -Iseconds)] Weekly report saved: $OUTPUT_FILE"
fi

# ---------------------------------------------------------------------------
# Phase 4 — Email weekly report
# ---------------------------------------------------------------------------

SENT_MARKER="$OUTPUT_DIR/$DATE-semanal.sent"
if [[ -f "$SENT_MARKER" ]]; then
    echo "[$(date -Iseconds)] Weekly email already sent today, skipping."
else
    echo "[$(date -Iseconds)] Emailing weekly report..."
    "$OMENS" send-email "$OUTPUT_FILE"
    touch "$SENT_MARKER"
fi
