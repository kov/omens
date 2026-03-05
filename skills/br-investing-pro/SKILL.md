---
name: br-investing-pro
description: Look up financial metrics for Brazilian equities on br.investing.com using the Explorador de Dados (Data Explorer) and ratios pages. Use when asked to find fundamentals, FCF yield, leverage, valuation, or other stock metrics for B3-listed companies.
---

# Skill: br.investing.com Financial Data Lookup

Uses `omens browse` commands (see `/browse` skill for general reference).
Requires a browse session (`cargo run -- browse start --display`).

Two main data sources on the site:

1. **Explorador de Dados** (Data Explorer) — rich metric pages with analysis
   text, historical trends, peer comparisons, and definitions
2. **Ratios pages** — classic fundamentals table (valuation, profitability,
   leverage, growth, efficiency, dividends)

---

## Source 1: Explorador de Dados (preferred)

### URL scheme

```
https://br.investing.com/pro/BVMF:{TICKER}/explorer              # landing
https://br.investing.com/pro/BVMF:{TICKER}/explorer/{metric_id}  # specific metric
```

Exchange prefix for B3 stocks: `BVMF:`. Examples:
- `BVMF:SIMH3`, `BVMF:ANIM3`, `BVMF:TTEN3`, `BVMF:PETR4`

### Finding a stock's slug

If you don't know the exchange prefix, use the site search:

```bash
cargo run -- browse navigate "https://br.investing.com/search/?q=SIMH3&tab=quotes"
cargo run -- browse links --contains "SIMH3"
```

The `links` output shows the equity page path (e.g. `/equities/jsl-on-nm`).
For the data explorer, use `BVMF:{TICKER}` in the URL directly.

### Metric discovery — dynamic search

The explorer page has two search inputs:
- Input 0: company search (prefilled from URL)
- Input 1: metric search ("Pesquisar uma métrica")

To search metrics, use the native value setter (React input):

```bash
cargo run -- browse eval "(function() {
  var input = document.querySelectorAll('input[type=search]')[1];
  input.focus();
  var setter = Object.getOwnPropertyDescriptor(
    window.HTMLInputElement.prototype, 'value').set;
  setter.call(input, 'FCF');
  input.dispatchEvent(new Event('input', {bubbles: true}));
  return 'ok';
})()"
```

Then read the filtered results:
```bash
cargo run -- browse content --max-chars 2000
```

To click a metric from the dropdown, match by exact text:
```bash
cargo run -- browse eval "(function() {
  var items = document.querySelectorAll('li, button, a, div');
  for (var i = 0; i < items.length; i++) {
    var t = (items[i].textContent || '').trim();
    if (t === 'Rend. do Fluxo de Caixa Livre') {
      items[i].click();
      return 'clicked';
    }
  }
  return 'not found';
})()"
```

Or navigate directly if you know the metric ID:
```bash
cargo run -- browse navigate "https://br.investing.com/pro/BVMF:SIMH3/explorer/fcf_yield_ltm"
```

### Known metric IDs (free tier)

These metrics show the "Resumo de performance" analysis text for free:

| Metric | ID | Notes |
|---|---|---|
| FCF Yield (LTM) | `fcf_yield_ltm` | Historical trend, peer comparison |
| Debt / Equity | `debt_to_equity` | Historical trend, peer comparison, formula |

### Paywalled metrics

Many metrics in the "risco" (risk) category show the metric description and
similar metrics for free, but lock the actual "Análises" section behind Pro+:

- `net_debt_to_ebitda` — Net Debt / EBITDA
- `total_debt_to_ebitda` — Total Debt / EBITDA
- `net_debt_to_equity` — Net Debt / Equity
- `financial_leverage` — Financial Leverage

When paywalled, the page shows: "Para liberar este recurso, atualize para o Pro+"

### Extracting data efficiently

For quick data extraction, use eval to grab just the analysis section:

```bash
cargo run -- browse eval "(function() {
  var text = document.body.innerText;
  var idx = text.indexOf('Resumo de performance');
  if (idx >= 0) return text.substring(idx, idx + 800);
  var idx2 = text.indexOf('Para liberar');
  if (idx2 >= 0) return 'PAYWALLED';
  return 'no data found';
})()"
```

### Peer comparisons

Free metrics include a "Comparativos" table at the bottom with ~10 peer
companies and the sector median. Read with:

```bash
cargo run -- browse content --max-chars 3000
```

The comparisons table appears after the analysis section and metric definition.

---

## Source 2: Ratios Pages (fallback)

### URL scheme

First find the equity slug via search:
```bash
cargo run -- browse navigate "https://br.investing.com/search/?q=SIMH3&tab=quotes"
cargo run -- browse links --contains "SIMH3"
# Output: [page] SIMH3 ... equities → /equities/jsl-on-nm
```

Then append `-ratios`:
```
https://br.investing.com/equities/{slug}-ratios
```

Examples:
- `https://br.investing.com/equities/jsl-on-nm-ratios` (SIMH3)
- `https://br.investing.com/equities/anima-on-ratios` (ANIM3)
- `https://br.investing.com/equities/tres-tentos-agroindustrial-ratios` (TTEN3)

### What's available

The ratios page shows all data for free (no paywall), organized in sections:

- **Múltiplos de Valuation**: P/E, P/S, P/CF, P/FCF, P/BV, P/TBV
- **Rentabilidade**: Gross/Op/Pretax/Net margins (TTM + 5YA)
- **Dados por ação**: Revenue/EPS/BV/Cash/CF per share
- **Eficácia de gestão**: ROE, ROA, ROI (TTM + 5YA)
- **Crescimento**: EPS/Sales growth (QoQ, YoY, 5yr)
- **Solidez financeira**: Quick/Current ratio, LT Debt/Capital, Total Debt/Capital
- **Eficiência**: Asset turnover, Inventory turnover, Receivables turnover
- **Dividendos**: Yield, Payout, Growth

Each metric shows `Empresa | Indústria` columns (company vs sector average).

### Reading ratios

Use `--full` because these pages lack semantic main-content elements:

```bash
cargo run -- browse navigate "https://br.investing.com/equities/jsl-on-nm-ratios"
cargo run -- browse content --max-chars 6000 --full
```

The output includes nav cruft at the top — scroll past it to the data section
starting with "Indicadores Fundamentalistas".

---

## Choosing between sources

| Need | Use |
|---|---|
| FCF Yield with historical trend + peers | Explorer: `fcf_yield_ltm` |
| Debt/Equity with trend + peers | Explorer: `debt_to_equity` |
| Quick overview of all fundamentals | Ratios page (`-ratios` URL) |
| Net Debt/EBITDA, Total Debt/EBITDA | Ratios page (free) — "Solidez financeira" |
| Specific metric deep-dive with formula | Explorer (if free) |

The ratios page is the reliable fallback — everything is free. The explorer
adds historical analysis and peer comparisons but many risk/leverage metrics
are paywalled.

---

## Common search terms (Portuguese)

| English | Portuguese (as shown on site) |
|---|---|
| FCF Yield | Rend. do Fluxo de Caixa Livre |
| Debt / Equity | Dívida / Ativo |
| Net Debt / EBITDA | Dívida líquida/Ebitda |
| Total Debt / EBITDA | Dívida total/EBITDA |
| Financial Leverage | Alavancagem Financeira |
| Stock Screener | Filtro de Ações |
| Data Explorer | Explorador de Dados |
| Profitability | Rentabilidade |
| Financial Strength | Solidez financeira |
| Growth | Crescimento |
| Valuation | Múltiplos de Valuation |
| Dividends | Dividendos |
