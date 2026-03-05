# omens — project instructions

## Running the CLI

**Always use `cargo run --` instead of a compiled binary.**

```bash
cargo run -- collect run --tickers BRCR11
cargo run -- report latest          # signals from the most recent run
cargo run -- report since 30d       # cross-run: signals with published_at in last 30 days
# etc.
```

## Skills

- **`/use-omens`** — FII data pipeline (explore → collect → report → analysis). See `skills/use-omens/SKILL.md`.
- **`/browse`** — Interactive CDP browser session for navigating, scraping, and interacting with any website. See `skills/browse/SKILL.md`.
- **`/br-investing-pro`** — Look up financial metrics for B3 equities on br.investing.com (Data Explorer, ratios pages). See `skills/br-investing-pro/SKILL.md`.
