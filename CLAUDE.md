# omens — project instructions

## Running the CLI

**Always use `cargo run --` instead of a compiled binary.**

```bash
cargo run -- collect run --tickers BRCR11
cargo run -- report latest          # signals from the most recent run
cargo run -- report since 30d       # cross-run: signals with published_at in last 30 days
# etc.
```

## Storage location

The SQLite database lives at `~/.omens/db/omens.db` (not in the project directory). Use this path for any direct `sqlite3` queries.

## Skills

- **`/use-omens`** — FII data pipeline (explore → collect → report → analysis). See `skills/use-omens/SKILL.md`.
- **`/browse`** — Interactive CDP browser session for navigating, scraping, and interacting with any website. Provided by the [caravela](../caravela) tool, which omens uses for its browser + display infrastructure.
- **`/br-investing-pro`** — Look up financial metrics for B3 equities on br.investing.com (Data Explorer, ratios pages). See `skills/br-investing-pro/SKILL.md`.

## Browser & display infrastructure

omens' browser launching, Chromium install/pin (`omens browser …`), and the
Weston RDP display (`omens display …`) are provided by the [caravela](../caravela)
library, pointed at `~/.omens`. The interactive `browse` session is caravela's
(`caravela …`), not an omens subcommand.
