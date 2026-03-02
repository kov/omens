# omens — project instructions

## Running the CLI

**Always use `cargo run --` instead of a compiled binary.**

```bash
cargo run -- collect run --tickers BRCR11
cargo run -- report latest          # signals from the most recent run
cargo run -- report since 30d       # cross-run: signals with published_at in last 30 days
# etc.
```

## Pipeline skill

Full pipeline documentation (explore → collect → report → analysis) is in:

```
skills/use-omens/SKILL.md
```

Invoke it with `/use-omens` when you need to run any part of the omens pipeline.
