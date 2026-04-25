# Polymarket Data v2

Partitioned Parquet pipeline for fetching, processing, and analyzing
Polymarket trading data. Cross-platform (Linux + Windows + macOS).

## Sources

- `markets` — Polymarket markets metadata
- `missing_markets` — markets discovered while processing trades
- `orderFilled` — Goldsky order-filled events
- `trades` — derived structured trades

All data lives under `data/<source>/year=YYYY/month=MM/{run-*.parquet,month.parquet}`.

## Install

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh         # macOS / Linux
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"  # Windows

# Install deps
uv sync --extra dev
```

## Quick start

```bash
uv run poly-data update-all              # markets + goldsky + process
uv run poly-data compact                 # nightly: dedup + compact month dirs
uv run poly-data push-hf --repo USER/REPO  # publish snapshot to HF Hub
```

## Subcommands

| Subcommand        | Purpose                                              |
|-------------------|------------------------------------------------------|
| `update-markets`  | Fetch Polymarket markets API                         |
| `update-goldsky`  | Scrape Goldsky orderFilled (`--workers N` parallel)  |
| `process`         | Derive `trades` from `orderFilled`                   |
| `compact`         | Rewrite month partitions (dedup, single file)        |
| `push-hf`         | Upload snapshot to HuggingFace Hub                   |
| `update-all`      | Legacy flow: markets → goldsky → process             |

## Migrating from v1 (CSV)

```bash
uv run python scripts/migrate_csv_to_parquet.py
```

This reads existing `goldsky/orderFilled.csv`, `markets.csv`,
`missing_markets.csv`, and `processed/trades.csv` and writes them into
`data/<source>/`. Legacy CSVs are not deleted; remove them when you've
verified.

## Reading in analysis code

```python
import polars as pl

trades = pl.scan_parquet("data/trades/**/*.parquet", hive_partitioning=True)
big_trades = trades.filter(pl.col("usd_amount") > 10_000).collect()
```

For ad-hoc SQL:

```python
import duckdb
duckdb.sql("SELECT count(*) FROM 'data/orderFilled/**/*.parquet'").show()
```

## Environment variables

| Var          | Purpose                                              |
|--------------|------------------------------------------------------|
| `HF_TOKEN`   | HuggingFace Hub auth token (for `push-hf`)           |

## License

MIT.
