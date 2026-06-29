# Polymarket Data (v2)

[![License: GPL-3.0](https://img.shields.io/badge/License-GPL--3.0-blue.svg)](https://opensource.org/licenses/GPL-3.0)
[![GitHub stars](https://img.shields.io/github/stars/warproxxx/poly_data)](https://github.com/warproxxx/poly_data/stargazers)
[![GitHub last commit](https://img.shields.io/github/last-commit/warproxxx/poly_data)](https://github.com/warproxxx/poly_data/commits/main)
[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)

A pipeline for fetching, processing, and analyzing Polymarket v2 trading data. Streams order events directly from the Polymarket **CTF Exchange V2** contract on Polygon via [Envio HyperSync](https://docs.envio.dev/docs/HyperSync/overview), joins them with market metadata from the Polymarket CLOB API, and writes structured trades to CSV.

## ⚠️ v1 → v2 migration

Polymarket migrated to a new set of CTF Exchange contracts on **2026-04-28** and stopped supporting their old subgraph indexer. The old pipeline in this repo (Goldsky subgraph + GraphQL polling) **no longer returns complete data**, so it has been removed.

The previous version is preserved at the [`v1-final`](https://github.com/warproxxx/poly_data/tree/v1-final) tag if you need it for historical analysis. **For any new work, use this v2 version.**

The V1 retriever used goldsky, but now goldsky only gives data through a turbo pipeline that is expensive and complex. 

V2 reads directly from on-chain events via HyperSync, which streams logs (with block timestamps inline) across the full chain in a single connection without RPC throttling. A single request scans hundreds of thousands of blocks, so the full backfill is only a handful of requests. HyperSync requires a free API token (mandatory since 2025-11-03); generate one at [envio.dev/app/api-tokens](https://envio.dev/app/api-tokens) and set it as `HYPERSYNC_API`.


## Configuration

All tuning is via environment variables.

| Variable | Default | What it does |
|---|---|---|
| `HYPERSYNC_API` | _(unset)_ | **Required.** Envio HyperSync bearer token (mandatory since 2025-11-03). Generate a free one at [envio.dev/app/api-tokens](https://envio.dev/app/api-tokens) — make sure it has **HyperSync** product access. Without it the endpoint returns 401/403. |
| `POLYGON_HYPERSYNC_URL` | `https://polygon.hypersync.xyz` | Envio HyperSync endpoint for Polygon. Override only to point at a different network or a self-hosted instance. |
| `PROCESS_CHUNK_SIZE` | `0` | When `>0`, streams `data/orderFilled.csv` through chunks if your machine has limitations processing the data. Use `500000` if processing OOMs on your machine. |

Set them in `.env` file:

```bash
export HYPERSYNC_API="your-token-here"     # required — get one at envio.dev/app/api-tokens
export PROCESS_CHUNK_SIZE=500000           # only if RAM is tight
```

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [HyperSync API token](#hypersync-api-token)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Data Files](#data-files)
- [Pipeline Stages](#pipeline-stages)
- [Resumable & Incremental](#resumable--incremental)
- [Troubleshooting](#troubleshooting)
- [Tests](#tests)
- [Analysis](#analysis)
- [License](#license)

## Overview

`update.py` runs three stages:

1. **Markets** — fetches all markets from the Polymarket **CLOB** API (`/markets`, 1000/page, concurrent) into `data/markets.csv`. Runs to completion first so the full list exists before any trade is processed.
2. **Chain** — streams `OrderFilled` events from the CTF Exchange V2 contract (`0xE111180000d2663C0091e4f400237545B87B996B`) on Polygon via Envio HyperSync. Resumable from the last scanned block.
3. **Process** — joins order events with market metadata to produce labeled trades with price, USD amount, and BUY/SELL direction.

The stages run **sequentially** (markets → chain → process): the complete market list is built first, so every scraped trade can be labeled against it.

## Installation

This project uses [UV](https://docs.astral.sh/uv/) for fast package management.

```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Or with pip
pip install uv
```

Then install dependencies:

```bash
uv sync
```

## HyperSync API token

The chain stage streams from Envio HyperSync, which **requires a bearer token** (mandatory since 2025-11-03). The free tier is enough for this pipeline. Get one:

1. Sign in at **[envio.dev/app/api-tokens](https://envio.dev/app/api-tokens)**.
2. Create a new token. **Make sure it has _HyperSync_ product access** — a token scoped only to HyperRPC/HyperIndex authenticates but returns `403 "Your token does not have access to this product"` on data queries.
3. Add it to a `.env` file in the project root:

   ```bash
   HYPERSYNC_API="your-token-here"
   ```

Without it the run stops immediately with a message telling you to set `HYPERSYNC_API`. The token authenticates the request; a single request scans hundreds of thousands of blocks, so the free-tier rate limit is a non-issue.

## Quick Start

```bash
uv run poly-data        # or: uv run python update.py
```

That's it. Runs markets → chain → process in order. **First run is the long one** — the full market list from CLOB takes a couple of minutes, then the initial chain backfill from v2 genesis (~April 2026) over HyperSync runs after it. Subsequent runs only pull deltas.

To run any stage individually:

```bash
uv run python -m update_utils.update_markets
uv run python -m update_utils.update_chain
uv run python -m update_utils.process_live
```

## Project Structure

```
poly_data/
├── update.py                  # thin shim → update_utils.pipeline.main
├── update_utils/
│   ├── pipeline.py            # orchestrator: markets → chain → process (poly-data entrypoint)
│   ├── update_markets.py      # Polymarket CLOB /markets → markets.csv
│   ├── update_chain.py        # HyperSync OrderFilled events → data/orderFilled.csv
│   └── process_live.py        # join orders ↔ markets → processed/trades.csv
├── poly_utils/
│   └── utils.py               # market loader, missing-token backfill
├── tests/                     # pytest unit tests (pure, offline)
├── data/                      # all generated data + resume state (gitignored)
│   ├── markets.csv            # all markets (id = condition_id, clobTokenIds, …)
│   ├── missing_markets.csv    # markets backfilled per-token from trades
│   ├── markets_state.json     # CLOB pagination cursor for incremental resume
│   ├── orderFilled.csv        # raw order events from chain
│   └── cursor_state.json      # last scanned block
└── processed/                 # user-facing output (gitignored)
    └── trades.csv             # labeled trades for analysis
```

## Data Files

### `data/markets.csv`
All markets from the Polymarket CLOB API. **Every field the CLOB market object carries is preserved as-is**; nested objects (`tokens`, `tags`, `rewards`, etc.) are stored as JSON strings. Two derived columns are prepended for the downstream join: `id` (= the on-chain `condition_id`) and `clobTokenIds` (JSON array of the market's two token IDs). Column order is set by the first market fetched.

Key fields used downstream: `id` (= `condition_id`), `clobTokenIds` (JSON array — first element = `token1`, second = `token2`), `question`, `market_slug`, `closed`.

### `data/orderFilled.csv`
Raw `OrderFilled` events decoded from the chain. Schema:

| Column | Notes |
|---|---|
| `timestamp` | Unix seconds (from block timestamp) |
| `maker` | Maker address, lowercase |
| `makerAssetId` | `"0"` if maker is paying USDC; otherwise the CTF token ID |
| `makerAmountFilled` | Raw integer (6 decimals — USDC and CTF tokens both use 6) |
| `taker` | Taker address, lowercase |
| `takerAssetId` | `"0"` if taker is paying USDC; otherwise the CTF token ID |
| `takerAmountFilled` | Raw integer (6 decimals) |
| `transactionHash` | Polygon transaction hash |

The v2 `OrderFilled` event natively carries a single `tokenId` + `side` (BUY=0, SELL=1) referring to the maker's order. The reader maps that back to the v1-compatible maker/taker/asset schema above so downstream code stays simple. The CTF Exchange contract address `0xe111180000d2663c0091e4f400237545b87b996b` may appear as `taker` for some events — this is the contract acting as an intermediary for CTF mint/burn flows during cross-side matches; treat it as a sub-event rather than a counterparty.

### `processed/trades.csv`
Labeled trades for analysis:

| Column | Notes |
|---|---|
| `timestamp` | datetime |
| `market_id` | from markets.csv (null if market wasn't found) |
| `maker`, `taker` | addresses |
| `nonusdc_side` | `"token1"` or `"token2"` |
| `maker_direction`, `taker_direction` | `"BUY"` / `"SELL"` |
| `price` | USDC per outcome token (0–1) |
| `usd_amount` | trade size in USD |
| `token_amount` | outcome tokens transferred |
| `transactionHash` | |

## Pipeline Stages

### 1. `update_markets` — Polymarket CLOB `/markets`

Pages through the CLOB `/markets` endpoint (1000 markets/page, offset cursor) with concurrent request waves, covering the full ~1.5M-market history — closed and active — in a couple of minutes (vs. the old Gamma keyset's ~hour at 100/page). Each market is written to `data/markets.csv` with `id` = `condition_id` and `clobTokenIds` derived from its `tokens`. The next offset is saved to `data/markets_state.json` so an interrupted run resumes.

### 2. `update_chain` — HyperSync `OrderFilled` stream

Opens a HyperSync stream filtered to the CTF Exchange V2 contract and the `OrderFilled` topic. HyperSync returns logs in bulk along with their block timestamps in the same response, so there's no separate `eth_getBlock` pass. Each batch is ABI-decoded, appended to `data/orderFilled.csv`, and the `next_block` cursor is persisted to `data/cursor_state.json`. A 20-block reorg buffer is applied to the chain tip.

### 3. `process_live`

Reads `data/orderFilled.csv`, finds the resume point in `processed/trades.csv`, joins new orders against `get_markets()` (which parses `clobTokenIds` into `token1`/`token2`), computes price/USD/direction, and appends to `processed/trades.csv`.

If any trade references a token ID not in `markets.csv`, it's backfilled into `missing_markets.csv` via batched, parallel Gamma API requests before the join (the CLOB list isn't queryable by token ID, so Gamma's `clob_token_ids` lookup is used here), with `id` = `conditionId` to stay consistent with `markets.csv`. For memory-bounded processing on large datasets, set `PROCESS_CHUNK_SIZE` — see [Configuration](#configuration).

## Tests

Unit tests cover the data-correctness logic — the `OrderFilled` decode and side→asset mapping, the trade-labeling transform (price, BUY/SELL direction, USD/token amounts), the CLOB market row mapping, and the parsing helpers. They're pure and offline (no network, no API token needed).

```bash
uv run pytest
```

Tests live in `tests/`. They exercise the pure functions directly, so a regression like a flipped trade side or inverted price fails fast.

## Analysis

```python
import polars as pl
from poly_utils.utils import get_markets

markets_df = get_markets()           # parses clobTokenIds → token1/token2
trades_df = pl.read_csv("processed/trades.csv", try_parse_dates=True)

# Filter trades for a specific user (filter on `maker` — see note below)
USERS = {
    'domah': '0x9d84ce0306f8551e02efef1680475fc0f1dc1344',
    '50pence': '0x3cf3e8d5427aed066a7a5926980600f6c3cf87b3',
}
trader_df = trades_df.filter(pl.col("maker") == USERS['domah'])
```

**Note on user filtering**: Polymarket emits `OrderFilled` from the maker's perspective at the contract level. When you want a user's trades from their side, filter on `maker`, not `taker`.

## License

GPL-3.0 — see [LICENSE](LICENSE).
