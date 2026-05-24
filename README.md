# Polymarket Data (v2)

[![License: GPL-3.0](https://img.shields.io/badge/License-GPL--3.0-blue.svg)](https://opensource.org/licenses/GPL-3.0)
[![GitHub stars](https://img.shields.io/github/stars/warproxxx/poly_data)](https://github.com/warproxxx/poly_data/stargazers)
[![GitHub last commit](https://img.shields.io/github/last-commit/warproxxx/poly_data)](https://github.com/warproxxx/poly_data/commits/main)
[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)

A pipeline for fetching, processing, and analyzing Polymarket v2 trading data. Reads order events directly from the Polymarket **CTF Exchange V2** contract on Polygon via JSON-RPC, joins them with market metadata from the Polymarket Gamma API, and writes structured trades to CSV.

## ⚠️ v1 → v2 migration

Polymarket migrated to a new set of CTF Exchange contracts on **2026-04-28** and stopped supporting their old subgraph indexer. The old pipeline in this repo (Goldsky subgraph + GraphQL polling) **no longer returns complete data**, so it has been removed.

The previous version is preserved at the [`v1-final`](https://github.com/warproxxx/poly_data/tree/v1-final) tag if you need it for historical analysis. **For any new work, use this v2 version.**

## Configuration

All tuning is via environment variables. 

| Variable | Default | What it does |
|---|---|---|
| `POLYGON_RPC_URL` | `https://polygon-bor-rpc.publicnode.com` | Polygon JSON-RPC endpoint. Public default works but is slow and times out under sustained backfill. Free tier of [QuickNode](https://quicknode.com/signup?via=daniel-s) or [Chainstack](https://chainstack.com) is much more reliable. Paid tier recommended if you're doing it in a serious environment. |
| `PROCESS_CHUNK_SIZE` | `0` | When `>0`, streams `data/orderFilled.csv` through chunks if your machine has limitations processing the data. Use `500000` if processing OOMs on your machine. |

Set them in `.env` file:

```bash
export POLYGON_RPC_URL="https://your-endpoint-here"
export PROCESS_CHUNK_SIZE=500000   # only if RAM is tight
```

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Polygon RPC setup](#polygon-rpc-setup)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Data Files](#data-files)
- [Pipeline Stages](#pipeline-stages)
- [Resumable & Incremental](#resumable--incremental)
- [Troubleshooting](#troubleshooting)
- [Analysis](#analysis)
- [License](#license)

## Overview

`update.py` runs three stages:

1. **Markets** — fetches all Polymarket markets (closed + active) via the Gamma **keyset** API (`/markets/keyset`). Resumable from a saved cursor; subsequent runs only fetch newly created markets.
2. **Chain** — reads `OrderFilled` events from the CTF Exchange V2 contract (`0xE111180000d2663C0091e4f400237545B87B996B`) on Polygon via direct JSON-RPC. Resumable from the last scanned block.
3. **Process** — joins order events with market metadata to produce labeled trades with price, USD amount, and BUY/SELL direction.

Stages 1 and 2 run in **parallel** (different APIs, zero contention), so total wall time is `max(markets, chain)` rather than the sum.

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

## Polygon RPC setup

The V1 retriever used goldsky's very leniet stack for free data but now goldsky only gives data thru turbo pipeline. It is expensive and high dependency. Other third party options are high dependency too. So I have decided to get the data directly onchain. For that it needs an RPC URL. If not set, it defaults to `https://polygon-bor-rpc.publicnode.com`

For faster retrieval get a node from Quicknode or Alchemy in their premier tiers. If you have no idea what that is, you can sign up [here](https://quicknode.com/signup?via=daniel-s)

## Quick Start

```bash
uv run python update.py
```

That's it. Runs markets + chain in parallel, then processes trades. **First run is the long one** — initial markets fetch is ~hour, initial chain backfill from v2 genesis (~April 2026) is several hours on a free RPC. Subsequent runs are seconds.

To run any stage individually:

```bash
uv run python -m update_utils.update_markets
uv run python -m update_utils.update_chain
uv run python -m update_utils.process_live
```

## Project Structure

```
poly_data/
├── update.py                  # orchestrator: markets + chain in parallel, then process
├── update_utils/
│   ├── update_markets.py      # Polymarket Gamma keyset API → markets.csv
│   ├── update_chain.py        # Polygon RPC OrderFilled events → data/orderFilled.csv
│   └── process_live.py        # join orders ↔ markets → processed/trades.csv
├── poly_utils/
│   └── utils.py               # market loader, missing-token backfill
├── data/                      # all generated data + resume state (gitignored)
│   ├── markets.csv            # all markets, all fields preserved
│   ├── missing_markets.csv    # markets backfilled per-token from trades
│   ├── markets_*_part.csv     # per-pass keyset output (closed/active)
│   ├── markets_*_state.json   # keyset cursor state for incremental resume
│   ├── orderFilled.csv        # raw order events from chain
│   └── cursor_state.json      # last scanned block
└── processed/                 # user-facing output (gitignored)
    └── trades.csv             # labeled trades for analysis
```

## Data Files

### `data/markets.csv`
All markets returned by the Gamma keyset API. **All API fields are preserved as-is**; nested objects (`outcomes`, `clobTokenIds`, `events`, etc.) are stored as JSON strings. The exact column set is determined by the first batch the API returns and persisted in `markets_*_state.json` so resumed runs stay consistent.

Key fields used downstream: `id`, `question`, `slug`, `conditionId`, `clobTokenIds` (JSON array — first element = `token1`, second = `token2`), `closedTime`, `volume`.

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

### 1. `update_markets` — Polymarket Gamma keyset API

Pages through `/markets/keyset` with `closed=true` then `closed=false`. Saves a cursor per pass; on subsequent runs, resumes from the cursor and only pulls newly created markets.

Outputs `markets_closed_part.csv` + `markets_active_part.csv` (kept across runs as the source of truth) and merges them into `markets.csv` at the end of each run.

### 2. `update_chain` — Polygon RPC `OrderFilled` events

Calls `eth_getLogs` on the CTF Exchange V2 contract in 1000-block windows (auto-halves on errors). Decodes events with the ABI, looks up block timestamps (cached per chunk), and appends to `data/orderFilled.csv`. Saves the last scanned block to `data/cursor_state.json`.

### 3. `process_live`

Reads `data/orderFilled.csv`, finds the resume point in `processed/trades.csv`, joins new orders against `get_markets()` (which parses `clobTokenIds` into `token1`/`token2`), computes price/USD/direction, and appends to `processed/trades.csv`.

If any trade references a token ID not in `markets.csv`, it's backfilled into `missing_markets.csv` via a per-token Gamma API call before the join. For memory-bounded processing on large datasets, set `PROCESS_CHUNK_SIZE` — see [Configuration](#configuration).

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
