# Polymarket Data Pipeline - Comprehensive Usage Guide

## Table of Contents
1. [Installation & Setup](#installation--setup)
2. [Running the Pipeline](#running-the-pipeline)
3. [Understanding the Data](#understanding-the-data)
4. [Advanced Usage](#advanced-usage)
5. [Data Analysis Examples](#data-analysis-examples)
6. [Troubleshooting](#troubleshooting)

---

## Installation & Setup

### Prerequisites
- Python 3.8+
- pip or conda package manager
- ~50GB free disk space (for full historical data)

### Install Dependencies

```bash
# Using pip
pip install pandas polars requests gql flatten-json

# Or using conda
conda install pandas polars requests
pip install gql flatten-json
```

### Verify Installation

```bash
python -c "import polars, pandas, gql, requests; print('All dependencies installed!')"
```

---

## Running the Pipeline

### Option 1: Run Complete Pipeline (Recommended for First Time)

```bash
python update_all.py
```

This runs all three stages sequentially:
1. Fetches all markets from Polymarket
2. Scrapes order-filled events from blockchain
3. Processes raw orders into structured trades

**Expected Runtime**:
- First run: 2-6 hours (depends on network speed and data volume)
- Subsequent runs: 5-30 minutes (incremental updates only)

### Option 2: Run Individual Stages

```python
# Update markets only
from update_utils.update_markets import update_markets
update_markets()

# Update order events only
from update_utils.update_goldsky import update_goldsky
update_goldsky()

# Process trades only
from update_utils.process_live import process_live
process_live()
```

### Scheduling Regular Updates

Set up a cron job (Linux/Mac) to run daily:

```bash
# Edit crontab
crontab -e

# Add this line to run daily at 2 AM
0 2 * * * cd /path/to/poly_data && /usr/bin/python3 update_all.py >> logs/update.log 2>&1
```

Or use Task Scheduler on Windows.

---

## Understanding the Data

### Data Flow Architecture

```
Polymarket API          Goldsky GraphQL
      ↓                        ↓
 markets.csv           goldsky/orderFilled.csv
      ↓                        ↓
      └────────┬───────────────┘
               ↓
        process_live.py
               ↓
        processed/trades.csv
```

### File Descriptions

#### 1. `markets.csv` (48MB+)
Complete catalog of all Polymarket prediction markets.

**Key Fields:**
- `id`: Unique market identifier
- `question`: The prediction market question
- `answer1`, `answer2`: Outcome labels (e.g., "Yes"/"No", "Biden"/"Trump")
- `token1`, `token2`: 76-digit token IDs for each outcome
- `volume`: Total USD volume traded
- `createdAt`, `closedTime`: Market lifecycle timestamps
- `neg_risk`: Whether market uses negative risk (affects pricing)

**Example Row:**
```csv
2024-11-05T10:30:00Z,511234,"Will Bitcoin hit $100k in 2024?",Yes,No,False,btc-100k-2024,123...789,987...321,0xabc...def,5420000,btc,2024-12-31T23:59:59Z
```

#### 2. `goldsky/orderFilled.csv` (Growing file)
Raw blockchain order-fill events.

**Key Fields:**
- `timestamp`: Unix epoch timestamp
- `maker`, `taker`: Ethereum wallet addresses
- `makerAssetId`, `takerAssetId`: Token IDs ("0" = USDC, other = outcome token)
- `makerAmountFilled`, `takerAmountFilled`: Raw amounts (need ÷ 10^6 to get actual values)
- `transactionHash`: Blockchain transaction identifier

**Example Row:**
```csv
1699200000,0x1234...abcd,123...789,5000000,0x5678...efgh,0,4850000,0xabc...def
```
*Interpretation: Maker sold 5 USDC worth of token `123...789`, Taker paid 4.85 USDC*

#### 3. `processed/trades.csv` (Processed output)
Structured, human-readable trade data.

**Key Fields:**
- `timestamp`: Human-readable datetime
- `market_id`: Links to market in markets.csv
- `maker`, `taker`: Wallet addresses
- `nonusdc_side`: Which outcome token (token1/token2)
- `maker_direction`, `taker_direction`: BUY or SELL
- `price`: Price in USDC per outcome token (0-1 range)
- `usd_amount`, `token_amount`: Normalized amounts
- `transactionHash`: Blockchain reference

**Example Row:**
```csv
2024-11-05 10:30:15,511234,0x1234...abcd,0x5678...efgh,token1,SELL,BUY,0.65,1000.00,1538.46,0xabc...def
```
*Interpretation: Taker bought 1538.46 units of "Yes" tokens at $0.65 each for $1000*

### Data Relationships

```
markets.csv (market_id) ←→ trades.csv (market_id)
markets.csv (token1/token2) ←→ trades.csv (nonusdc_side)
markets.csv (token1/token2) ←→ orderFilled.csv (makerAssetId/takerAssetId)
```

---

## Advanced Usage

### Customizing Batch Sizes

```python
# Fetch markets in larger batches (faster, more memory)
from update_utils.update_markets import update_markets
update_markets(batch_size=1000)  # Default: 500

# Fetch smaller order batches (slower, less memory)
from update_utils.update_goldsky import scrape
scrape(at_once=500)  # Default: 1000
```

### Incremental Updates Explained

The pipeline is **fully resumable** - it tracks progress automatically:

**Markets**: Counts rows in `markets.csv` to set API offset
```python
# If markets.csv has 10,000 rows, next fetch starts at offset 10,000
current_offset = count_csv_lines(csv_filename)
```

**Goldsky**: Reads last timestamp from `goldsky/orderFilled.csv`
```python
# Resumes from last timestamp to avoid duplicate orders
last_timestamp = get_latest_timestamp()  # From last row of CSV
```

**Processing**: Tracks last processed transaction
```python
# Only processes new orders since last run
last_processed = {'timestamp': ..., 'transactionHash': ...}
```

### Handling Missing Markets

When processing encounters a token ID not in `markets.csv`, it automatically:
1. Calls Polymarket API to fetch that market
2. Saves to `missing_markets.csv`
3. The `get_markets()` utility function merges both files automatically

```python
from poly_utils.utils import get_markets, update_missing_tokens

# Load all markets (combines markets.csv + missing_markets.csv)
markets_df = get_markets()

# Manually fetch specific missing tokens
missing_tokens = ['123...789', '987...321']
update_missing_tokens(missing_tokens)
```

### Filtering Platform Trades

Exclude Polymarket's internal wallets from analysis:

```python
from poly_utils.utils import PLATFORM_WALLETS
import polars as pl

df = pl.scan_csv("processed/trades.csv").collect(streaming=True)

# Remove platform wallet trades
user_trades = df.filter(
    ~pl.col("maker").is_in(PLATFORM_WALLETS) &
    ~pl.col("taker").is_in(PLATFORM_WALLETS)
)
```

---

## Data Analysis Examples

### Example 1: Analyze Market Volume

```python
import polars as pl
from poly_utils.utils import get_markets

# Load markets
markets = get_markets()

# Top 10 markets by volume
top_markets = (
    markets
    .sort("volume", descending=True)
    .select(["question", "volume", "ticker"])
    .head(10)
)

print(top_markets)
```

### Example 2: Track Specific Trader Performance

```python
import polars as pl

# Load trades
trades = pl.scan_csv("processed/trades.csv").collect(streaming=True)
trades = trades.with_columns(
    pl.col("timestamp").str.to_datetime()
)

# Define trader
TRADER = "0x9d84ce0306f8551e02efef1680475fc0f1dc1344"

# Get all their trades (filter by maker)
trader_trades = trades.filter(pl.col("maker") == TRADER)

# Calculate total volume
total_volume = trader_trades["usd_amount"].sum()
print(f"Total volume: ${total_volume:,.2f}")

# Count markets traded
unique_markets = trader_trades["market_id"].n_unique()
print(f"Markets traded: {unique_markets}")

# Win rate (requires market resolution data - not included)
```

**Important Note**: Always filter by `maker` column for a user's perspective. This is how Polymarket contracts generate events - the maker's view includes their price and direction.

### Example 3: Market Price History

```python
import polars as pl
import matplotlib.pyplot as plt

# Load trades for a specific market
market_id = "511234"
trades = pl.scan_csv("processed/trades.csv").collect(streaming=True)

market_trades = (
    trades
    .filter(pl.col("market_id") == market_id)
    .with_columns(pl.col("timestamp").str.to_datetime())
    .sort("timestamp")
)

# Plot price over time
plt.figure(figsize=(12, 6))
plt.plot(
    market_trades["timestamp"],
    market_trades["price"],
    alpha=0.5
)
plt.xlabel("Date")
plt.ylabel("Price (USDC)")
plt.title(f"Market {market_id} Price History")
plt.grid(True)
plt.show()
```

### Example 4: Daily Trading Volume

```python
import polars as pl

trades = pl.scan_csv("processed/trades.csv").collect(streaming=True)
trades = trades.with_columns(
    pl.col("timestamp").str.to_datetime()
)

# Aggregate by day
daily_volume = (
    trades
    .group_by(pl.col("timestamp").dt.date().alias("date"))
    .agg([
        pl.col("usd_amount").sum().alias("total_volume"),
        pl.col("transactionHash").count().alias("trade_count")
    ])
    .sort("date")
)

print(daily_volume.tail(30))  # Last 30 days
```

### Example 5: Most Active Markets Today

```python
import polars as pl
from datetime import datetime, timedelta
from poly_utils.utils import get_markets

# Load data
trades = pl.scan_csv("processed/trades.csv").collect(streaming=True)
trades = trades.with_columns(pl.col("timestamp").str.to_datetime())
markets = get_markets()

# Filter last 24 hours
yesterday = datetime.now() - timedelta(days=1)
recent_trades = trades.filter(pl.col("timestamp") > yesterday)

# Aggregate by market
market_activity = (
    recent_trades
    .group_by("market_id")
    .agg([
        pl.col("usd_amount").sum().alias("volume_24h"),
        pl.col("transactionHash").count().alias("trades_24h")
    ])
    .sort("volume_24h", descending=True)
    .head(10)
)

# Join with market metadata
result = market_activity.join(
    markets.select(["id", "question"]),
    left_on="market_id",
    right_on="id"
)

print(result)
```

### Example 6: Identify "Smart Money" Wallets

```python
import polars as pl
from poly_utils.utils import PLATFORM_WALLETS

trades = pl.scan_csv("processed/trades.csv").collect(streaming=True)

# Calculate per-wallet statistics
wallet_stats = (
    trades
    .filter(~pl.col("maker").is_in(PLATFORM_WALLETS))
    .group_by("maker")
    .agg([
        pl.col("usd_amount").sum().alias("total_volume"),
        pl.col("market_id").n_unique().alias("markets_traded"),
        pl.col("transactionHash").count().alias("trade_count")
    ])
    .with_columns([
        (pl.col("total_volume") / pl.col("trade_count")).alias("avg_trade_size"),
        (pl.col("trade_count") / pl.col("markets_traded")).alias("trades_per_market")
    ])
    .sort("total_volume", descending=True)
    .head(50)
)

print(wallet_stats)
```

---

## Troubleshooting

### Issue: "No module named 'gql'"
**Solution**: Install missing dependency
```bash
pip install gql[all]
```

### Issue: "PermissionError: markets.csv"
**Solution**: File is open in another program (Excel, etc.). Close it and retry.

### Issue: Pipeline stops at random points
**Solution**: Network timeout. Just re-run - it resumes automatically.

### Issue: "Market not found" warnings during processing
**Solution**: Normal. The pipeline auto-fetches missing markets. Re-run `update_markets()` to get latest markets first.

### Issue: Duplicate trades in output
**Solution**: Delete `processed/trades.csv` and re-run `process_live()` to rebuild from scratch.

### Issue: Out of memory errors
**Solution**: Process in smaller batches:
```python
from update_utils.update_goldsky import scrape
scrape(at_once=500)  # Reduce from default 1000
```

### Issue: API rate limiting (429 errors)
**Solution**: The pipeline handles this automatically with backoff. If persistent, add manual delays:
```python
import time
time.sleep(10)  # Wait 10 seconds between runs
```

### Issue: Timestamps are wrong timezone
**Solution**: Timestamps are in UTC. Convert to your timezone:
```python
import polars as pl

df = pl.scan_csv("processed/trades.csv").collect(streaming=True)
df = df.with_columns(
    pl.col("timestamp")
    .str.to_datetime()
    .dt.convert_time_zone("America/New_York")  # Change to your timezone
)
```

### Issue: Large CSV files slow to open
**Solution**: Use Polars for fast lazy loading:
```python
import polars as pl

# Lazy scan (doesn't load into memory)
df = pl.scan_csv("processed/trades.csv")

# Only load filtered subset
filtered = df.filter(pl.col("market_id") == "511234").collect()
```

---

## Performance Tips

1. **Use Polars instead of Pandas** for large datasets (10x+ faster)
2. **Filter early** when loading CSVs to reduce memory usage
3. **Run updates off-peak** to avoid API rate limits (e.g., 2-6 AM)
4. **Monitor disk space** - full dataset can grow to 50GB+
5. **Use `streaming=True`** for memory-efficient Polars operations:
   ```python
   df = pl.scan_csv("processed/trades.csv").collect(streaming=True)
   ```

---

## Advanced Topics

### Custom Market Filters

```python
from update_utils.update_markets import update_markets
import polars as pl

# Only process high-volume markets
markets = pl.read_csv("markets.csv")
high_volume = markets.filter(pl.col("volume") > 100000)

# Use these market IDs in your analysis
high_volume_ids = high_volume["id"].to_list()
```

### Parallel Processing

For very large datasets, process trades in parallel:

```python
import polars as pl
from multiprocessing import Pool

def process_chunk(market_ids):
    df = pl.scan_csv("processed/trades.csv").collect(streaming=True)
    return df.filter(pl.col("market_id").is_in(market_ids))

# Split markets into chunks
markets = get_markets()
market_chunks = [markets["id"][i:i+1000] for i in range(0, len(markets), 1000)]

# Process in parallel
with Pool(4) as p:
    results = p.map(process_chunk, market_chunks)

# Combine results
final_df = pl.concat(results)
```

---

## Getting Help

- **Check logs**: The pipeline prints detailed progress messages
- **Inspect raw data**: Open CSVs directly to verify data quality
- **Test small**: Run on a single market first to validate your approach
- **Community**: Search existing issues or open a new one on GitHub

---

## Quick Reference

### Essential Commands

```bash
# Full pipeline update
python update_all.py

# Check last market fetched
tail -n 1 markets.csv | cut -d',' -f1-3

# Check last order timestamp
tail -n 1 goldsky/orderFilled.csv | cut -d',' -f1

# Count total trades
wc -l processed/trades.csv
```

### Essential Python Snippets

```python
# Load all data
from poly_utils.utils import get_markets
import polars as pl

markets = get_markets()
trades = pl.scan_csv("processed/trades.csv").collect(streaming=True)
orders = pl.scan_csv("goldsky/orderFilled.csv").collect(streaming=True)

# Quick stats
print(f"Markets: {len(markets):,}")
print(f"Trades: {len(trades):,}")
print(f"Total Volume: ${trades['usd_amount'].sum():,.2f}")
```

---

**Last Updated**: 2025-10-03
**Pipeline Version**: 1.0
**Maintainer**: Research use - modify as needed
