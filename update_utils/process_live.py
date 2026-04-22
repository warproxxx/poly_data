import warnings
warnings.filterwarnings('ignore')

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import io
import json
import polars as pl
from poly_utils.utils import get_markets, update_missing_tokens

import subprocess

import pandas as pd

OFFSET_FILE = 'processed/source_offset.json'
BOOTSTRAP_CHUNK_ROWS = 100_000   # rows per chunk (~8 MB each) during full-file scans

def _load_offset():
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE, 'r') as f:
            return json.load(f).get('byte_offset', 0)
    return 0

def _save_offset(byte_offset):
    os.makedirs('processed', exist_ok=True)
    with open(OFFSET_FILE, 'w') as f:
        json.dump({'byte_offset': byte_offset}, f)

def _load_markets_long():
    """Load and pivot markets once; reuse across chunks."""
    markets_df = get_markets()
    markets_df = markets_df.rename({'id': 'market_id'})
    markets_long = (
        markets_df
        .select(["market_id", "token1", "token2"])
        .melt(id_vars="market_id", value_vars=["token1", "token2"],
              variable_name="side", value_name="asset_id")
    )
    return markets_long

def get_processed_df(df, markets_long=None):
    if markets_long is None:
        markets_long = _load_markets_long()

    # Identify the non-USDC asset for each trade
    df = df.with_columns(
        pl.when(pl.col("makerAssetId") != "0")
        .then(pl.col("makerAssetId"))
        .otherwise(pl.col("takerAssetId"))
        .alias("nonusdc_asset_id")
    )

    # Join to recover market_id + side
    df = df.join(
        markets_long,
        left_on="nonusdc_asset_id",
        right_on="asset_id",
        how="left",
    )

    df = df.with_columns([
        pl.when(pl.col("makerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("makerAsset"),
        pl.when(pl.col("takerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("takerAsset"),
        pl.col("market_id"),
    ])

    df = df[['timestamp', 'market_id', 'maker', 'makerAsset', 'makerAmountFilled', 'taker', 'takerAsset', 'takerAmountFilled', 'transactionHash']]

    df = df.with_columns([
        (pl.col("makerAmountFilled") / 10**6).alias("makerAmountFilled"),
        (pl.col("takerAmountFilled") / 10**6).alias("takerAmountFilled"),
    ])

    df = df.with_columns([
        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.lit("BUY"))
        .otherwise(pl.lit("SELL"))
        .alias("taker_direction"),

        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.lit("SELL"))
        .otherwise(pl.lit("BUY"))
        .alias("maker_direction"),
    ])

    df = df.with_columns([
        pl.when(pl.col("makerAsset") != "USDC")
        .then(pl.col("makerAsset"))
        .otherwise(pl.col("takerAsset"))
        .alias("nonusdc_side"),

        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.col("takerAmountFilled"))
        .otherwise(pl.col("makerAmountFilled"))
        .alias("usd_amount"),

        pl.when(pl.col("takerAsset") != "USDC")
        .then(pl.col("takerAmountFilled"))
        .otherwise(pl.col("makerAmountFilled"))
        .alias("token_amount"),

        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.col("takerAmountFilled") / pl.col("makerAmountFilled"))
        .otherwise(pl.col("makerAmountFilled") / pl.col("takerAmountFilled"))
        .cast(pl.Float64)
        .alias("price")
    ])

    df = df[['timestamp', 'market_id', 'maker', 'taker', 'nonusdc_side', 'maker_direction', 'taker_direction', 'price', 'usd_amount', 'token_amount', 'transactionHash']]
    return df


def _process_chunked(source_file, op_file, schema_overrides, markets_long,
                     skip_before_epoch=None, anchor=None):
    """
    Stream source_file in BOOTSTRAP_CHUNK_ROWS chunks, optionally skipping rows
    before skip_before_epoch and before the anchor row.

    anchor dict keys: timestamp (pd.Timestamp), transactionHash, maker, taker
    Returns total rows written.
    """
    reader = pl.read_csv_batched(
        source_file,
        schema_overrides=schema_overrides,
        batch_size=BOOTSTRAP_CHUNK_ROWS,
    )

    anchor_found = (anchor is None)  # if no anchor needed, treat as already found
    total_written = 0
    chunk_num = 0

    while True:
        batches = reader.next_batches(1)
        if not batches:
            break
        batch = batches[0]

        batch = batch.with_columns(
            pl.from_epoch(pl.col('timestamp'), time_unit='s').alias('timestamp')
        )

        # Fast-skip entire chunks that end before the resume point
        if skip_before_epoch is not None:
            max_ts = batch['timestamp'].max()
            if max_ts is not None and max_ts < pd.Timestamp(skip_before_epoch, unit='s', tz='UTC'):
                continue

        if not anchor_found:
            # Find anchor row and discard everything up to and including it
            batch = batch.with_row_index("_idx")
            hits = batch.filter(
                (pl.col('timestamp') == anchor['timestamp']) &
                (pl.col('transactionHash') == anchor['transactionHash']) &
                (pl.col('maker') == anchor['maker']) &
                (pl.col('taker') == anchor['taker'])
            )
            if len(hits) > 0:
                cut = hits['_idx'][-1]   # last matching row (most conservative)
                batch = batch.filter(pl.col('_idx') > cut)
                anchor_found = True
            batch = batch.drop('_idx')

        if len(batch) == 0:
            continue

        new_df = get_processed_df(batch, markets_long)
        total_written += len(new_df)
        chunk_num += 1

        file_exists = os.path.isfile(op_file)
        with open(op_file, mode="ab") as f:
            new_df.write_csv(f, include_header=not file_exists)

        if chunk_num % 10 == 0:
            print(f"  [chunk {chunk_num}] +{len(new_df):,} rows  (total written: {total_written:,})")

    return total_written


def process_live():
    source_file = 'goldsky/orderFilled.csv'
    op_file = 'processed/trades.csv'

    print("=" * 60)
    print("[*] Processing Live Trades")
    print("=" * 60)

    schema_overrides = {
        "takerAssetId": pl.Utf8,
        "makerAssetId": pl.Utf8,
    }

    current_size = os.path.getsize(source_file)
    last_offset = _load_offset()

    os.makedirs('processed', exist_ok=True)

    # ------------------------------------------------------------------
    # PATH 1 — Normal incremental run (offset file exists)
    # Seek directly to byte offset; only read new bytes appended since
    # the last run.  No full-file scan needed.
    # ------------------------------------------------------------------
    if last_offset > 0 and last_offset <= current_size:
        print(f"[*] Incremental read: {(current_size - last_offset) / 1e6:.1f} MB new data")
        with open(source_file, 'rb') as f:
            header = f.readline()
            if last_offset <= len(header):
                new_bytes = f.read()
            else:
                f.seek(last_offset)
                new_bytes = f.read()

        if not new_bytes.strip():
            print("[*] No new data since last run.")
            _save_offset(current_size)
            return

        buf = io.BytesIO(header + new_bytes)
        df = pl.read_csv(buf, schema_overrides=schema_overrides)
        df = df.with_columns(
            pl.from_epoch(pl.col('timestamp'), time_unit='s').alias('timestamp')
        )
        print(f"[+] Loaded {len(df):,} new rows")

        markets_long = _load_markets_long()
        new_df = get_processed_df(df, markets_long)
        print(f"[~] Writing {len(new_df):,} processed rows...")

        file_exists = os.path.isfile(op_file)
        with open(op_file, mode="ab") as f:
            new_df.write_csv(f, include_header=not file_exists)

    # ------------------------------------------------------------------
    # PATH 2 — Bootstrap: output file exists but no offset file.
    # Chunked scan to avoid loading 37 GB into memory at once.
    # ------------------------------------------------------------------
    elif last_offset == 0 and os.path.exists(op_file) and os.path.getsize(op_file) > 0:
        print(f"[!] No offset file — bootstrapping via chunked scan ({BOOTSTRAP_CHUNK_ROWS:,} rows/chunk)")

        result = subprocess.run(['tail', '-n', '1', op_file], capture_output=True, text=True)
        last_line = result.stdout.strip().split(',')
        last_ts   = pd.to_datetime(last_line[0])
        last_epoch = int(last_ts.timestamp())
        anchor = {
            'timestamp':       last_ts,
            'transactionHash': last_line[-1],
            'maker':           last_line[2],
            'taker':           last_line[3],
        }
        print(f"[>] Resuming from: {last_ts}")

        markets_long = _load_markets_long()
        total = _process_chunked(
            source_file, op_file, schema_overrides, markets_long,
            skip_before_epoch=last_epoch,
            anchor=anchor,
        )
        print(f"[*] Bootstrap complete — {total:,} new rows written")

    # ------------------------------------------------------------------
    # PATH 3 — First-ever run: no output file at all.
    # Also chunked to stay within RAM.
    # ------------------------------------------------------------------
    else:
        print(f"[!] No existing output — full chunked scan ({BOOTSTRAP_CHUNK_ROWS:,} rows/chunk)")
        markets_long = _load_markets_long()
        total = _process_chunked(
            source_file, op_file, schema_overrides, markets_long,
        )
        print(f"[*] Initial processing complete — {total:,} rows written")

    # Save offset so next run is O(new bytes only)
    _save_offset(current_size)

    print("=" * 60)
    print("[OK] Processing complete!")
    print("=" * 60)


if __name__ == "__main__":
    process_live()
