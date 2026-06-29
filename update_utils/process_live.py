"""
Join raw order-fill events with market metadata to produce labeled trades.

Input:  data/orderFilled.csv  (written by update_chain.py)
Output: processed/trades.csv
"""

import warnings

warnings.filterwarnings("ignore")

import os
import subprocess
import sys

import pandas as pd
import polars as pl

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from poly_utils.utils import get_lean_markets, update_missing_tokens

ORDERS_CSV = "data/orderFilled.csv"
TRADES_CSV = "processed/trades.csv"

# Chunk size for streaming the orders CSV through the join.
# Default 0 = chunking disabled (load everything in one pass).
# Set e.g. PROCESS_CHUNK_SIZE=500000 to bound memory on large orderFilled.csv files.
CHUNK_SIZE = int(os.environ.get("PROCESS_CHUNK_SIZE", "0"))


def _processed_df(df: pl.DataFrame, markets_df: pl.DataFrame) -> pl.DataFrame:
    markets_df = markets_df.rename({"id": "market_id"})

    markets_long = markets_df.select(["market_id", "token1", "token2"]).melt(
        id_vars="market_id",
        value_vars=["token1", "token2"],
        variable_name="side",
        value_name="asset_id",
    )

    df = df.with_columns(
        pl.when(pl.col("makerAssetId") != "0")
        .then(pl.col("makerAssetId"))
        .otherwise(pl.col("takerAssetId"))
        .alias("nonusdc_asset_id")
    )

    df = df.join(
        markets_long,
        left_on="nonusdc_asset_id",
        right_on="asset_id",
        how="left",
    )

    df = df.with_columns(
        [
            pl.when(pl.col("makerAssetId") == "0")
            .then(pl.lit("USDC"))
            .otherwise(pl.col("side"))
            .alias("makerAsset"),
            pl.when(pl.col("takerAssetId") == "0")
            .then(pl.lit("USDC"))
            .otherwise(pl.col("side"))
            .alias("takerAsset"),
            pl.col("market_id"),
        ]
    )

    df = df[
        [
            "timestamp",
            "market_id",
            "maker",
            "makerAsset",
            "makerAssetId",
            "makerAmountFilled",
            "taker",
            "takerAsset",
            "takerAmountFilled",
            "transactionHash",
        ]
    ]

    # USDC has 6 decimals. Outcome tokens also have 6 (CTF wraps to 6 for parity).
    df = df.with_columns(
        [
            (pl.col("makerAmountFilled") / 10**6).alias("makerAmountFilled"),
            (pl.col("takerAmountFilled") / 10**6).alias("takerAmountFilled"),
        ]
    )

    df = df.with_columns(
        [
            pl.when(pl.col("takerAsset") == "USDC")
            .then(pl.lit("BUY"))
            .otherwise(pl.lit("SELL"))
            .alias("taker_direction"),
            pl.when(pl.col("takerAsset") == "USDC")
            .then(pl.lit("SELL"))
            .otherwise(pl.lit("BUY"))
            .alias("maker_direction"),
        ]
    )

    df = df.with_columns(
        [
            # Derive from the raw assetId (never null) so unknown markets stay null
            # instead of leaking the literal "USDC" through polars three-valued logic.
            pl.when(pl.col("makerAssetId") == "0")
            .then(pl.col("takerAsset"))
            .otherwise(pl.col("makerAsset"))
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
            .alias("price"),
        ]
    )

    return df[
        [
            "timestamp",
            "market_id",
            "maker",
            "taker",
            "nonusdc_side",
            "maker_direction",
            "taker_direction",
            "price",
            "usd_amount",
            "token_amount",
            "transactionHash",
        ]
    ]


def _last_processed_marker():
    if not os.path.exists(TRADES_CSV):
        return None
    result = subprocess.run(["tail", "-n", "1", TRADES_CSV], capture_output=True, text=True)
    last_line = result.stdout.strip()
    if not last_line:
        return None
    parts = last_line.split(",")
    if len(parts) < 4:
        return None
    return {
        "timestamp": pd.to_datetime(parts[0]),
        "transactionHash": parts[-1],
        "maker": parts[2],
        "taker": parts[3],
    }


def _discover_missing_tokens(markets_df: pl.DataFrame) -> None:
    """Scan orderFilled.csv for asset IDs not present in markets and fetch them.

    Uses a lazy polars scan (two columns only) so finding the distinct traded
    tokens across tens of millions of rows takes seconds, not a Python row loop.
    """
    lf = pl.scan_csv(
        ORDERS_CSV,
        schema_overrides={"makerAssetId": pl.Utf8, "takerAssetId": pl.Utf8},
    )
    maker = lf.select("makerAssetId").filter(pl.col("makerAssetId") != "0").unique().collect()
    taker = lf.select("takerAssetId").filter(pl.col("takerAssetId") != "0").unique().collect()
    trade_asset_ids = set(maker["makerAssetId"].to_list()) | set(taker["takerAssetId"].to_list())

    existing = set()
    for col in ("token1", "token2"):
        if col in markets_df.columns:
            existing.update(markets_df[col].drop_nulls().to_list())

    missing = sorted(trade_asset_ids - existing)
    if missing:
        print(f"🔍 {len(missing)} tokens not in markets.csv — fetching (batched) from Gamma")
        update_missing_tokens(missing)
    else:
        print("✅ All markets present")


def _match_marker(chunk: pl.DataFrame, last: dict) -> int | None:
    """Return the row index in `chunk` matching the last-processed marker, or None."""
    mask = chunk.with_row_index().filter(
        (pl.col("timestamp") == last["timestamp"])
        & (pl.col("transactionHash") == last["transactionHash"])
        & (pl.col("maker") == last["maker"])
        & (pl.col("taker") == last["taker"])
    )
    if mask.is_empty():
        return None
    return int(mask.row(0)[0])


def _accumulate(reader, target_rows: int):
    """Pull batches from the polars batched reader until we have at least
    `target_rows` rows, or the reader is exhausted. polars's batch_size is
    an I/O hint, not a row count — without this we get hundreds of tiny
    chunks and pay the join setup cost repeatedly.
    """
    parts: list = []
    rows = 0
    while rows < target_rows:
        batches = reader.next_batches(1)
        if not batches:
            break
        parts.append(batches[0])
        rows += len(batches[0])
    if not parts:
        return None
    return parts[0] if len(parts) == 1 else pl.concat(parts)


def _write_chunk(trades: pl.DataFrame, first_write: bool) -> None:
    if first_write and not os.path.isfile(TRADES_CSV):
        trades.write_csv(TRADES_CSV)
    else:
        with open(TRADES_CSV, mode="a") as f:
            trades.write_csv(f, include_header=False)


def process_live() -> None:
    print("=" * 60)
    print("🔄 Processing trades")
    print("=" * 60)

    if not os.path.exists(ORDERS_CSV):
        print(f"⚠ {ORDERS_CSV} not found — run update_chain() first")
        return

    last = _last_processed_marker()
    if last:
        print(f"📍 Resuming after {last['timestamp']}  ({last['transactionHash'][:16]}…)")
    else:
        print("⚠ No existing trades.csv — processing from beginning")

    # Streaming discovery pass (already low-memory via DictReader). Run first so
    # update_missing_tokens populates missing_markets.csv before we load markets.
    markets_df = get_lean_markets()
    _discover_missing_tokens(markets_df)
    markets_df = get_lean_markets()  # reload if backfilled

    os.makedirs("processed", exist_ok=True)

    overrides = {"makerAssetId": pl.Utf8, "takerAssetId": pl.Utf8}

    # Single reader path for both modes. CHUNK_SIZE=0 means "load everything"
    # (accumulate all batches into one before processing). >0 streams per-chunk.
    # Use a big batch_size as an I/O hint — polars treats it as a ceiling, not target.
    target_per_chunk = CHUNK_SIZE if CHUNK_SIZE > 0 else sys.maxsize
    reader = pl.read_csv_batched(
        ORDERS_CSV,
        schema_overrides=overrides,
        batch_size=CHUNK_SIZE if CHUNK_SIZE > 0 else 100_000,
    )

    if CHUNK_SIZE > 0:
        print(f"⚙️  Streaming in chunks of {CHUNK_SIZE:,} rows")
    else:
        print("⚙️  Loading all order events (chunking disabled)")

    resumed = last is None
    total_written = 0
    first_write = not os.path.isfile(TRADES_CSV)

    while True:
        chunk = _accumulate(reader, target_per_chunk)
        if chunk is None:
            break
        chunk = chunk.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="s").alias("timestamp")
        )
        if not resumed:
            marker_idx = _match_marker(chunk, last)
            if marker_idx is None:
                continue  # marker not in this chunk, skip entirely
            chunk = chunk.slice(marker_idx + 1)
            resumed = True
            if chunk.is_empty():
                continue
        trades_chunk = _processed_df(chunk, markets_df)
        _write_chunk(trades_chunk, first_write=first_write)
        first_write = False
        total_written += len(trades_chunk)
        if CHUNK_SIZE > 0:
            print(f"  +{len(trades_chunk):,} rows  (total: {total_written:,})")

    if not resumed:
        print("⚠ Last-processed marker not found anywhere; nothing written")
    else:
        print(f"✓ Done. Wrote {total_written:,} rows → {TRADES_CSV}")

    print("=" * 60)
    print("✅ Done")
    print("=" * 60)


if __name__ == "__main__":
    process_live()
