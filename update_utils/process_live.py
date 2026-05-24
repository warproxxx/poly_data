"""
Join raw order-fill events with market metadata to produce labeled trades.

Input:  data/orderFilled.csv  (written by update_chain.py)
Output: processed/trades.csv
"""

import warnings

warnings.filterwarnings("ignore")

import csv as csv_lib
import os
import subprocess
import sys

import pandas as pd
import polars as pl

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from poly_utils.utils import get_markets, update_missing_tokens

ORDERS_CSV = "data/orderFilled.csv"
TRADES_CSV = "processed/trades.csv"


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
    """Scan orderFilled.csv for asset IDs not present in markets and fetch them."""
    maker_ids: set = set()
    taker_ids: set = set()
    with open(ORDERS_CSV, newline="", encoding="utf-8") as f:
        reader = csv_lib.DictReader(f)
        for row in reader:
            if row.get("makerAssetId", "0") != "0":
                maker_ids.add(row["makerAssetId"])
            if row.get("takerAssetId", "0") != "0":
                taker_ids.add(row["takerAssetId"])
    trade_asset_ids = maker_ids | taker_ids

    existing = set()
    for col in ("token1", "token2"):
        if col in markets_df.columns:
            existing.update(markets_df[col].drop_nulls().to_list())

    missing = sorted(trade_asset_ids - existing)
    if missing:
        print(f"🔍 {len(missing)} markets not in markets.csv — fetching from Polymarket API")
        update_missing_tokens(missing)
    else:
        print("✅ All markets present")


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

    df = pl.read_csv(
        ORDERS_CSV,
        schema_overrides={"makerAssetId": pl.Utf8, "takerAssetId": pl.Utf8},
    )
    df = df.with_columns(pl.from_epoch(pl.col("timestamp"), time_unit="s").alias("timestamp"))
    print(f"✓ Loaded {len(df):,} order events")

    df = df.with_row_index()
    if last is None:
        new_orders = df.drop("index")
    else:
        marker = df.filter(
            (pl.col("timestamp") == last["timestamp"])
            & (pl.col("transactionHash") == last["transactionHash"])
            & (pl.col("maker") == last["maker"])
            & (pl.col("taker") == last["taker"])
        )
        if marker.is_empty():
            print("⚠ Last-processed marker not found; reprocessing everything")
            new_orders = df.drop("index")
        else:
            new_orders = df.filter(pl.col("index") > marker.row(0)[0]).drop("index")

    print(f"⚙️  Processing {len(new_orders):,} new orders…")

    # Load markets first so we can both fetch missing tokens and do the join.
    markets_df = get_markets()
    _discover_missing_tokens(markets_df)
    # Reload if we backfilled.
    markets_df = get_markets()

    trades = _processed_df(new_orders, markets_df)

    os.makedirs("processed", exist_ok=True)
    if os.path.isfile(TRADES_CSV):
        print(f"✓ Appending {len(trades):,} rows → {TRADES_CSV}")
        with open(TRADES_CSV, mode="a") as f:
            trades.write_csv(f, include_header=False)
    else:
        trades.write_csv(TRADES_CSV)
        print(f"✓ Created {TRADES_CSV}")

    print("=" * 60)
    print("✅ Done")
    print("=" * 60)


if __name__ == "__main__":
    process_live()
