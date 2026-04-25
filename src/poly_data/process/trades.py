from __future__ import annotations

import logging

import polars as pl

from poly_data.io.parquet_store import ParquetStore

logger = logging.getLogger(__name__)


def _list_partitions(store: ParquetStore, source: str) -> list[tuple[int, int]]:
    base = store.root / source
    if not base.is_dir():
        return []
    out: set[tuple[int, int]] = set()
    for year_dir in base.glob("year=*"):
        try:
            year = int(year_dir.name.split("=", 1)[1])
        except ValueError:
            continue
        for month_dir in year_dir.glob("month=*"):
            try:
                month = int(month_dir.name.split("=", 1)[1])
            except ValueError:
                continue
            if any(month_dir.glob("*.parquet")):
                out.add((year, month))
    return sorted(out)


def _transform(orders_lf: pl.LazyFrame, markets_lf: pl.LazyFrame) -> pl.LazyFrame:
    markets_long = (
        markets_lf.rename({"id": "market_id"})
        .select(["market_id", "token1", "token2"])
        .unpivot(
            index="market_id",
            on=["token1", "token2"],
            variable_name="side",
            value_name="asset_id",
        )
    )

    df = orders_lf.with_columns(
        pl.when(pl.col("makerAssetId") != "0")
        .then(pl.col("makerAssetId"))
        .otherwise(pl.col("takerAssetId"))
        .alias("nonusdc_asset_id")
    )

    df = df.join(markets_long, left_on="nonusdc_asset_id", right_on="asset_id",
                 how="left")

    df = df.with_columns([
        pl.when(pl.col("makerAssetId") == "0")
        .then(pl.lit("USDC")).otherwise(pl.col("side"))
        .alias("makerAsset"),
        pl.when(pl.col("takerAssetId") == "0")
        .then(pl.lit("USDC")).otherwise(pl.col("side"))
        .alias("takerAsset"),
    ])

    df = df.with_columns([
        (pl.col("makerAmountFilled").cast(pl.Float64) / 10**6)
        .alias("makerAmountFilled"),
        (pl.col("takerAmountFilled").cast(pl.Float64) / 10**6)
        .alias("takerAmountFilled"),
    ])

    df = df.with_columns([
        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.lit("BUY")).otherwise(pl.lit("SELL"))
        .alias("taker_direction"),
        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.lit("SELL")).otherwise(pl.lit("BUY"))
        .alias("maker_direction"),
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
        .alias("price"),
    ])

    return df.select([
        "timestamp", "market_id", "maker", "taker", "nonusdc_side",
        "maker_direction", "taker_direction", "price", "usd_amount",
        "token_amount", "transactionHash", "id",
    ])


def process_trades(store: ParquetStore) -> int:
    cursor = store.last_cursor("trades") or {}
    cur_year = cursor.get("year")
    cur_month = cursor.get("month")
    last_id = cursor.get("last_id")

    markets_lf = store.scan("markets")
    if markets_lf.collect().height == 0:
        logger.warning("No markets in store — process_trades is a no-op")
        return 0

    total = 0
    for (year, month) in _list_partitions(store, "orderFilled"):
        if cur_year is not None and (year, month) < (cur_year, cur_month):
            continue

        orders_lf = store.scan("orderFilled", year=year, month=month)
        if cur_year is not None and (year, month) == (cur_year, cur_month) \
                and last_id is not None:
            orders_lf = orders_lf.filter(pl.col("id") > last_id)

        df = _transform(orders_lf, markets_lf).collect()
        if df.height == 0:
            continue

        store.append("trades", df.drop("id"))
        max_id = df["id"].max()
        store.save_cursor("trades", {
            "year": year, "month": month, "last_id": max_id,
        })
        cur_year, cur_month, last_id = year, month, max_id
        total += df.height

    return total
