"""One-shot migration of legacy CSVs to the v2 Parquet store."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import polars as pl

from poly_data.io.parquet_store import ParquetStore
from poly_data.logging_setup import configure_logging

logger = logging.getLogger(__name__)


CSV_LOCATIONS = {
    "orderFilled": "goldsky/orderFilled.csv",
    "markets": "markets.csv",
    "missing_markets": "missing_markets.csv",
    "trades": "processed/trades.csv",
}


def _load(path: Path, source: str) -> pl.DataFrame | None:
    if not path.is_file():
        return None
    try:
        df = pl.read_csv(path, ignore_errors=True, infer_schema_length=10_000)
    except Exception as e:
        logger.error("failed to read %s: %s", path, e)
        return None
    if "timestamp" not in df.columns and "createdAt" in df.columns:
        df = df.with_columns(
            pl.col("createdAt").cast(pl.Int64, strict=False).fill_null(0)
            .alias("timestamp")
        )
    if "timestamp" not in df.columns:
        df = df.with_columns(pl.lit(0).cast(pl.Int64).alias("timestamp"))
    df = df.with_columns(pl.col("timestamp").cast(pl.Int64, strict=False).fill_null(0))
    if "id" in df.columns:
        df = df.unique(subset="id", keep="first", maintain_order=False)
    return df


def migrate(*, repo_root: Path, store_root: Path) -> dict[str, int]:
    store = ParquetStore(store_root)
    out: dict[str, int] = {}
    for source, rel in CSV_LOCATIONS.items():
        df = _load(repo_root / rel, source)
        if df is None or df.height == 0:
            out[source] = 0
            continue
        store.append(source, df)
        out[source] = df.height
        logger.info("migrated %s: %d rows", source, df.height)
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="migrate legacy CSVs to Parquet store")
    p.add_argument("--repo-root", default=".", type=Path)
    p.add_argument("--store-root", default="data", type=Path)
    ns = p.parse_args(argv)

    configure_logging()
    counts = migrate(repo_root=ns.repo_root, store_root=ns.store_root)
    for s, n in counts.items():
        logger.info("%s: %d rows", s, n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
