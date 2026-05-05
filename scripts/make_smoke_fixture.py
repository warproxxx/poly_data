"""Build a tiny, self-consistent smoke fixture under ``data_smoke/``.

Picks markets that have trades in a recent ``orderFilled`` month, then writes:
- ``data_smoke/markets/year=YYYY/month=MM/markets.parquet`` (subset of markets)
- ``data_smoke/trades/year=YYYY/month=MM/trades.parquet`` (derived for those markets)

The output is small enough (~10k rows) for smoke tests to run in seconds with no
risk of OOM.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import polars as pl

from poly_data.process.trades import _scan_orderfilled_partition
from poly_data.io.parquet_store import ParquetStore

logger = logging.getLogger(__name__)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--src",  default="data",       help="real data root")
    p.add_argument("--dst",  default="data_smoke", help="smoke fixture root")
    p.add_argument("--year", type=int, default=2025)
    p.add_argument("--months", type=int, nargs="+", default=[7, 8],
                   help="orderFilled months to include")
    p.add_argument("--n-markets", type=int, default=2000,
                   help="max number of overlapping markets to sample")
    args = p.parse_args()

    src = Path(args.src).resolve()
    dst = Path(args.dst).resolve()

    # 1. Find markets with token IDs that appear in any chosen orderFilled month.
    m_lf = pl.scan_parquet(str(src / "markets" / "**" / "*.parquet"))
    m_full = m_lf.collect()

    store = ParquetStore(src)
    of_partitions: dict[int, pl.DataFrame] = {}
    asset_set: set[str] = set()
    for month in args.months:
        of_part = src / "orderFilled" / f"year={args.year}" / f"month={month}"
        if not of_part.is_dir():
            raise SystemExit(f"orderFilled partition not found: {of_part}")
        of_lf = _scan_orderfilled_partition(store, args.year, month)
        of_full = of_lf.collect()
        of_partitions[month] = of_full
        asset_set |= set(of_full["makerAssetId"].to_list())
        asset_set |= set(of_full["takerAssetId"].to_list())
    asset_set.discard("0")

    overlap_mask = (
        pl.col("token1").is_in(asset_set) | pl.col("token2").is_in(asset_set)
    )
    overlap_markets = m_full.filter(overlap_mask)
    print(f"overlapping markets across months: {overlap_markets.height}")
    if overlap_markets.height == 0:
        raise SystemExit("no overlap; pick different months")

    sample = overlap_markets.head(args.n_markets)
    sample_tokens = set(sample["token1"].to_list()) | set(sample["token2"].to_list())
    print(f"sampled markets: {sample.height}; tokens: {len(sample_tokens)}")

    dst.mkdir(parents=True, exist_ok=True)
    out_markets = dst / "markets" / f"year={args.year}" / f"month={args.months[-1]}"
    out_markets.mkdir(parents=True, exist_ok=True)
    sample.write_parquet(out_markets / "markets.parquet")
    print(f"wrote {out_markets / 'markets.parquet'} ({sample.height} rows)")

    total_rows = 0
    for month, of_full in of_partitions.items():
        of_filtered = of_full.filter(
            pl.col("makerAssetId").is_in(sample_tokens)
            | pl.col("takerAssetId").is_in(sample_tokens)
        )
        if of_filtered.height == 0:
            print(f"month={month}: 0 trades touch sampled markets, skipping")
            continue
        out_of = dst / "orderFilled" / f"year={args.year}" / f"month={month}"
        out_of.mkdir(parents=True, exist_ok=True)
        of_filtered.write_parquet(out_of / "orderFilled.parquet")
        print(f"wrote {out_of / 'orderFilled.parquet'} ({of_filtered.height} rows)")
        total_rows += of_filtered.height
    if total_rows == 0:
        raise SystemExit("no trades touch sampled markets across selected months")

    print("\nNext: derive trades for the smoke fixture:")
    print(f"  python -m poly_data.cli process --data-root {dst.as_posix()}")


if __name__ == "__main__":
    main()
