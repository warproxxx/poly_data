"""Stream-decompress orderFilled_complete.csv.xz into the v2 Parquet store.

Reads the legacy goldsky orderFilled CSV directly from its `.xz` archive
(no full decompression to disk) and writes Hive-partitioned Parquet under
``<store>/orderFilled/year=YYYY/month=MM/``.

The legacy CSV has no `id` column, so a synthetic id ``legacy:<row_idx>``
is assigned in stream order to keep rows uniquely identifiable for later
compaction.
"""

from __future__ import annotations

import argparse
import logging
import lzma
import sys
import time
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.csv as pa_csv

from poly_data.logging_setup import configure_logging

logger = logging.getLogger(__name__)

SOURCE = "orderFilled"
FLUSH_ROWS_DEFAULT = 1_000_000
BLOCK_SIZE_DEFAULT = 64 * 1024 * 1024  # 64 MiB pyarrow CSV block


def _flush(
    store_root: Path,
    key: tuple[int, int],
    parts: list[pl.DataFrame],
    epoch_ms: int,
    seq: int,
) -> Path:
    year, month = key
    df = pl.concat(parts, how="vertical_relaxed")
    partition_dir = store_root / SOURCE / f"year={year}" / f"month={month}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    out = partition_dir / f"run-{epoch_ms}-{seq}.parquet"
    df.write_parquet(out, compression="zstd")
    return out


def migrate(
    *,
    xz_path: Path,
    store_root: Path,
    flush_rows: int = FLUSH_ROWS_DEFAULT,
    block_size: int = BLOCK_SIZE_DEFAULT,
    row_limit: int | None = None,
) -> int:
    """Stream `xz_path` into Parquet partitions under `store_root`.

    Returns total rows written.
    """
    store_root.mkdir(parents=True, exist_ok=True)
    epoch_ms = int(time.time() * 1000)
    seq = 0

    buffers: dict[tuple[int, int], list[pl.DataFrame]] = {}
    buffer_rows: dict[tuple[int, int], int] = {}

    total_rows = 0
    last_log = time.time()

    with lzma.open(xz_path, "rb") as raw:
        reader = pa_csv.open_csv(
            raw,
            read_options=pa_csv.ReadOptions(block_size=block_size),
            convert_options=pa_csv.ConvertOptions(
                column_types={
                    "timestamp": pa.int64(),
                    "maker": pa.string(),
                    "makerAssetId": pa.string(),
                    "makerAmountFilled": pa.int64(),
                    "taker": pa.string(),
                    "takerAssetId": pa.string(),
                    "takerAmountFilled": pa.int64(),
                    "transactionHash": pa.string(),
                },
            ),
        )
        for batch in reader:
            df = pl.from_arrow(pa.Table.from_batches([batch]))
            n = df.height
            if row_limit is not None and total_rows + n > row_limit:
                n = row_limit - total_rows
                df = df.head(n)
                if n == 0:
                    break

            df = df.with_columns(
                pl.col("timestamp").cast(pl.Int64, strict=False).fill_null(0),
            )
            df = df.with_columns(
                (
                    pl.lit("legacy:")
                    + pl.int_range(total_rows, total_rows + n, dtype=pl.Int64).cast(pl.Utf8)
                ).alias("id"),
            )
            df = df.with_columns(
                pl.from_epoch(pl.col("timestamp"), time_unit="s").alias("_ts")
            ).with_columns(
                pl.col("_ts").dt.year().alias("_year"),
                pl.col("_ts").dt.month().alias("_month"),
            ).drop("_ts")

            for (y, m), grp in df.group_by(["_year", "_month"]):
                grp = grp.drop(["_year", "_month"])
                key = (int(y), int(m))
                buffers.setdefault(key, []).append(grp)
                buffer_rows[key] = buffer_rows.get(key, 0) + grp.height
                if buffer_rows[key] >= flush_rows:
                    seq += 1
                    out = _flush(store_root, key, buffers.pop(key), epoch_ms, seq)
                    logger.info(
                        "flushed %s -> %s (%d rows)", key, out.name, buffer_rows[key]
                    )
                    buffer_rows[key] = 0

            total_rows += n
            now = time.time()
            if now - last_log > 10:
                logger.info("progress: %d rows ingested", total_rows)
                last_log = now

            if row_limit is not None and total_rows >= row_limit:
                break

    for key in list(buffers.keys()):
        seq += 1
        out = _flush(store_root, key, buffers.pop(key), epoch_ms, seq)
        logger.info(
            "final flush %s -> %s (%d rows)", key, out.name, buffer_rows[key]
        )

    logger.info("migration complete: %d total rows", total_rows)
    return total_rows


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="migrate orderFilled .csv.xz to Parquet")
    p.add_argument(
        "--xz",
        default="orderFilled_complete.csv.xz",
        type=Path,
        help="path to the .csv.xz archive",
    )
    p.add_argument(
        "--store-root",
        default="data",
        type=Path,
        help="parquet store root (will create <root>/orderFilled/year=*/month=*/)",
    )
    p.add_argument("--flush-rows", default=FLUSH_ROWS_DEFAULT, type=int)
    p.add_argument("--block-size", default=BLOCK_SIZE_DEFAULT, type=int)
    p.add_argument(
        "--limit",
        default=None,
        type=int,
        help="stop after N rows (smoke test)",
    )
    ns = p.parse_args(argv)

    configure_logging()
    if not ns.xz.is_file():
        logger.error("xz file not found: %s", ns.xz)
        return 2

    n = migrate(
        xz_path=ns.xz,
        store_root=ns.store_root,
        flush_rows=ns.flush_rows,
        block_size=ns.block_size,
        row_limit=ns.limit,
    )
    print(f"migrated {n} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
