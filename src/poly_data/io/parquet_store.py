from __future__ import annotations

import os
import time
from pathlib import Path

import polars as pl

from poly_data.io import cursor as _cursor


class ParquetStore:
    """Hive-partitioned Parquet data lake facade.

    Layout: <root>/<source>/year=YYYY/month=MM/{run-<epoch_ms>.parquet | month.parquet}
    """

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    # ----- writing ---------------------------------------------------------

    def append(self, source: str, df: pl.DataFrame) -> Path:
        """Append `df` to <root>/<source>, partitioned by year/month derived from
        the `timestamp` column (UNIX seconds, integer)."""
        if "timestamp" not in df.columns:
            raise ValueError("DataFrame must contain a 'timestamp' column")

        df_part = df.with_columns(
            pl.from_epoch(pl.col("timestamp").cast(pl.Int64), time_unit="s")
            .alias("_ts_dt")
        ).with_columns([
            pl.col("_ts_dt").dt.year().alias("_year"),
            pl.col("_ts_dt").dt.month().alias("_month"),
        ]).drop("_ts_dt")

        epoch_ms = int(time.time() * 1000)
        last_path: Path | None = None
        for (year, month), group in df_part.group_by(["_year", "_month"]):
            group = group.drop(["_year", "_month"])
            partition_dir = (
                self.root / source / f"year={int(year)}" / f"month={int(month)}"
            )
            partition_dir.mkdir(parents=True, exist_ok=True)
            file_path = partition_dir / f"run-{epoch_ms}.parquet"
            i = 0
            while file_path.exists():
                i += 1
                file_path = partition_dir / f"run-{epoch_ms}-{i}.parquet"
            group.write_parquet(file_path, compression="zstd")
            last_path = file_path
        if last_path is None:
            raise ValueError("Empty DataFrame — nothing to append")
        return last_path

    # ----- reading ---------------------------------------------------------

    def scan(
        self,
        source: str,
        year: int | None = None,
        month: int | None = None,
    ) -> pl.LazyFrame:
        """Lazy scan with partition pruning. Returns empty LazyFrame if no data."""
        source_dir = self.root / source
        if not source_dir.is_dir():
            return pl.DataFrame().lazy()

        if year is not None and month is not None:
            target = source_dir / f"year={year}" / f"month={month}"
        elif year is not None:
            target = source_dir / f"year={year}"
        else:
            target = source_dir

        if not target.is_dir():
            return pl.DataFrame().lazy()

        files = list(target.rglob("*.parquet"))
        if not files:
            return pl.DataFrame().lazy()

        return pl.scan_parquet(
            [str(f) for f in files],
            hive_partitioning=True,
        )

    # ----- cursor ----------------------------------------------------------

    def last_cursor(self, source: str) -> dict | None:
        return _cursor.load(self.root / source / "cursor.json")

    def save_cursor(self, source: str, state: dict) -> None:
        (self.root / source).mkdir(parents=True, exist_ok=True)
        _cursor.save(self.root / source / "cursor.json", state)

    # ----- compaction ------------------------------------------------------

    def compact(self, source: str, year: int, month: int) -> int:
        """Rewrite all parquet files in the (year, month) partition into a single
        deduplicated `month.parquet`. Returns rows in compacted file, or 0 if
        nothing to compact (missing partition or single existing month.parquet)."""
        partition_dir = (
            self.root / source / f"year={year}" / f"month={month}"
        )
        if not partition_dir.is_dir():
            return 0
        existing = sorted(partition_dir.glob("*.parquet"))
        if len(existing) <= 1 and (
            len(existing) == 0 or existing[0].name == "month.parquet"
        ):
            return 0

        lf = (
            pl.scan_parquet([str(f) for f in existing])
            .unique(subset="id", keep="first", maintain_order=False)
            .sort("timestamp")
        )
        tmp = partition_dir / "month.parquet.tmp"
        lf.sink_parquet(tmp, compression="zstd")
        final = partition_dir / "month.parquet"
        os.replace(tmp, final)
        for f in existing:
            if f.name not in {"month.parquet", "month.parquet.tmp"}:
                try:
                    f.unlink()
                except FileNotFoundError:
                    pass

        return pl.scan_parquet(final).select(pl.len()).collect().item()
