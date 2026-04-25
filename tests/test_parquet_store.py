from __future__ import annotations

import time
from pathlib import Path

import polars as pl
import pytest

from poly_data.io.parquet_store import ParquetStore


def test_append_creates_partitioned_file(store_root: Path,
                                         sample_orderfilled_df: pl.DataFrame) -> None:
    store = ParquetStore(store_root)
    written = store.append("orderFilled", sample_orderfilled_df)

    assert written.exists()
    parts = list((store_root / "orderFilled").rglob("run-*.parquet"))
    months = {p.parent.name for p in parts}
    assert months == {"month=1", "month=2"}


def test_append_idempotent_writes_distinct_files(store_root: Path,
                                                 sample_orderfilled_df: pl.DataFrame) -> None:
    store = ParquetStore(store_root)
    p1 = store.append("orderFilled", sample_orderfilled_df.head(1))
    time.sleep(0.01)
    p2 = store.append("orderFilled", sample_orderfilled_df.head(1))
    assert p1 != p2
    assert p1.exists() and p2.exists()


def test_scan_reads_back_appended_rows(store_root: Path,
                                       sample_orderfilled_df: pl.DataFrame) -> None:
    store = ParquetStore(store_root)
    store.append("orderFilled", sample_orderfilled_df)

    lf = store.scan("orderFilled")
    out = lf.select("id").collect().sort("id")
    assert out["id"].to_list() == ["a1", "a2", "a3"]


def test_scan_empty_source_returns_empty_lazyframe(store_root: Path) -> None:
    store = ParquetStore(store_root)
    lf = store.scan("orderFilled")
    out = lf.collect()
    assert out.height == 0


def test_scan_partition_filter_prunes(store_root: Path,
                                      sample_orderfilled_df: pl.DataFrame) -> None:
    store = ParquetStore(store_root)
    store.append("orderFilled", sample_orderfilled_df)

    lf = store.scan("orderFilled", year=2024, month=1)
    ids = lf.select("id").collect().sort("id")["id"].to_list()
    assert ids == ["a1", "a2"]


def test_append_requires_timestamp_column(store_root: Path) -> None:
    store = ParquetStore(store_root)
    df = pl.DataFrame({"id": ["x"], "value": [1]})
    with pytest.raises(ValueError, match="timestamp"):
        store.append("orderFilled", df)
