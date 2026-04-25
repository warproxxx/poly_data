from __future__ import annotations

from pathlib import Path

import polars as pl

from poly_data.io.parquet_store import ParquetStore
from poly_data.compact.monthly import compact_all


def test_compact_all_iterates_every_month(tmp_path: Path,
                                          sample_orderfilled_df: pl.DataFrame) -> None:
    store = ParquetStore(tmp_path / "data")
    store.append("orderFilled", sample_orderfilled_df)
    store.append("orderFilled", sample_orderfilled_df)

    rewritten = compact_all(store, "orderFilled")
    assert rewritten == {"2024-1": 2, "2024-2": 1}

    files = list((tmp_path / "data" / "orderFilled").rglob("*.parquet"))
    assert all(f.name == "month.parquet" for f in files)


def test_compact_all_unknown_source_returns_empty(tmp_path: Path) -> None:
    store = ParquetStore(tmp_path / "data")
    assert compact_all(store, "orderFilled") == {}
