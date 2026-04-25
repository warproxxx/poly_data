from __future__ import annotations

from pathlib import Path

import polars as pl

from poly_data.io.parquet_store import ParquetStore
from poly_data.ingest.parallel import run_segments


class _StubScraper:
    """Calls fetch_range with the assigned segment, writing N synthetic rows."""

    def __init__(self, store: ParquetStore, rows_per_segment: int = 3) -> None:
        self.store = store
        self.rows_per_segment = rows_per_segment
        self.calls: list[tuple[int, int, int]] = []

    def fetch_range(self, start_ts: int, end_ts: int, *, worker_id: int,
                    shutdown_event=None) -> int:
        self.calls.append((worker_id, start_ts, end_ts))
        rows = [
            {
                "id": f"w{worker_id}-r{i}",
                "timestamp": start_ts + 1 + i,
                "maker": "0x0", "makerAssetId": "0", "makerAmountFilled": "1",
                "taker": "0x0", "takerAssetId": "1", "takerAmountFilled": "1",
                "transactionHash": "0xh",
            }
            for i in range(self.rows_per_segment)
        ]
        self.store.append("orderFilled", pl.DataFrame(rows))
        return self.rows_per_segment


def test_run_segments_partitions_range_evenly(tmp_path: Path) -> None:
    store = ParquetStore(tmp_path / "data")
    scraper = _StubScraper(store)

    total = run_segments(scraper, start_ts=0, end_ts=300, workers=3)
    assert total == 9

    starts_ends = sorted([(s, e) for _, s, e in scraper.calls])
    assert starts_ends == [(0, 100), (100, 200), (200, 300)]


def test_run_segments_skips_when_already_up_to_date(tmp_path: Path) -> None:
    store = ParquetStore(tmp_path / "data")
    scraper = _StubScraper(store)

    n = run_segments(scraper, start_ts=500, end_ts=500, workers=3)
    assert n == 0
    assert scraper.calls == []
