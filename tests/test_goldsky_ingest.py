from __future__ import annotations

import threading
from pathlib import Path

import polars as pl
import pytest

from poly_data.io.parquet_store import ParquetStore
from poly_data.ingest.goldsky import GoldskyScraper, GoldskyEvent


def _ev(eid: str, ts: int) -> GoldskyEvent:
    return {
        "id": eid,
        "timestamp": str(ts),
        "maker": "0xaa",
        "makerAssetId": "0",
        "makerAmountFilled": "1000000",
        "taker": "0xbb",
        "takerAssetId": "111",
        "takerAmountFilled": "2000000",
        "transactionHash": "0xt",
    }


def test_fetch_range_writes_events_to_store(tmp_path: Path, mocker) -> None:
    store = ParquetStore(tmp_path / "data")
    scraper = GoldskyScraper(
        event_type="orderFilled",
        store=store,
        project_url="https://example.invalid/gn",
        batch_size=2,
    )

    pages = [
        [_ev("a", 1700000000), _ev("b", 1700000001)],
        [],
    ]
    mocker.patch.object(scraper, "_query", side_effect=pages)

    n = scraper.fetch_range(1699999999, 1700001000)
    assert n == 2

    rows = store.scan("orderFilled").select("id").collect()["id"].to_list()
    assert sorted(rows) == ["a", "b"]


def test_fetch_range_sticky_when_full_batch_same_timestamp(tmp_path: Path,
                                                           mocker) -> None:
    store = ParquetStore(tmp_path / "data")
    scraper = GoldskyScraper(
        event_type="orderFilled",
        store=store,
        project_url="https://example.invalid/gn",
        batch_size=2,
    )

    pages = [
        [_ev("a", 1700000000), _ev("b", 1700000000)],
        [_ev("c", 1700000000)],
        [],
    ]
    mocker.patch.object(scraper, "_query", side_effect=pages)

    n = scraper.fetch_range(1699999999, 1700001000)
    assert n == 3

    rows = store.scan("orderFilled").select("id").collect()["id"].to_list()
    assert sorted(rows) == ["a", "b", "c"]


def test_fetch_range_respects_shutdown_event(tmp_path: Path, mocker) -> None:
    store = ParquetStore(tmp_path / "data")
    scraper = GoldskyScraper(
        event_type="orderFilled",
        store=store,
        project_url="https://example.invalid/gn",
        batch_size=2,
    )
    sh = threading.Event()
    sh.set()

    spy = mocker.patch.object(scraper, "_query", return_value=[])
    n = scraper.fetch_range(0, 9999, shutdown_event=sh)
    assert n == 0
    spy.assert_not_called()
