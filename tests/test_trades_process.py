from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from poly_data.io.parquet_store import ParquetStore
from poly_data.process.trades import process_trades


def _ts(year: int, month: int, day: int = 1) -> int:
    return int(datetime(year, month, day, tzinfo=timezone.utc).timestamp())


def _seed(store: ParquetStore) -> None:
    markets = pl.DataFrame([
        {
            "id": "M1", "createdAt": "2024-01-01", "question": "?",
            "answer1": "Y", "answer2": "N", "neg_risk": False,
            "market_slug": "s", "token1": "111", "token2": "222",
            "condition_id": "c", "volume": "0", "ticker": "T",
            "closedTime": "", "timestamp": _ts(2024, 1),
        }
    ])
    store.append("markets", markets)

    orders = pl.DataFrame([
        {
            "id": "o1", "timestamp": _ts(2024, 1, 5),
            "maker": "0xaa", "makerAssetId": "111",
            "makerAmountFilled": "10000000",
            "taker": "0xbb", "takerAssetId": "0",
            "takerAmountFilled": "5000000",
            "transactionHash": "0xt1",
        },
        {
            "id": "o2", "timestamp": _ts(2024, 2, 5),
            "maker": "0xcc", "makerAssetId": "0",
            "makerAmountFilled": "3000000",
            "taker": "0xdd", "takerAssetId": "222",
            "takerAmountFilled": "6000000",
            "transactionHash": "0xt2",
        },
    ])
    store.append("orderFilled", orders)


def test_process_trades_writes_partitioned_trades(tmp_path: Path) -> None:
    store = ParquetStore(tmp_path / "data")
    _seed(store)

    n = process_trades(store)
    assert n == 2

    df = store.scan("trades").collect().sort("timestamp")
    assert df["market_id"].to_list() == ["M1", "M1"]
    assert df["taker_direction"].to_list() == ["BUY", "SELL"]
    assert df["maker_direction"].to_list() == ["SELL", "BUY"]
    assert df["price"].to_list() == [pytest.approx(0.5), pytest.approx(0.5)]


def test_process_trades_resumes_from_cursor(tmp_path: Path) -> None:
    store = ParquetStore(tmp_path / "data")
    _seed(store)
    process_trades(store)
    n = process_trades(store)
    assert n == 0
