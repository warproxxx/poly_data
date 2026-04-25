from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest


@pytest.fixture()
def store_root(tmp_path: Path) -> Path:
    """Root dir for an isolated ParquetStore in each test."""
    root = tmp_path / "data"
    root.mkdir()
    return root


@pytest.fixture()
def sample_orderfilled_df() -> pl.DataFrame:
    """A minimal orderFilled DataFrame spanning two months."""
    rows = [
        {"id": "a1", "timestamp": int(datetime(2024, 1, 15, tzinfo=timezone.utc).timestamp()),
         "maker": "0xaa", "makerAssetId": "0", "makerAmountFilled": "1000000",
         "taker": "0xbb", "takerAssetId": "111", "takerAmountFilled": "2000000",
         "transactionHash": "0xt1"},
        {"id": "a2", "timestamp": int(datetime(2024, 1, 20, tzinfo=timezone.utc).timestamp()),
         "maker": "0xcc", "makerAssetId": "222", "makerAmountFilled": "3000000",
         "taker": "0xdd", "takerAssetId": "0", "takerAmountFilled": "1500000",
         "transactionHash": "0xt2"},
        {"id": "a3", "timestamp": int(datetime(2024, 2, 5, tzinfo=timezone.utc).timestamp()),
         "maker": "0xee", "makerAssetId": "0", "makerAmountFilled": "5000000",
         "taker": "0xff", "takerAssetId": "333", "takerAmountFilled": "10000000",
         "transactionHash": "0xt3"},
    ]
    return pl.DataFrame(rows)
