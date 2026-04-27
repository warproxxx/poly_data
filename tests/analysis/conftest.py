from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
import pytest

_BASE = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp())


def _row(ts_offset, market, maker, taker, side, m_dir, t_dir, price, usd, tok, h):
    return (_BASE + ts_offset, market, maker, taker, side,
            m_dir, t_dir, price, usd, tok, h)


@pytest.fixture
def trades_df() -> pl.DataFrame:
    """Synthetic 20-row fills DataFrame mirroring data/trades schema."""
    rows = [
        # M1 — token1 wins (last fill 0.99)
        _row(0,    "M1", "P1", "P2", "token1", "BUY",  "SELL", 0.50, 50.0, 100.0, "h1"),
        _row(10,   "M1", "P3", "P1", "token1", "BUY",  "SELL", 0.55, 55.0, 100.0, "h2"),
        _row(20,   "M1", "P3", "P2", "token1", "BUY",  "SELL", 0.60, 60.0, 100.0, "h3"),
        _row(30,   "M1", "P1", "P3", "token1", "SELL", "BUY",  0.70, 70.0, 100.0, "h4"),
        _row(40,   "M1", "P4", "P5", "token1", "BUY",  "SELL", 0.80, 80.0, 100.0, "h5"),
        _row(50,   "M1", "P4", "P5", "token1", "BUY",  "SELL", 0.90, 90.0, 100.0, "h6"),
        _row(60,   "M1", "P5", "P4", "token1", "SELL", "BUY",  0.95, 95.0, 100.0, "h7"),
        _row(70,   "M1", "P5", "P1", "token1", "BUY",  "SELL", 0.98, 98.0, 100.0, "h8"),
        _row(80,   "M1", "P3", "P2", "token1", "BUY",  "SELL", 0.99, 99.0, 100.0, "h9"),

        # M2 — token2 wins (last fill 0.99 on token2)
        _row(200,  "M2", "P1", "P2", "token2", "BUY",  "SELL", 0.30, 30.0, 100.0, "h10"),
        _row(210,  "M2", "P2", "P1", "token2", "BUY",  "SELL", 0.40, 40.0, 100.0, "h11"),
        _row(220,  "M2", "P3", "P4", "token1", "BUY",  "SELL", 0.55, 55.0, 100.0, "h12"),
        _row(230,  "M2", "P4", "P3", "token2", "BUY",  "SELL", 0.50, 50.0, 100.0, "h13"),
        _row(240,  "M2", "P5", "P1", "token2", "BUY",  "SELL", 0.70, 70.0, 100.0, "h14"),
        _row(250,  "M2", "P5", "P2", "token2", "BUY",  "SELL", 0.85, 85.0, 100.0, "h15"),
        _row(260,  "M2", "P4", "P3", "token2", "BUY",  "SELL", 0.95, 95.0, 100.0, "h16"),
        _row(270,  "M2", "P5", "P1", "token2", "BUY",  "SELL", 0.99, 99.0, 100.0, "h17"),

        # M3 — still open (last fills 0.55 — neither token reaches 0.98)
        _row(400,  "M3", "P1", "P2", "token1", "BUY",  "SELL", 0.50, 50.0, 100.0, "h18"),
        _row(410,  "M3", "P2", "P3", "token1", "BUY",  "SELL", 0.55, 55.0, 100.0, "h19"),
        _row(420,  "M3", "P3", "P4", "token1", "BUY",  "SELL", 0.55, 55.0, 100.0, "h20"),
    ]
    schema = ["timestamp", "market_id", "maker", "taker", "nonusdc_side",
              "maker_direction", "taker_direction", "price", "usd_amount",
              "token_amount", "transactionHash"]
    return pl.DataFrame(rows, schema=schema, orient="row").with_columns([
        pl.col("timestamp").cast(pl.Int64),
    ])


@pytest.fixture
def trades_lf(trades_df: pl.DataFrame) -> pl.LazyFrame:
    return trades_df.lazy()


@pytest.fixture
def markets_df() -> pl.DataFrame:
    """Synthetic markets table mirroring data/markets schema."""
    return pl.DataFrame([
        {
            "id": "M1", "token1": "t1a", "token2": "t1b",
            "question": "Will A happen?", "ticker": "a-2024",
            "category": "Sports", "closedTime": "2024-01-01T00:02:00Z",
            "createdAt": "2024-01-01T00:00:00Z", "answer1": "Yes",
            "answer2": "No", "neg_risk": False, "market_slug": "a",
            "condition_id": "c1", "volume": "1000",
            "timestamp": _BASE,
        },
        {
            "id": "M2", "token1": "t2a", "token2": "t2b",
            "question": "Will B happen?", "ticker": "b-2024",
            "category": "Sports", "closedTime": "2024-01-01T00:05:00Z",
            "createdAt": "2024-01-01T00:00:00Z", "answer1": "Yes",
            "answer2": "No", "neg_risk": False, "market_slug": "b",
            "condition_id": "c2", "volume": "2000",
            "timestamp": _BASE,
        },
        {
            "id": "M3", "token1": "t3a", "token2": "t3b",
            "question": "Will C happen?", "ticker": "c-2024",
            "category": "Politics", "closedTime": "",
            "createdAt": "2024-01-01T00:00:00Z", "answer1": "Yes",
            "answer2": "No", "neg_risk": False, "market_slug": "c",
            "condition_id": "c3", "volume": "500",
            "timestamp": _BASE,
        },
    ])
