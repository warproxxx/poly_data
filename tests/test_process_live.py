"""Tier-1 unit tests for process_live._processed_df — the core labeling logic.

A regression here (price inversion, direction flip, wrong join) would corrupt
every trade, so we feed tiny inline orders + markets frames and assert the
exact derived price, direction, USD/token amounts, and that unknown markets
stay null instead of leaking a label.
"""

import polars as pl
import pytest

from update_utils.process_live import _processed_df, _read_last_line


class TestReadLastLine:
    def test_multiple_lines(self, tmp_path):
        p = tmp_path / "f.csv"
        p.write_text("a,1\nb,2\nc,3\n")
        assert _read_last_line(str(p)) == "c,3"

    def test_no_trailing_newline(self, tmp_path):
        p = tmp_path / "f.csv"
        p.write_text("a,1\nb,2")
        assert _read_last_line(str(p)) == "b,2"

    def test_single_line(self, tmp_path):
        p = tmp_path / "f.csv"
        p.write_text("only,row\n")
        assert _read_last_line(str(p)) == "only,row"

    def test_empty_file(self, tmp_path):
        p = tmp_path / "f.csv"
        p.write_text("")
        assert _read_last_line(str(p)) == ""

    def test_line_longer_than_chunk(self, tmp_path):
        # last line exceeds the 4096-byte seek block -> must keep walking back
        p = tmp_path / "f.csv"
        last = "x" * 10000
        p.write_text("first\n" + last + "\n")
        assert _read_last_line(str(p)) == last

TOKEN = "1001"
OTHER = "1002"

ORDERS_SCHEMA = {
    "timestamp": pl.Int64,
    "maker": pl.Utf8,
    "makerAssetId": pl.Utf8,
    "makerAmountFilled": pl.Int64,
    "taker": pl.Utf8,
    "takerAssetId": pl.Utf8,
    "takerAmountFilled": pl.Int64,
    "transactionHash": pl.Utf8,
}


def _markets():
    return pl.DataFrame({"id": ["mktA"], "token1": [TOKEN], "token2": [OTHER]})


def _orders(**overrides):
    base = {
        "timestamp": 1, "maker": "0xm", "makerAssetId": "0", "makerAmountFilled": 1,
        "taker": "0xt", "takerAssetId": TOKEN, "takerAmountFilled": 1, "transactionHash": "0xtx",
    }
    base.update(overrides)
    return pl.DataFrame([base], schema=ORDERS_SCHEMA)


def test_maker_buys_token():
    # maker pays 3.4 USDC, receives 5.0 token -> price 0.68
    df = _orders(makerAssetId="0", makerAmountFilled=3_400_000,
                 takerAssetId=TOKEN, takerAmountFilled=5_000_000)
    out = _processed_df(df, _markets()).row(0, named=True)
    assert out["market_id"] == "mktA"
    assert out["maker_direction"] == "BUY"
    assert out["taker_direction"] == "SELL"
    assert out["usd_amount"] == pytest.approx(3.4)
    assert out["token_amount"] == pytest.approx(5.0)
    assert out["price"] == pytest.approx(0.68)
    assert out["nonusdc_side"] == "token1"


def test_maker_sells_token():
    # maker gives 5.0 token, receives 3.4 USDC -> price 0.68, directions flipped
    df = _orders(makerAssetId=TOKEN, makerAmountFilled=5_000_000,
                 takerAssetId="0", takerAmountFilled=3_400_000)
    out = _processed_df(df, _markets()).row(0, named=True)
    assert out["market_id"] == "mktA"
    assert out["maker_direction"] == "SELL"
    assert out["taker_direction"] == "BUY"
    assert out["usd_amount"] == pytest.approx(3.4)
    assert out["token_amount"] == pytest.approx(5.0)
    assert out["price"] == pytest.approx(0.68)


def test_unknown_token_stays_null():
    df = _orders(makerAssetId="0", makerAmountFilled=1_000_000,
                 takerAssetId="9999", takerAmountFilled=2_000_000)
    out = _processed_df(df, _markets()).row(0, named=True)
    assert out["market_id"] is None
    assert out["nonusdc_side"] is None
    # price is pure arithmetic, still computed even when the market is unknown
    assert out["price"] == pytest.approx(0.5)


def test_output_columns():
    out = _processed_df(_orders(), _markets())
    assert out.columns == [
        "timestamp", "market_id", "maker", "taker", "nonusdc_side",
        "maker_direction", "taker_direction", "price", "usd_amount",
        "token_amount", "transactionHash",
    ]
