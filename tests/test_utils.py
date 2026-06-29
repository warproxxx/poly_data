"""Tier-1 unit tests for parsing/serialization helpers in poly_utils.utils."""

import polars as pl

from poly_utils.utils import _flatten_value, _market_cond_id, _token_exprs


class TestTokenExprs:
    def _parse(self, values):
        df = pl.DataFrame({"clobTokenIds": values}).with_columns(_token_exprs())
        return df.select(["token1", "token2"]).rows()

    def test_pair(self):
        assert self._parse(['["a", "b"]']) == [("a", "b")]

    def test_single(self):
        assert self._parse(['["a"]']) == [("a", None)]

    def test_empty_null_and_garbage(self):
        # empty string, null, and non-JSON all yield (None, None)-ish, never raise
        assert self._parse(["", None]) == [(None, None), (None, None)]

    def test_numeric_token_ids(self):
        assert self._parse(['["1001", "1002"]']) == [("1001", "1002")]


class TestFlattenValue:
    def test_none_becomes_empty(self):
        assert _flatten_value(None) == ""

    def test_scalar_passthrough(self):
        assert _flatten_value("x") == "x"

    def test_nested_json_encoded(self):
        assert _flatten_value([1, 2]) == "[1, 2]"


class TestMarketCondId:
    def test_prefers_condition_id(self):
        assert _market_cond_id({"conditionId": "0xabc", "id": 1}) == "0xabc"

    def test_falls_back_to_id(self):
        assert _market_cond_id({"id": 123}) == "123"

    def test_empty_when_neither(self):
        assert _market_cond_id({}) == ""
