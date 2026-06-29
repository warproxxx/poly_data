"""Tier-1 unit tests for the CLOB market row-mapping in update_markets."""

import base64

from update_utils.update_markets import (
    _cursor,
    _flatten,
    _read_tail_ids,
    _row,
    _token_ids,
)


class TestReadTailIds:
    def test_last_ids(self, tmp_path):
        p = tmp_path / "m.csv"
        p.write_text("id,clobTokenIds\nc1,x\nc2,y\nc3,z\n")
        assert _read_tail_ids(str(p), 2) == {"c2", "c3"}

    def test_excludes_header(self, tmp_path):
        p = tmp_path / "m.csv"
        p.write_text("id,clobTokenIds\nc1,x\n")
        assert _read_tail_ids(str(p), 10) == {"c1"}

    def test_missing_file(self, tmp_path):
        assert _read_tail_ids(str(tmp_path / "nope.csv"), 10) == set()


class TestCursor:
    def test_zero(self):
        assert _cursor(0) == base64.b64encode(b"0").decode()

    def test_offset(self):
        assert _cursor(1000) == "MTAwMA=="


class TestTokenIds:
    def test_pair(self):
        m = {"tokens": [{"token_id": "111"}, {"token_id": 222}]}
        assert _token_ids(m) == ["111", "222"]

    def test_missing_or_empty(self):
        assert _token_ids({}) == []
        assert _token_ids({"tokens": None}) == []
        assert _token_ids({"tokens": [{"outcome": "Yes"}]}) == []


class TestFlatten:
    def test_none_becomes_empty(self):
        assert _flatten(None) == ""

    def test_scalars_passthrough(self):
        assert _flatten("x") == "x"
        assert _flatten(5) == 5

    def test_nested_json_encoded(self):
        assert _flatten(["a", "b"]) == '["a", "b"]'
        assert _flatten({"k": 1}) == '{"k": 1}'


class TestRow:
    def test_maps_condition_id_and_tokens(self):
        m = {
            "condition_id": "0xabc",
            "tokens": [{"token_id": "1"}, {"token_id": "2"}],
            "question": "Q?",
            "closed": True,
        }
        row = _row(m, ["id", "clobTokenIds", "question", "closed"])
        assert row == ["0xabc", '["1", "2"]', "Q?", True]

    def test_missing_tokens_blank(self):
        row = _row({"condition_id": "0xdef"}, ["id", "clobTokenIds"])
        assert row == ["0xdef", ""]
