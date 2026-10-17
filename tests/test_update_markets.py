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


class TestUpdateMarketsLoop:
    """Drive update_markets against a fake CLOB to check the wave pipelining."""

    def _fake_clob(self, monkeypatch, tmp_path, total, page=10):
        import update_utils.update_markets as um

        monkeypatch.setattr(um, "PAGE", page)
        monkeypatch.setattr(um, "STATE_FILE", str(tmp_path / "state.json"))
        calls = []

        def fake_fetch(offset):
            calls.append(offset)
            ids = range(offset, min(offset + page, total))
            return offset, [{"condition_id": f"c{i}", "tokens": [{"token_id": i}]} for i in ids]

        monkeypatch.setattr(um, "_fetch_page", fake_fetch)
        return um, calls

    def _ids(self, path):
        import csv

        with open(path, newline="") as f:
            return [r[0] for r in list(csv.reader(f))[1:]]

    def test_fetches_everything_once_in_order(self, monkeypatch, tmp_path):
        um, calls = self._fake_clob(monkeypatch, tmp_path, total=95)
        out = str(tmp_path / "markets.csv")
        assert um.update_markets(out, max_workers=3) == 95
        assert self._ids(out) == [f"c{i}" for i in range(95)]
        assert um._load_state()["completed"] is True

    def test_resume_skips_already_saved(self, monkeypatch, tmp_path):
        um, _ = self._fake_clob(monkeypatch, tmp_path, total=55)
        out = str(tmp_path / "markets.csv")
        um.update_markets(out, max_workers=2)
        # Simulate new markets appearing since the last run.
        um, _ = self._fake_clob(monkeypatch, tmp_path, total=73)
        assert um.update_markets(out, max_workers=2) == 73
        assert self._ids(out) == [f"c{i}" for i in range(73)]
