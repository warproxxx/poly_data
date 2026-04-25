from __future__ import annotations

from pathlib import Path

from poly_data.io import cursor


def test_load_returns_none_when_missing(tmp_path: Path) -> None:
    assert cursor.load(tmp_path / "absent.json") is None


def test_save_and_load_roundtrip(tmp_path: Path) -> None:
    p = tmp_path / "c.json"
    cursor.save(p, {"last_timestamp": 1700000000, "last_id": "abc"})
    assert cursor.load(p) == {"last_timestamp": 1700000000, "last_id": "abc"}


def test_save_creates_parent_dirs(tmp_path: Path) -> None:
    p = tmp_path / "deep" / "tree" / "c.json"
    cursor.save(p, {"x": 1})
    assert cursor.load(p) == {"x": 1}


def test_load_returns_none_on_corrupt_json(tmp_path: Path) -> None:
    p = tmp_path / "c.json"
    p.write_bytes(b"{not json")
    assert cursor.load(p) is None


def test_save_is_atomic_no_tmp_files(tmp_path: Path) -> None:
    p = tmp_path / "c.json"
    cursor.save(p, {"k": "v"})
    assert list(tmp_path.glob("*.tmp")) == []
