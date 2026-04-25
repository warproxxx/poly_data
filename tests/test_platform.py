from __future__ import annotations

from pathlib import Path

import pytest

from poly_data.io.platform import last_line


def test_last_line_returns_none_for_missing_file(tmp_path: Path) -> None:
    assert last_line(tmp_path / "nope.txt") is None


def test_last_line_returns_none_for_empty_file(tmp_path: Path) -> None:
    p = tmp_path / "empty.txt"
    p.write_text("")
    assert last_line(p) is None


def test_last_line_single_line_no_newline(tmp_path: Path) -> None:
    p = tmp_path / "one.txt"
    p.write_text("hello")
    assert last_line(p) == "hello"


def test_last_line_single_line_with_newline(tmp_path: Path) -> None:
    p = tmp_path / "one.txt"
    p.write_text("hello\n")
    assert last_line(p) == "hello"


def test_last_line_lf(tmp_path: Path) -> None:
    p = tmp_path / "lf.txt"
    p.write_bytes(b"a\nb\nc\n")
    assert last_line(p) == "c"


def test_last_line_crlf(tmp_path: Path) -> None:
    p = tmp_path / "crlf.txt"
    p.write_bytes(b"a\r\nb\r\nc\r\n")
    assert last_line(p) == "c"


def test_last_line_no_trailing_newline(tmp_path: Path) -> None:
    p = tmp_path / "raw.txt"
    p.write_bytes(b"a\nb\nc")
    assert last_line(p) == "c"


def test_last_line_utf8_bom(tmp_path: Path) -> None:
    p = tmp_path / "bom.txt"
    p.write_bytes(b"\xef\xbb\xbfheader\nvalue\n")
    assert last_line(p) == "value"


def test_last_line_long_file_beyond_one_chunk(tmp_path: Path) -> None:
    p = tmp_path / "big.txt"
    body = ("row\n" * 5000) + "final"
    p.write_text(body)
    assert last_line(p) == "final"


def test_last_line_skips_trailing_blank_lines(tmp_path: Path) -> None:
    p = tmp_path / "blank.txt"
    p.write_bytes(b"a\nb\n\n\n")
    assert last_line(p) == "b"


from poly_data.io.platform import atomic_write


def test_atomic_write_creates_parent(tmp_path: Path) -> None:
    target = tmp_path / "nested" / "deep" / "file.json"
    atomic_write(target, b'{"x": 1}')
    assert target.read_bytes() == b'{"x": 1}'


def test_atomic_write_overwrites_existing(tmp_path: Path) -> None:
    target = tmp_path / "f.txt"
    target.write_text("old")
    atomic_write(target, "new")
    assert target.read_text() == "new"


def test_atomic_write_no_tmp_left_behind(tmp_path: Path) -> None:
    target = tmp_path / "f.bin"
    atomic_write(target, b"bytes")
    leftovers = list(tmp_path.glob("*.tmp"))
    assert leftovers == []
