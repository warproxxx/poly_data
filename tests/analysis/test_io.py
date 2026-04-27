import time
from pathlib import Path

import polars as pl
import pytest

from poly_data.analysis import io as ioh


def test_rss_guard_passes_when_under_cap():
    with ioh.rss_guard("ok", cap_mb=4096, sample_ms=10):
        x = 1 + 1
    assert x == 2


def test_rss_guard_raises_on_breach():
    with pytest.raises(MemoryError):
        with ioh.rss_guard("breach", cap_mb=10, sample_ms=10):
            buf = bytearray(50 * 1024 * 1024)  # 50 MB > 10 MB cap
            time.sleep(0.1)                    # let watcher sample
            del buf


def test_open_duckdb_applies_settings(tmp_path: Path):
    con = ioh.open_duckdb(memory_limit="1GB", threads=2,
                          temp_directory=tmp_path / "ddb_tmp")
    mem_raw = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    threads = con.execute("SELECT current_setting('threads')").fetchone()[0]
    # DuckDB stores ~95% of requested limit; parse numeric MiB value and verify range
    import re
    m = re.search(r"([\d.]+)\s*([MmGg]i?[Bb])", mem_raw)
    assert m, f"Unexpected memory_limit format: {mem_raw!r}"
    value, unit = float(m.group(1)), m.group(2).upper()
    mb = value * 1024 if unit.startswith("G") else value
    assert 800 <= mb <= 1100, f"memory_limit out of expected range: {mem_raw!r}"
    assert int(threads) == 2
    con.close()


def test_scan_trades_returns_lazyframe(tmp_path: Path):
    target = tmp_path / "trades" / "year=2024" / "month=1" / "month.parquet"
    target.parent.mkdir(parents=True)
    pl.DataFrame({"timestamp": [1], "market_id": ["m"], "price": [0.5]})\
      .write_parquet(target)
    lf = ioh.scan_trades(data_root=tmp_path)
    assert isinstance(lf, pl.LazyFrame)
    df = lf.collect()
    assert df.height == 1
