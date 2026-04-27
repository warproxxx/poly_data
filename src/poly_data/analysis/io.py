from __future__ import annotations

import gc
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import duckdb
import polars as pl
import psutil

DEFAULT_CAP_MB = 2048


@contextmanager
def rss_guard(label: str, *, cap_mb: int = DEFAULT_CAP_MB,
              sample_ms: int = 50) -> Iterator[None]:
    """Background-poll process RSS; raise MemoryError on exit if cap_mb exceeded above baseline."""
    proc = psutil.Process()
    gc.collect()
    baseline = proc.memory_info().rss
    breach = {"hit": False, "rss": baseline}
    stop = threading.Event()

    def watcher() -> None:
        while not stop.is_set():
            rss = proc.memory_info().rss
            if rss - baseline > cap_mb * 1024 * 1024:
                breach["hit"] = True
                breach["rss"] = rss
                stop.set()
                return
            stop.wait(sample_ms / 1000.0)

    t = threading.Thread(target=watcher, daemon=True)
    t.start()
    try:
        yield
    finally:
        stop.set()
        t.join(timeout=1.0)
    if breach["hit"]:
        raise MemoryError(
            f"rss_guard[{label}]: cap {cap_mb} MB exceeded; "
            f"rss={breach['rss'] / (1024 * 1024):.0f} MB"
        )


def scan_trades(data_root: Path = Path("data")) -> pl.LazyFrame:
    glob = str(data_root / "trades" / "**" / "*.parquet")
    return pl.scan_parquet(glob)


def scan_markets(data_root: Path = Path("data")) -> pl.LazyFrame:
    glob = str(data_root / "markets" / "**" / "*.parquet")
    return pl.scan_parquet(glob)


def scan_orderfilled(data_root: Path = Path("data")) -> pl.LazyFrame:
    glob = str(data_root / "orderFilled" / "**" / "*.parquet")
    return pl.scan_parquet(glob)


def open_duckdb(*, memory_limit: str = "2GB", threads: int = 4,
                temp_directory: Path = Path("./_duckdb_tmp")
                ) -> duckdb.DuckDBPyConnection:
    temp_directory = Path(temp_directory)
    temp_directory.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute(f"SET threads={threads}")
    con.execute(f"SET temp_directory='{temp_directory.resolve().as_posix()}'")
    return con
