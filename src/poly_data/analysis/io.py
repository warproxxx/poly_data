from __future__ import annotations

import gc
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import duckdb
import polars as pl
import psutil

from poly_data.analysis._sampling import SAMPLER

DEFAULT_CAP_MB = 2048


@contextmanager
def rss_guard(label: str, *, cap_mb: int = DEFAULT_CAP_MB,
              sample_ms: int | None = None) -> Iterator[None]:
    """Postmortem RSS audit. Raises MemoryError on exit if cap_mb was exceeded above baseline.

    **Important**: this is a *postmortem detector*, not a circuit breaker. A
    shared background sampler polls process RSS every ~50 ms (see
    ``poly_data.analysis._sampling``); if it sees cap_mb breached, it records
    the breach but cannot interrupt the running polars/duckdb query (no
    thread-cancel API in either engine). The ``MemoryError`` raises only after
    the guarded block completes — by which time the OS may already have
    OOM-killed the process. Use it to *detect* excessive allocations after the
    fact, not to *prevent* them.

    For real prevention, push the constraint into the engines themselves:
    ``open_duckdb(memory_limit='2GB')`` for DuckDB, query restructuring (avoid
    non-partitionable group-bys) for Polars.

    The ``sample_ms`` parameter is accepted for backwards compatibility but
    ignored — the shared sampler runs at a fixed cadence.
    """
    del sample_ms  # accepted for back-compat, ignored
    proc = psutil.Process()
    gc.collect()
    baseline = proc.memory_info().rss
    cap_bytes = cap_mb * 1024 * 1024
    breach = {"hit": False, "rss": baseline}

    def listener(rss: int) -> None:
        if rss - baseline > cap_bytes:
            breach["hit"] = True
            breach["rss"] = rss

    listener_id = SAMPLER.register(listener)
    try:
        yield
    finally:
        SAMPLER.deregister(listener_id)
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
