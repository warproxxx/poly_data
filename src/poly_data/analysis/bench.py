from __future__ import annotations

import gc
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from typing import Iterator

import polars as pl
import psutil

from poly_data.analysis._sampling import SAMPLER


@dataclass
class BenchResult:
    label: str
    engine: str
    seconds: float
    peak_rss_mb: float
    rows_out: int


class Bench:
    def __init__(self) -> None:
        self.results: list[BenchResult] = []

    @contextmanager
    def __call__(self, label: str, engine: str) -> Iterator[dict]:
        out: dict = {"rows_out": 0}
        proc = psutil.Process()
        gc.collect()
        time.sleep(0.05)
        baseline = proc.memory_info().rss
        # Synchronous pre-sample so queries that finish faster than the shared
        # sampler's tick still record a non-zero peak.
        peak = max(baseline, proc.memory_info().rss)

        def listener(rss: int) -> None:
            nonlocal peak
            if rss > peak:
                peak = rss

        listener_id = SAMPLER.register(listener)
        t0 = time.perf_counter()
        try:
            yield out
        finally:
            elapsed = time.perf_counter() - t0
            SAMPLER.deregister(listener_id)
            # Final synchronous sample — covers the case where the worker
            # finished between the last sampler tick and deregister().
            rss_final = proc.memory_info().rss
            if rss_final > peak:
                peak = rss_final
            self.results.append(BenchResult(
                label=label,
                engine=engine,
                seconds=elapsed,
                peak_rss_mb=(peak - baseline) / (1024 * 1024),
                rows_out=int(out.get("rows_out", 0)),
            ))

    def df(self) -> pl.DataFrame:
        return pl.DataFrame([asdict(r) for r in self.results])
