from __future__ import annotations

import gc
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

import polars as pl
import psutil


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
        peak = baseline
        stop = threading.Event()

        def sampler() -> None:
            nonlocal peak
            while not stop.is_set():
                rss = proc.memory_info().rss
                if rss > peak:
                    peak = rss
                stop.wait(0.05)

        t = threading.Thread(target=sampler, daemon=True)
        t.start()
        t0 = time.perf_counter()
        try:
            yield out
        finally:
            elapsed = time.perf_counter() - t0
            stop.set()
            t.join(timeout=1.0)
            self.results.append(BenchResult(
                label=label,
                engine=engine,
                seconds=elapsed,
                peak_rss_mb=max(0.0, (peak - baseline) / (1024 * 1024)),
                rows_out=int(out.get("rows_out", 0)),
            ))

    def df(self) -> pl.DataFrame:
        return pl.DataFrame([{
            "label": r.label,
            "engine": r.engine,
            "seconds": r.seconds,
            "peak_rss_mb": r.peak_rss_mb,
            "rows_out": r.rows_out,
        } for r in self.results])
