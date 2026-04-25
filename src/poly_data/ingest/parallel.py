from __future__ import annotations

import logging
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Protocol

logger = logging.getLogger(__name__)


class _ScraperProtocol(Protocol):
    def fetch_range(
        self,
        start_ts: int,
        end_ts: int,
        *,
        worker_id: int,
        shutdown_event: threading.Event | None,
    ) -> int: ...


def run_segments(
    scraper: _ScraperProtocol,
    *,
    start_ts: int,
    end_ts: int,
    workers: int,
    install_signal_handler: bool = False,
) -> int:
    """Split (start_ts, end_ts] across `workers`; run scraper.fetch_range in parallel."""
    if start_ts >= end_ts:
        logger.info("Up to date — nothing to fetch")
        return 0

    shutdown_event = threading.Event()
    if install_signal_handler:
        def _handle(_sig, _frm):
            logger.warning("Shutdown signal — finishing in-flight batches")
            shutdown_event.set()
        signal.signal(signal.SIGINT, _handle)
        signal.signal(signal.SIGTERM, _handle)

    gap = end_ts - start_ts
    seg_size = gap // workers if workers > 0 else gap
    segments: list[tuple[int, int, int]] = []
    for i in range(max(workers, 1)):
        s = start_ts + i * seg_size
        e = start_ts + (i + 1) * seg_size if i < workers - 1 else end_ts
        segments.append((i, s, e))

    t0 = time.time()
    total = 0
    with ThreadPoolExecutor(max_workers=max(workers, 1)) as pool:
        futures = {
            pool.submit(
                scraper.fetch_range,
                s,
                e,
                worker_id=i,
                shutdown_event=shutdown_event,
            ): i
            for i, s, e in segments
        }
        for fut in as_completed(futures):
            wid = futures[fut]
            try:
                count = fut.result()
                total += count
                logger.info("Worker %d done: %d rows", wid, count)
            except Exception as e:
                logger.exception("Worker %d failed: %s", wid, e)

    logger.info("Parallel ingest done in %.1fs — %d rows total", time.time() - t0, total)
    return total
