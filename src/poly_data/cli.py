from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from poly_data.compact.monthly import compact_all
from poly_data.distribute.huggingface import push_snapshot
from poly_data.ingest.goldsky import GoldskyScraper
from poly_data.ingest.markets import update_markets
from poly_data.ingest.parallel import run_segments
from poly_data.io.parquet_store import ParquetStore
from poly_data.logging_setup import configure_logging
from poly_data.process.trades import process_trades

logger = logging.getLogger(__name__)

GOLDSKY_URL = (
    "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/"
    "subgraphs/orderbook-subgraph/0.0.1/gn"
)


def _scraper_fetch_incremental(scraper: GoldskyScraper) -> int:
    """Indirection for easier mocking in tests."""
    return scraper.fetch_incremental()


def _build_parser() -> argparse.ArgumentParser:
    # Shared options inherited by every subcommand
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--data-root", default="data",
                        help="root dir for the Parquet store (default: ./data)")
    common.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    p = argparse.ArgumentParser(prog="poly-data", parents=[common])
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("update-markets", parents=[common],
                   help="fetch Polymarket markets")

    g = sub.add_parser("update-goldsky", parents=[common],
                       help="scrape Goldsky events")
    g.add_argument("--event", default="orderFilled")
    g.add_argument("--workers", type=int, default=1)

    sub.add_parser("process", parents=[common],
                   help="derive trades from orderFilled")

    c = sub.add_parser("compact", parents=[common],
                       help="compact month partitions")
    c.add_argument("--source", default=None)

    h = sub.add_parser("push-hf", parents=[common],
                        help="push snapshot to HuggingFace Hub")
    h.add_argument("--repo", required=True)
    h.add_argument("--source", action="append", default=None)

    sub.add_parser("update-all", parents=[common],
                   help="markets + goldsky + process (legacy flow)")

    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    ns = parser.parse_args(argv)
    configure_logging(level=getattr(logging, ns.log_level))

    store = ParquetStore(Path(ns.data_root))

    if ns.cmd == "update-markets":
        n = update_markets(store)
        logger.info("update-markets: %d new rows", n)
        return 0

    if ns.cmd == "update-goldsky":
        scraper = GoldskyScraper(
            event_type=ns.event,
            store=store,
            project_url=GOLDSKY_URL,
        )
        if ns.workers > 1:
            cursor = store.last_cursor(ns.event) or {}
            import time as _t
            start = int(cursor.get("last_timestamp", 0))
            end = int(_t.time())
            n = run_segments(
                scraper, start_ts=start, end_ts=end,
                workers=ns.workers, install_signal_handler=True,
            )
            store.save_cursor(ns.event, {
                "last_timestamp": end, "last_id": None, "sticky_ts": None,
            })
        else:
            n = _scraper_fetch_incremental(scraper)
        logger.info("update-goldsky[%s]: %d new rows", ns.event, n)
        return 0

    if ns.cmd == "process":
        n = process_trades(store)
        logger.info("process: %d new trades", n)
        return 0

    if ns.cmd == "compact":
        sources = [ns.source] if ns.source else \
            ["orderFilled", "markets", "missing_markets", "trades"]
        for s in sources:
            compact_all(store, s)
        return 0

    if ns.cmd == "push-hf":
        url = push_snapshot(store, repo_id=ns.repo, sources=ns.source)
        logger.info("pushed: %s", url)
        return 0

    if ns.cmd == "update-all":
        update_markets(store)
        scraper = GoldskyScraper(
            event_type="orderFilled", store=store, project_url=GOLDSKY_URL
        )
        _scraper_fetch_incremental(scraper)
        process_trades(store)
        return 0

    parser.error(f"unknown command: {ns.cmd}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
