from __future__ import annotations

import logging
import threading
import time
from typing import Any, TypedDict

import polars as pl
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

from poly_data.io.parquet_store import ParquetStore

logger = logging.getLogger(__name__)


class GoldskyEvent(TypedDict, total=False):
    id: str
    timestamp: str
    maker: str
    makerAssetId: str
    makerAmountFilled: str
    taker: str
    takerAssetId: str
    takerAmountFilled: str
    transactionHash: str


_DEFAULT_FIELDS = (
    "id", "timestamp", "maker", "makerAssetId", "makerAmountFilled",
    "taker", "takerAssetId", "takerAmountFilled", "transactionHash",
)


class GoldskyScraper:
    """Generic Goldsky event scraper.

    Sticky-cursor logic lives here once. One scraper per event type."""

    def __init__(
        self,
        event_type: str,
        store: ParquetStore,
        project_url: str,
        *,
        fields: tuple[str, ...] = _DEFAULT_FIELDS,
        batch_size: int = 1000,
        sticky_threshold: int = 100,
    ) -> None:
        self.event_type = event_type
        self.store = store
        self.project_url = project_url
        self.fields = fields
        self.batch_size = batch_size
        self.sticky_threshold = sticky_threshold

        transport = RequestsHTTPTransport(url=project_url, verify=True, retries=3)
        self._client = Client(transport=transport)

    def fetch_range(
        self,
        start_ts: int,
        end_ts: int,
        *,
        worker_id: int = 0,
        shutdown_event: threading.Event | None = None,
    ) -> int:
        if shutdown_event is not None and shutdown_event.is_set():
            return 0

        last_ts = start_ts
        last_id: str | None = None
        sticky_ts: int | None = None
        total = 0

        while True:
            if shutdown_event is not None and shutdown_event.is_set():
                logger.info("[w%d] shutdown — stopping fetch_range", worker_id)
                break

            if sticky_ts is not None:
                where = f'timestamp: "{sticky_ts}", id_gt: "{last_id}"'
            else:
                where = f'timestamp_gt: "{last_ts}", timestamp_lte: "{end_ts}"'

            events = self._query(where)
            if not events:
                if sticky_ts is not None:
                    last_ts = sticky_ts
                    sticky_ts = None
                    last_id = None
                    continue
                break

            events.sort(key=lambda e: (int(e["timestamp"]), e["id"]))
            n = len(events)
            first_ts = int(events[0]["timestamp"])
            last_batch_ts = int(events[-1]["timestamp"])

            self._persist(events)
            total += n

            if n >= self.batch_size:
                if first_ts == last_batch_ts:
                    sticky_ts = last_batch_ts
                    last_id = events[-1]["id"]
                else:
                    boundary_count = sum(
                        1 for e in events if int(e["timestamp"]) == last_batch_ts
                    )
                    if boundary_count >= self.sticky_threshold:
                        sticky_ts = last_batch_ts
                        last_id = events[-1]["id"]
                    else:
                        safe_ts = first_ts
                        for e in events:
                            t = int(e["timestamp"])
                            if t < last_batch_ts:
                                safe_ts = t
                        last_ts = safe_ts
                        sticky_ts = None
                        last_id = None
            else:
                if sticky_ts is not None:
                    last_ts = sticky_ts
                    sticky_ts = None
                    last_id = None
                else:
                    last_ts = last_batch_ts
                    if last_batch_ts >= end_ts:
                        break

        return total

    def fetch_incremental(self) -> int:
        cursor = self.store.last_cursor(self.event_type) or {}
        last_ts = int(cursor.get("last_timestamp", 0))
        end_ts = int(time.time())
        if last_ts >= end_ts:
            return 0
        n = self.fetch_range(last_ts, end_ts)
        self.store.save_cursor(
            self.event_type,
            {"last_timestamp": end_ts, "last_id": None, "sticky_ts": None},
        )
        return n

    def _query(self, where_clause: str) -> list[GoldskyEvent]:
        selection = "\n            ".join(self.fields)
        q = gql(f"""query Q {{
            {self.event_type}Events(
                orderBy: timestamp, orderDirection: asc,
                first: {self.batch_size},
                where: {{{where_clause}}}
            ) {{
                {selection}
            }}
        }}""")
        try:
            res = self._client.execute(q)
        except Exception as e:
            logger.warning("goldsky query failed: %s", e)
            time.sleep(5)
            return []
        return list(res.get(f"{self.event_type}Events", []))

    def _persist(self, events: list[GoldskyEvent]) -> None:
        rows = []
        for e in events:
            rows.append({
                "id": str(e["id"]),
                "timestamp": int(e["timestamp"]),
                "maker": str(e.get("maker", "")),
                "makerAssetId": str(e.get("makerAssetId", "")),
                "makerAmountFilled": str(e.get("makerAmountFilled", "")),
                "taker": str(e.get("taker", "")),
                "takerAssetId": str(e.get("takerAssetId", "")),
                "takerAmountFilled": str(e.get("takerAmountFilled", "")),
                "transactionHash": str(e.get("transactionHash", "")),
            })
        if rows:
            self.store.append(self.event_type, pl.DataFrame(rows))
