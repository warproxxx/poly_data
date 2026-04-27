from __future__ import annotations

import json
import logging
import time
from typing import Any

import polars as pl
import requests

from poly_data.io.parquet_store import ParquetStore

logger = logging.getLogger(__name__)

API_URL = "https://gamma-api.polymarket.com/markets"

MARKET_COLUMNS = [
    "createdAt", "id", "question", "answer1", "answer2", "neg_risk",
    "market_slug", "token1", "token2", "condition_id", "volume", "ticker",
    "closedTime", "timestamp", "category",
]


def _existing_row_count(store: ParquetStore, source: str) -> int:
    try:
        return int(store.scan(source).select(pl.len()).collect().item() or 0)
    except Exception:
        return 0


def _parse_market(market: dict[str, Any]) -> dict[str, Any] | None:
    try:
        outcomes_raw = market.get("outcomes", "[]")
        outcomes = (
            json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
        )
        clob_raw = market.get("clobTokenIds", "[]")
        clob = json.loads(clob_raw) if isinstance(clob_raw, str) else clob_raw
        if len(clob) < 2:
            return None
        token1, token2 = str(clob[0]), str(clob[1])
        answer1 = outcomes[0] if outcomes else ""
        answer2 = outcomes[1] if len(outcomes) > 1 else ""
        neg_risk = bool(
            market.get("negRiskAugmented") or market.get("negRiskOther")
        )
        ticker = ""
        category = str(market.get("category", "") or "")
        events = market.get("events") or []
        if events:
            ticker = events[0].get("ticker", "")
            if not category:
                category = str(events[0].get("category", "") or "")

        created_at = market.get("createdAt", "")
        ts_int = _to_unix_seconds(created_at)
        return {
            "createdAt": str(created_at),
            "id": str(market.get("id", "")),
            "question": market.get("question") or market.get("title") or "",
            "answer1": str(answer1),
            "answer2": str(answer2),
            "neg_risk": neg_risk,
            "market_slug": str(market.get("slug", "")),
            "token1": token1,
            "token2": token2,
            "condition_id": str(market.get("conditionId", "")),
            "volume": str(market.get("volume", "")),
            "ticker": str(ticker),
            "closedTime": str(market.get("closedTime", "")),
            "timestamp": ts_int,
            "category": category,
        }
    except (ValueError, KeyError, json.JSONDecodeError, TypeError) as e:
        logger.warning("market parse failed for id=%s: %s",
                       market.get("id", "?"), e)
        return None


def _to_unix_seconds(value: Any) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    if not value:
        return 0
    try:
        from datetime import datetime, timezone
        s = str(value).replace("Z", "+00:00")
        return int(datetime.fromisoformat(s).astimezone(timezone.utc).timestamp())
    except Exception:
        return 0


def update_markets(store: ParquetStore, *, batch_size: int = 500) -> int:
    """Paginate Polymarket markets API; append parsed rows to `markets` source.

    Resumes from existing row count. Total inserted rows returned.
    """
    offset = _existing_row_count(store, "markets")
    if offset:
        logger.info("Resuming markets fetch at offset %d", offset)

    total_inserted = 0
    session = requests.Session()
    while True:
        params = {
            "order": "createdAt",
            "ascending": "true",
            "limit": batch_size,
            "offset": offset,
        }
        resp = session.get(API_URL, params=params, timeout=30)
        if resp.status_code in (429, 500, 502, 503, 504):
            logger.warning("API %s, sleeping 5s", resp.status_code)
            time.sleep(5)
            continue
        resp.raise_for_status()
        markets = resp.json()
        if not markets:
            break

        rows = [r for r in (_parse_market(m) for m in markets) if r is not None]
        if rows:
            store.append("markets", pl.DataFrame(rows))
            total_inserted += len(rows)

        # CRITICAL: advance by API count so offset stays in lockstep with
        # server-side pagination, even if some rows failed to parse.
        offset += len(markets)

        if len(markets) < batch_size:
            break

    return total_inserted


def update_missing_tokens(
    store: ParquetStore,
    missing_token_ids: list[str],
    *,
    inter_request_sleep: float = 0.5,
) -> int:
    """Fetch markets for token IDs not already in the store; append new ones."""
    if not missing_token_ids:
        return 0

    existing_ids: set[str] = set()
    try:
        existing_ids = set(
            store.scan("missing_markets")
            .select("id")
            .collect()["id"]
            .to_list()
        )
    except Exception:
        pass

    session = requests.Session()
    new_rows: list[dict] = []
    for token_id in missing_token_ids:
        for attempt in range(3):
            try:
                resp = session.get(
                    API_URL, params={"clob_token_ids": token_id}, timeout=30
                )
                if resp.status_code == 429:
                    time.sleep(10)
                    continue
                if resp.status_code != 200:
                    time.sleep(2)
                    continue
                payload = resp.json()
                if not payload:
                    break
                row = _parse_market(payload[0])
                if row is None:
                    break
                if row["id"] in existing_ids:
                    break
                existing_ids.add(row["id"])
                new_rows.append(row)
                break
            except requests.RequestException:
                time.sleep(2)
        time.sleep(inter_request_sleep)

    if new_rows:
        store.append("missing_markets", pl.DataFrame(new_rows))
    return len(new_rows)
