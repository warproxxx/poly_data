"""Helpers for loading markets and backfilling tokens that aren't in markets.csv."""

import csv
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable

import polars as pl
import requests

GAMMA_MARKETS = "https://gamma-api.polymarket.com/markets"

# Token IDs per Gamma request. The URL holds ~50 of these 77-char ids before the
# server returns 414 (URI too large), so 40 leaves headroom.
_MISSING_BATCH = 40
_MISSING_WORKERS = 12

_local = threading.local()


def _gamma_session() -> requests.Session:
    s = getattr(_local, "s", None)
    if s is None:
        s = requests.Session()
        _local.s = s
    return s


def _split_tokens(s):
    if not s:
        return [None, None]
    try:
        arr = json.loads(s)
        t1 = str(arr[0]) if len(arr) > 0 else None
        t2 = str(arr[1]) if len(arr) > 1 else None
        return [t1, t2]
    except Exception:
        return [None, None]


MARKETS_CSV = "data/markets.csv"
MISSING_MARKETS_CSV = "data/missing_markets.csv"


def get_lean_markets() -> pl.DataFrame:
    """
    Same as `get_markets()` but loads only the three columns the trade processor
    needs (`id`, `token1`, `token2`). Drops the wide schema to keep memory
    bounded during chunked processing — typically ~10× smaller than get_markets().
    """
    frames = []
    for fname in (MARKETS_CSV, MISSING_MARKETS_CSV):
        if os.path.exists(fname):
            df = pl.read_csv(
                fname,
                columns=["id", "clobTokenIds"],
                schema_overrides={"id": pl.Utf8, "clobTokenIds": pl.Utf8},
                ignore_errors=True,
            )
            frames.append(df)
    if not frames:
        raise FileNotFoundError(
            "markets.csv not found — run update_markets() first"
        )
    df = pl.concat(frames, how="diagonal_relaxed").unique(subset=["id"], keep="first")
    tokens = df["clobTokenIds"].map_elements(
        _split_tokens, return_dtype=pl.List(pl.Utf8)
    )
    return df.with_columns(
        [
            tokens.list.get(0).alias("token1"),
            tokens.list.get(1).alias("token2"),
        ]
    ).drop("clobTokenIds")


def get_markets() -> pl.DataFrame:
    """
    Load markets.csv (+ missing_markets.csv if present) and derive token1/token2
    from the JSON-encoded clobTokenIds column written by update_markets().
    """
    frames = []
    for fname in (MARKETS_CSV, MISSING_MARKETS_CSV):
        if os.path.exists(fname):
            # Force id/clobTokenIds to string so concat across files stays consistent.
            df = pl.read_csv(
                fname,
                infer_schema_length=10_000,
                schema_overrides={"id": pl.Utf8, "clobTokenIds": pl.Utf8},
                ignore_errors=True,
            )
            frames.append(df)

    if not frames:
        raise FileNotFoundError(
            "markets.csv not found — run update_markets() first"
        )

    df = pl.concat(frames, how="diagonal_relaxed").unique(subset=["id"], keep="first")

    if "clobTokenIds" not in df.columns:
        raise KeyError(
            "markets.csv is missing the 'clobTokenIds' column — re-run update_markets()"
        )

    tokens = df["clobTokenIds"].map_elements(
        _split_tokens, return_dtype=pl.List(pl.Utf8)
    )
    df = df.with_columns(
        [
            tokens.list.get(0).alias("token1"),
            tokens.list.get(1).alias("token2"),
        ]
    )
    return df


def _flatten_value(v):
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return v


def _market_cond_id(m: dict) -> str:
    """Stable id for a Gamma market — conditionId, to match the CLOB-built
    markets.csv (whose `id` column is the on-chain condition_id)."""
    return str(m.get("conditionId") or m.get("id") or "")


def _fetch_token_batch(token_ids: list) -> list:
    """Fetch markets for a batch of token ids. Tries closed then active (Gamma
    defaults to closed=false, but most missed tokens are recently-closed
    short-duration markets), de-duped by conditionId within the batch."""
    s = _gamma_session()
    found: dict = {}
    for closed_flag in ("true", "false"):
        params = [("clob_token_ids", t) for t in token_ids]
        params += [("closed", closed_flag), ("limit", len(token_ids))]
        try:
            resp = s.get(GAMMA_MARKETS, params=params, timeout=20)
            if resp.status_code != 200:
                continue
            payload = resp.json()
            markets = (
                payload
                if isinstance(payload, list)
                else payload.get("markets") or payload.get("data") or []
            )
            for m in markets:
                cid = _market_cond_id(m)
                if cid:
                    found[cid] = m
        except Exception as e:
            print(f"  ! batch fetch failed ({len(token_ids)} tokens): {e}")
    return list(found.values())


# Columns written to missing_markets.csv. `id` = conditionId so the join key and
# market_id stay consistent with the CLOB-built markets.csv; clobTokenIds carries
# the token pair through for get_lean_markets.
_MISSING_COLUMNS = ["id", "clobTokenIds", "conditionId", "question", "slug", "closed"]


def update_missing_tokens(missing_ids: Iterable[str]) -> None:
    """
    Fetch markets for asset IDs that appear in trades but aren't in markets.csv.

    Batches token ids (~40 per Gamma request) and runs the batches in parallel,
    rather than one request per token, so the run-time fallback is fast. Results
    are appended to missing_markets.csv with `id` = conditionId, matching the
    CLOB-built markets.csv schema.
    """
    missing_ids = [m for m in missing_ids if m and m != "0"]
    if not missing_ids:
        return

    out = MISSING_MARKETS_CSV
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)

    existing_ids: set = set()
    if os.path.exists(out):
        with open(out, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header and "id" in header:
                idx = header.index("id")
                for row in reader:
                    if row and len(row) > idx:
                        existing_ids.add(row[idx])

    batches = [
        missing_ids[i : i + _MISSING_BATCH]
        for i in range(0, len(missing_ids), _MISSING_BATCH)
    ]

    fetched: list = []
    with ThreadPoolExecutor(max_workers=_MISSING_WORKERS) as ex:
        for markets in ex.map(_fetch_token_batch, batches):
            for m in markets:
                cid = _market_cond_id(m)
                if cid and cid not in existing_ids:
                    existing_ids.add(cid)
                    fetched.append(m)

    if not fetched:
        print("  (no markets found for missing tokens)")
        return

    write_header = not os.path.exists(out)
    with open(out, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(_MISSING_COLUMNS)
        for m in fetched:
            cid = _market_cond_id(m)
            writer.writerow(
                [
                    cid,
                    _flatten_value(m.get("clobTokenIds")),
                    cid,
                    _flatten_value(m.get("question")),
                    _flatten_value(m.get("slug")),
                    _flatten_value(m.get("closed")),
                ]
            )

    print(f"  Fetched {len(fetched)} missing markets → {out}")
