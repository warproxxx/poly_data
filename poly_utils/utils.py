"""Helpers for loading markets and backfilling tokens that aren't in markets.csv."""

import csv
import json
import os
from typing import Iterable

import polars as pl
import requests

GAMMA_MARKETS = "https://gamma-api.polymarket.com/markets"


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


def update_missing_tokens(missing_ids: Iterable[str]) -> None:
    """
    Fetch markets for asset IDs that appear in trades but aren't in markets.csv.
    Appends results to missing_markets.csv using the same wide schema as
    markets.csv (all API fields preserved, nested values JSON-encoded).
    """
    missing_ids = [m for m in missing_ids if m and m != "0"]
    if not missing_ids:
        return

    out = MISSING_MARKETS_CSV
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)

    existing_ids: set = set()
    existing_columns = None
    if os.path.exists(out):
        with open(out, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header:
                existing_columns = header
                if "id" in header:
                    idx = header.index("id")
                    for row in reader:
                        if row and len(row) > idx:
                            existing_ids.add(row[idx])

    fetched: list = []
    session = requests.Session()
    for token_id in missing_ids:
        # Gamma's default is closed=false. Most missed tokens are recently-closed
        # short-duration markets (e.g. 15-min XRP/BTC price markets), so try
        # closed=true first, then fall back to active.
        markets: list = []
        try:
            for closed_flag in ("true", "false"):
                resp = session.get(
                    GAMMA_MARKETS,
                    params={
                        "clob_token_ids": token_id,
                        "closed": closed_flag,
                        "limit": 1,
                    },
                    timeout=15,
                )
                if resp.status_code != 200:
                    continue
                payload = resp.json()
                markets = (
                    payload
                    if isinstance(payload, list)
                    else payload.get("markets") or payload.get("data") or []
                )
                if markets:
                    break
            for m in markets:
                mid = str(m.get("id", ""))
                if mid and mid not in existing_ids:
                    existing_ids.add(mid)
                    fetched.append(m)
        except Exception as e:
            print(f"  ! failed to fetch market for token {token_id}: {e}")

    if not fetched:
        return

    columns = existing_columns or list(fetched[0].keys())
    write_header = not os.path.exists(out)
    with open(out, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(columns)
        for m in fetched:
            writer.writerow([_flatten_value(m.get(c)) for c in columns])

    print(f"  Fetched {len(fetched)} missing markets → {out}")
