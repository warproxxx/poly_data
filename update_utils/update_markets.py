"""
Fetch the full Polymarket market list from the CLOB API into data/markets.csv.

CLOB's /markets endpoint paginates 1000 rows/page (vs the Gamma keyset's hard
cap of 100) with an offset-based cursor, so the entire ~1.5M-market history —
overwhelmingly closed/resolved markets — can be pulled with concurrent requests
in well under a minute, instead of the ~hour the sequential Gamma keyset took.

Each market is written with the columns process_live expects:

    id            -> CLOB condition_id (stable on-chain market identifier)
    clobTokenIds  -> JSON array of the market's CLOB token_ids (token1, token2)

plus every other field the CLOB market object carries, preserved as-is (nested
values JSON-encoded), mirroring the previous Gamma-based schema.

Resumable: the next offset and discovered column order are saved to
data/markets_state.json so an interrupted run picks up where it left off.
"""

import base64
import csv
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Set

import requests

CLOB_MARKETS = "https://clob.polymarket.com/markets"
PAGE = 1000            # CLOB's fixed page size
WAVE = 20              # concurrent pages per wave (CLOB allows 9000 req / 10s)
END_CURSOR = "LTE="    # base64("-1"): CLOB's end-of-data marker
MAX_RETRIES = 8

MARKETS_CSV = "data/markets.csv"
STATE_FILE = "data/markets_state.json"

_local = threading.local()


def _session() -> requests.Session:
    s = getattr(_local, "s", None)
    if s is None:
        s = requests.Session()
        _local.s = s
    return s


def _cursor(offset: int) -> str:
    return base64.b64encode(str(offset).encode()).decode()


def _token_ids(market: dict) -> List[str]:
    toks = market.get("tokens") or []
    return [str(t.get("token_id")) for t in toks if t.get("token_id") is not None]


def _flatten(v):
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return v


def _row(market: dict, columns: List[str]) -> list:
    ids = _token_ids(market)
    out = []
    for c in columns:
        if c == "id":
            out.append(market.get("condition_id", ""))
        elif c == "clobTokenIds":
            out.append(json.dumps(ids) if ids else "")
        else:
            out.append(_flatten(market.get(c)))
    return out


def _fetch_page(offset: int):
    """Return (offset, markets). Empty list means at/past end of data."""
    s = _session()
    for attempt in range(MAX_RETRIES):
        try:
            r = s.get(CLOB_MARKETS, params={"next_cursor": _cursor(offset)}, timeout=30)
            if r.status_code == 200:
                return offset, r.json().get("data", [])
            if r.status_code in (429, 500, 502, 503):
                time.sleep(min(2 ** attempt, 10))
                continue
            r.raise_for_status()
        except requests.exceptions.RequestException:
            time.sleep(min(2 ** attempt, 10))
    raise RuntimeError(f"CLOB /markets failed at offset {offset} after {MAX_RETRIES} retries")


def _load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_state(offset: int, fetched: int, columns: Optional[List[str]], completed: bool = False):
    with open(STATE_FILE, "w") as f:
        json.dump(
            {"offset": offset, "fetched": fetched, "columns": columns, "completed": completed}, f
        )


def _load_seen_ids(csv_file: str) -> Set[str]:
    seen: Set[str] = set()
    if not os.path.exists(csv_file):
        return seen
    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header or "id" not in header:
            return seen
        idx = header.index("id")
        for row in reader:
            if len(row) > idx:
                seen.add(row[idx])
    return seen


def update_markets(csv_filename: str = MARKETS_CSV, max_workers: int = WAVE) -> int:
    """Fetch all markets from CLOB into csv_filename. Resumable and concurrent."""
    os.makedirs(os.path.dirname(csv_filename) or ".", exist_ok=True)

    state = _load_state()
    columns = state.get("columns")
    resuming = state.get("offset", 0) > 0 and os.path.exists(csv_filename)

    if resuming:
        seen = _load_seen_ids(csv_filename)
        offset = state["offset"]
        fetched = len(seen)
        print(f"  Resuming from offset {offset:,} ({fetched:,} markets already saved)")
        f = open(csv_filename, "a", newline="", encoding="utf-8")
    else:
        seen = set()
        offset = 0
        fetched = 0
        columns = None
        f = open(csv_filename, "w", newline="", encoding="utf-8")

    writer = csv.writer(f)
    done = False
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            while not done:
                offsets = [offset + i * PAGE for i in range(max_workers)]
                for off, markets in ex.map(_fetch_page, offsets):
                    if not markets:
                        done = True
                        continue
                    for m in markets:
                        cid = str(m.get("condition_id", ""))
                        if not cid or cid in seen:
                            continue
                        seen.add(cid)
                        if columns is None:
                            columns = ["id", "clobTokenIds"] + list(m.keys())
                            writer.writerow(columns)
                        writer.writerow(_row(m, columns))
                        fetched += 1
                offset += max_workers * PAGE
                f.flush()
                _save_state(offset, fetched, columns)
                print(f"  fetched {fetched:,} markets (scanned through offset {offset:,})")
    finally:
        f.close()

    _save_state(offset, fetched, columns, completed=True)
    print(f"Total markets: {fetched:,}  ->  {csv_filename}")
    return fetched


if __name__ == "__main__":
    update_markets()
