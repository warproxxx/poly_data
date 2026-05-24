import requests
import csv
import json
import os
import time
from typing import Optional, Set, List


def count_csv_lines(csv_filename: str) -> int:
    """Count the number of data lines in CSV (excluding header)"""
    if not os.path.exists(csv_filename):
        return 0
    try:
        with open(csv_filename, "r", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            next(reader, None)
            return sum(1 for row in reader if row)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return 0


def _load_state(state_file: str) -> dict:
    if os.path.exists(state_file):
        try:
            with open(state_file, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"next_cursor": None, "fetched": 0, "completed": False, "columns": None}


def _save_state(
    state_file: str,
    next_cursor: Optional[str],
    fetched: int,
    columns: Optional[List[str]] = None,
    completed: bool = False,
):
    with open(state_file, "w") as f:
        json.dump(
            {
                "next_cursor": next_cursor,
                "fetched": fetched,
                "completed": completed,
                "columns": columns,
            },
            f,
        )


def _load_seen_ids(csv_file: str, columns: List[str]) -> Set[str]:
    """Load already-written market IDs. Finds 'id' column by position in columns."""
    seen: Set[str] = set()
    if not os.path.exists(csv_file):
        return seen
    try:
        id_idx = columns.index("id")
    except ValueError:
        return seen
    try:
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if row and len(row) > id_idx:
                    seen.add(row[id_idx])
    except Exception as e:
        print(f"  Warning: could not load seen IDs from {csv_file}: {e}")
    return seen


def _flatten_value(v):
    """Convert non-scalar values to JSON strings for CSV storage."""
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return v


def _market_to_row(market: dict, columns: List[str]) -> list:
    """Convert a market dict to a row list aligned with columns."""
    return [_flatten_value(market.get(col)) for col in columns]


def _fetch_markets_keyset(
    params_extra: dict, batch_size: int, csv_file: str, state_file: str
) -> int:
    """
    Fetch markets using the keyset pagination endpoint.
    Writes ALL fields returned by the API into the CSV.
    Column order is determined by the first batch and stored in state
    so that resumed runs stay consistent.
    """
    base_url = "https://gamma-api.polymarket.com/markets/keyset"
    state = _load_state(state_file)
    session = requests.Session()

    # No "completed" gate: subsequent runs re-poll from the saved cursor to pick up
    # markets created since last run. Dedupe via seen_ids handles the overlap page.
    cursor = state["next_cursor"]
    fetched = state["fetched"]
    columns = state.get("columns")
    resuming = cursor is not None

    if resuming:
        print(f"  Resuming from cursor, fetched so far={fetched}")
        seen_ids = _load_seen_ids(csv_file, columns or [])
    else:
        seen_ids: Set[str] = set()

    file_mode = "a" if resuming else "w"

    with open(csv_file, file_mode, newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)

        while True:
            params = {
                "limit": batch_size,
                **params_extra,
            }
            if cursor:
                params["after_cursor"] = cursor

            try:
                response = session.get(base_url, params=params, timeout=30)

                if response.status_code == 500:
                    print("  Server error (500) - retrying in 5 seconds...")
                    time.sleep(5)
                    continue
                elif response.status_code == 429:
                    print("  Rate limited (429) - waiting 10 seconds...")
                    time.sleep(10)
                    continue
                elif response.status_code == 503:
                    print("  Service unavailable (503) - waiting 10 seconds...")
                    time.sleep(10)
                    continue
                elif response.status_code != 200:
                    print(f"  API error {response.status_code}: {response.text}")
                    time.sleep(3)
                    continue

                data = response.json()
                markets = data.get("markets", [])
                next_cursor = data.get("next_cursor")

                if not markets:
                    break

                # Discover columns from the first batch
                if columns is None:
                    columns = list(markets[0].keys())
                    writer.writerow(columns)

                batch_count = 0
                for market in markets:
                    mid = str(market.get("id", ""))
                    if mid in seen_ids:
                        continue
                    seen_ids.add(mid)
                    writer.writerow(_market_to_row(market, columns))
                    batch_count += 1

                fetched += batch_count
                # Preserve the last valid cursor even when the API signals "end of data"
                # (next_cursor=None). Re-polling that cursor on a future run returns
                # any newly created markets past it.
                if next_cursor:
                    cursor = next_cursor
                csvfile.flush()
                _save_state(state_file, cursor, fetched, columns)
                print(f"  Fetched {fetched} markets so far")

                if not next_cursor:
                    break

                time.sleep(0.1)

            except requests.exceptions.RequestException as e:
                print(f"  Network error: {e}, retrying in 5s...")
                time.sleep(5)
                continue

    _save_state(state_file, cursor, fetched, columns)
    return fetched


def _merge_parts(closed_csv: str, active_csv: str, output_csv: str):
    """Merge closed and active part CSVs into the final output file."""
    with open(output_csv, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        for i, part_file in enumerate([closed_csv, active_csv]):
            with open(part_file, "r", encoding="utf-8") as infile:
                reader = csv.reader(infile)
                header = next(reader, None)
                if i == 0 and header:
                    writer.writerow(header)
                for row in reader:
                    if row:
                        writer.writerow(row)


def update_markets(csv_filename: str = "data/markets.csv", batch_size: int = 500):
    """
    Fetch ALL markets (closed + active) using keyset pagination and save to CSV.
    All fields from the API are preserved as-is; nested objects are JSON-encoded.
    """
    os.makedirs(os.path.dirname(csv_filename) or ".", exist_ok=True)
    base = csv_filename.replace(".csv", "")
    closed_csv = f"{base}_closed_part.csv"
    active_csv = f"{base}_active_part.csv"
    closed_state = f"{base}_closed_state.json"
    active_state = f"{base}_active_state.json"

    print("=== Pass 1: Fetching CLOSED markets ===")
    closed_count = _fetch_markets_keyset(
        {"closed": "true"}, batch_size, closed_csv, closed_state
    )
    print(f"Closed markets: {closed_count}")

    print("\n=== Pass 2: Fetching ACTIVE markets ===")
    active_count = _fetch_markets_keyset(
        {"closed": "false"}, batch_size, active_csv, active_state
    )
    print(f"Active markets: {active_count}")

    print(f"\n=== Merging into {csv_filename} ===")
    _merge_parts(closed_csv, active_csv, csv_filename)

    total = closed_count + active_count
    print(f"Total markets: {total}")
    print(f"Data saved to: {csv_filename}")
    # Part files + state files are preserved across runs so subsequent calls
    # can resume incrementally from the saved cursors. markets.csv is
    # regenerated each run by merging the (growing) part files.
