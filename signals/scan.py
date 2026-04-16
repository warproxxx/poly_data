#!/usr/bin/env python3
"""Main scanner — tails orderFilled.csv, detects signals, sends alerts."""

import csv
import io
import json
import os
import subprocess
import sys
import time

SIGNALS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SIGNALS_DIR)
CONFIG_FILE = os.path.join(SIGNALS_DIR, "config.json")
STATE_FILE = os.path.join(SIGNALS_DIR, "state.json")
ORDER_CSV = os.path.join(PROJECT_DIR, "goldsky", "orderFilled.csv")
MARKETS_CSV = os.path.join(PROJECT_DIR, "markets.csv")


def load_config():
    with open(CONFIG_FILE) as f:
        return json.load(f)


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"last_scanned_timestamp": 0}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def load_btc_token_ids(cfg):
    """Load BTC market token IDs from btc_price_markets.csv."""
    path = os.path.join(PROJECT_DIR, cfg.get("btc_markets_file", "btc_price_markets.csv"))
    token_ids = set()
    if not os.path.exists(path):
        print(f"Warning: BTC markets file not found: {path}", file=sys.stderr)
        return token_ids
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            for col in ("token1", "token2"):
                if col in row and row[col]:
                    token_ids.add(row[col])
    return token_ids


def load_market_names():
    """Load token_id -> market question mapping from markets.csv."""
    token_to_name = {}
    if not os.path.exists(MARKETS_CSV):
        return token_to_name
    with open(MARKETS_CSV) as f:
        reader = csv.DictReader(f)
        for row in reader:
            q = row.get("question", row.get("market_slug", "unknown"))
            for col in ("token1", "token2"):
                if col in row and row[col]:
                    token_to_name[row[col]] = q
    return token_to_name


def tail_recent_rows(n=100000):
    """Use tail to get last N lines from the big CSV, return parsed rows."""
    result = subprocess.run(
        ["tail", "-n", str(n), ORDER_CSV],
        capture_output=True, text=True, timeout=60
    )
    if result.returncode != 0:
        print(f"tail failed: {result.stderr}", file=sys.stderr)
        return []

    header = "timestamp,maker,makerAssetId,makerAmountFilled,taker,takerAssetId,takerAmountFilled,transactionHash"
    text = header + "\n" + result.stdout
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


def main():
    from detect import detect_whales, detect_volume_spikes
    from alert import send_alert

    cfg = load_config()
    state = load_state()
    last_ts = state.get("last_scanned_timestamp", 0)

    print(f"[scan] Loading recent rows via tail...")
    rows = tail_recent_rows()
    if not rows:
        print("[scan] No rows found.")
        return

    # Filter to only new rows
    new_rows = [r for r in rows if int(r["timestamp"]) > last_ts]
    print(f"[scan] Total tailed: {len(rows)}, new since last scan: {len(new_rows)}")

    if not new_rows:
        print("[scan] No new data to scan.")
        # Still update timestamp to latest
        max_ts = max(int(r["timestamp"]) for r in rows)
        state["last_scanned_timestamp"] = max_ts
        save_state(state)
        return

    btc_tokens = load_btc_token_ids(cfg)
    print(f"[scan] Loaded {len(btc_tokens)} BTC token IDs")

    # Use ALL tailed rows for volume context but only new rows for whales
    whale_signals = detect_whales(
        new_rows,
        threshold_usd=cfg["whale_threshold_usd"],
        btc_token_ids=btc_tokens,
    )

    volume_signals = detect_volume_spikes(
        rows,  # full context for rate comparison
        window_minutes=cfg["scan_window_minutes"],
        lookback_minutes=cfg["lookback_minutes"],
        multiplier=cfg["volume_spike_multiplier"],
        btc_token_ids=btc_tokens,
        min_volume_usd=cfg.get("min_spike_volume_usd", 1000),
    )

    # Load market names for nice formatting
    market_names = load_market_names()

    total = 0
    for s in whale_signals:
        name = market_names.get(s["token_id"], s["token_id"][:20] + "...")
        btc_tag = " 🟠 BTC" if s.get("is_btc") else ""
        ts_str = time.strftime("%Y-%m-%d %H:%M", time.gmtime(s["timestamp"]))
        msg = f"🐋 Whale alert: ${s['usd']:,.0f} order on [{name}] at {ts_str}{btc_tag}"
        send_alert(msg)
        total += 1

    for s in volume_signals:
        name = market_names.get(s["token_id"], s["token_id"][:20] + "...")
        btc_tag = " 🟠 BTC" if s.get("is_btc") else ""
        msg = f"📈 Volume spike: [{name}] {s['ratio']}x normal volume (${s['window_volume']:,.0f} in window){btc_tag}"
        send_alert(msg)
        total += 1

    # Update state
    max_ts = max(int(r["timestamp"]) for r in new_rows)
    state["last_scanned_timestamp"] = max_ts
    save_state(state)

    print(f"\n[scan] Done. {total} signals detected. State updated to ts={max_ts}")


if __name__ == "__main__":
    main()
