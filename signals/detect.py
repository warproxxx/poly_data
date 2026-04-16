"""Signal detection logic. Operates on pre-extracted rows (list of dicts)."""

import csv
import io
import os
import time
from collections import defaultdict


def detect_whales(rows, threshold_usd=50000, btc_token_ids=None):
    """Find single orders above threshold. Returns list of signal dicts."""
    signals = []
    # takerAmountFilled when takerAssetId == "0" means taker paid USDC
    # makerAmountFilled when makerAssetId == "0" means maker paid USDC
    for row in rows:
        usd = 0
        if row["makerAssetId"] == "0":
            usd = int(row["makerAmountFilled"]) / 1e6
        elif row["takerAssetId"] == "0":
            usd = int(row["takerAmountFilled"]) / 1e6

        if usd >= threshold_usd:
            # Determine which token is the outcome token
            token_id = row["makerAssetId"] if row["makerAssetId"] != "0" else row["takerAssetId"]
            is_btc = btc_token_ids and token_id in btc_token_ids
            signals.append({
                "type": "whale",
                "usd": usd,
                "token_id": token_id,
                "timestamp": int(row["timestamp"]),
                "tx": row["transactionHash"],
                "is_btc": is_btc,
            })
    return signals


def detect_volume_spikes(rows, window_minutes=15, lookback_minutes=60,
                         multiplier=3.0, btc_token_ids=None,
                         min_volume_usd=1000):
    """Detect markets where recent volume >> historical average."""
    now = max(int(r["timestamp"]) for r in rows) if rows else int(time.time())
    window_start = now - window_minutes * 60
    lookback_start = now - lookback_minutes * 60

    # Aggregate volume per token
    window_vol = defaultdict(float)
    lookback_vol = defaultdict(float)

    for row in rows:
        ts = int(row["timestamp"])
        if ts < lookback_start:
            continue
        # USD side
        usd = 0
        if row["makerAssetId"] == "0":
            usd = int(row["makerAmountFilled"]) / 1e6
        elif row["takerAssetId"] == "0":
            usd = int(row["takerAmountFilled"]) / 1e6
        if usd == 0:
            continue

        token_id = row["makerAssetId"] if row["makerAssetId"] != "0" else row["takerAssetId"]

        if ts >= window_start:
            window_vol[token_id] += usd
        lookback_vol[token_id] += usd

    signals = []
    lookback_duration = lookback_minutes
    window_duration = window_minutes

    for token_id, wvol in window_vol.items():
        if wvol < min_volume_usd:
            continue
        lvol = lookback_vol.get(token_id, 0)
        if lvol == 0:
            continue
        # Normalize to per-minute rate
        lookback_rate = lvol / lookback_duration
        window_rate = wvol / window_duration
        if lookback_rate > 0 and window_rate >= multiplier * lookback_rate:
            ratio = window_rate / lookback_rate
            is_btc = btc_token_ids and token_id in btc_token_ids
            signals.append({
                "type": "volume_spike",
                "token_id": token_id,
                "window_volume": wvol,
                "ratio": round(ratio, 1),
                "is_btc": is_btc,
            })
    return signals
