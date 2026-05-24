"""
Polymarket CTF Exchange V2 OrderFilled event poller.

Reads order-fill events directly from Polygon via JSON-RPC.
No Goldsky, no subgraph, no API key beyond an optional RPC URL.

Writes data/orderFilled.csv with columns matching the legacy v1 shape so the
downstream processor can stay close to its original form:

    timestamp, maker, makerAssetId, makerAmountFilled,
    taker, takerAssetId, takerAmountFilled, transactionHash

Cursor (last block scanned) is persisted in data/cursor_state.json.
"""

import csv
import json
import os
import time
from datetime import datetime, timezone

from eth_abi import decode as abi_decode
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

# Polymarket CTF Exchange V2 on Polygon (deployed 2026-03-31).
# Migration from v1 occurred on 2026-04-28.
CTF_EXCHANGE_V2 = Web3.to_checksum_address("0xE111180000d2663C0091e4f400237545B87B996B")
V2_GENESIS_BLOCK = 84_902_353

# OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)
# 3 indexed params (orderHash, maker, taker) + 7 in data.
ORDERFILLED_TOPIC = "0x" + Web3.keccak(
    text=(
        "OrderFilled(bytes32,address,address,uint8,uint256,"
        "uint256,uint256,uint256,bytes32,bytes32)"
    )
).hex().lstrip("0x")
_DATA_TYPES = ["uint8", "uint256", "uint256", "uint256", "uint256", "bytes32", "bytes32"]

OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "orderFilled.csv")
CURSOR_FILE = os.path.join(OUTPUT_DIR, "cursor_state.json")

COLUMNS = [
    "timestamp",
    "maker",
    "makerAssetId",
    "makerAmountFilled",
    "taker",
    "takerAssetId",
    "takerAmountFilled",
    "transactionHash",
]

DEFAULT_RPC = "https://polygon-bor-rpc.publicnode.com"


def _rpc_url() -> str:
    return os.environ.get("POLYGON_RPC_URL", DEFAULT_RPC)


def _block_range() -> int:
    # 500 is conservative: the V2 contract is very active (often >200 events/block),
    # and most providers return-size-cap before they range-cap. Auto-halves on errors.
    # Bump POLYGON_BLOCK_RANGE on a paid plan with bigger response limits.
    return int(os.environ.get("POLYGON_BLOCK_RANGE", "500"))


def _confirmations() -> int:
    return int(os.environ.get("POLYGON_CONFIRMATIONS", "20"))


def _load_cursor() -> int:
    if os.path.isfile(CURSOR_FILE):
        try:
            with open(CURSOR_FILE) as f:
                last = json.load(f).get("last_block")
            if isinstance(last, int) and last >= V2_GENESIS_BLOCK:
                return last
        except Exception:
            pass
    return V2_GENESIS_BLOCK


def _save_cursor(next_block: int) -> None:
    with open(CURSOR_FILE, "w") as f:
        json.dump({"last_block": next_block}, f)


def _decode_log(log) -> dict:
    """Decode a single OrderFilled log into a v1-shaped row."""
    # topics: [event_sig, orderHash, maker, taker]
    topics = log["topics"]
    maker = "0x" + topics[2].hex()[-40:]
    taker = "0x" + topics[3].hex()[-40:]

    data = log["data"]
    if isinstance(data, str):
        data = bytes.fromhex(data[2:] if data.startswith("0x") else data)

    side, token_id, maker_amt, taker_amt, _fee, _builder, _metadata = abi_decode(
        _DATA_TYPES, data
    )

    # V2 `side` reflects the MAKER order's side. BUY=0, SELL=1.
    # process_live treats "0" as USDC and any other id as an outcome token.
    if side == 0:
        # Maker buys tokens: gives USDC, gets tokens.
        maker_asset_id = "0"
        taker_asset_id = str(token_id)
    else:
        # Maker sells tokens: gives tokens, gets USDC.
        maker_asset_id = str(token_id)
        taker_asset_id = "0"

    tx_hash = log["transactionHash"]
    if isinstance(tx_hash, (bytes, bytearray)):
        tx_hash = "0x" + tx_hash.hex()

    return {
        "maker": maker.lower(),
        "taker": taker.lower(),
        "makerAssetId": maker_asset_id,
        "takerAssetId": taker_asset_id,
        "makerAmountFilled": str(maker_amt),
        "takerAmountFilled": str(taker_amt),
        "transactionHash": tx_hash,
        "_block_number": log["blockNumber"],
    }


def _get_logs_with_backoff(w3, start: int, end: int):
    """Fetch logs, halving the range on provider-side range errors."""
    cur_end = end
    while True:
        try:
            return w3.eth.get_logs(
                {
                    "fromBlock": start,
                    "toBlock": cur_end,
                    "address": CTF_EXCHANGE_V2,
                    "topics": [ORDERFILLED_TOPIC],
                }
            ), cur_end
        except Exception as e:
            msg = str(e).lower()
            range_err = any(s in msg for s in ("range", "too many", "limit", "result"))
            if not range_err or cur_end <= start:
                raise
            new_end = start + max(1, (cur_end - start) // 2)
            print(f"  ! get_logs failed ({e}); shrinking range {start}-{cur_end} → {start}-{new_end}")
            cur_end = new_end


def update_chain() -> None:
    if not os.path.isdir(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    rpc = _rpc_url()
    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 30}))
    # Polygon is PoA (Bor consensus) — extraData exceeds the 32-byte default validator.
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    if not w3.is_connected():
        raise RuntimeError(f"Cannot connect to Polygon RPC: {rpc}")

    latest = w3.eth.block_number
    safe_latest = latest - _confirmations()
    start_block = _load_cursor()

    print(f"RPC: {rpc}")
    print(f"Latest block: {latest:,}  (safe: {safe_latest:,} after {_confirmations()} confs)")
    print(f"Resuming from block {start_block:,}")

    if start_block > safe_latest:
        print("Already up to date.")
        return

    new_file = not os.path.isfile(OUTPUT_FILE)
    if new_file:
        with open(OUTPUT_FILE, "w", newline="") as f:
            csv.writer(f).writerow(COLUMNS)

    block_range = _block_range()
    cur = start_block
    total = 0
    ts_cache: dict = {}

    while cur <= safe_latest:
        end = min(cur + block_range - 1, safe_latest)
        logs, end = _get_logs_with_backoff(w3, cur, end)

        if logs:
            rows = []
            for log in logs:
                row = _decode_log(log)
                bn = row.pop("_block_number")
                if bn not in ts_cache:
                    ts_cache[bn] = w3.eth.get_block(bn)["timestamp"]
                row["timestamp"] = ts_cache[bn]
                rows.append([row[c] for c in COLUMNS])

            with open(OUTPUT_FILE, "a", newline="") as f:
                csv.writer(f).writerows(rows)
            total += len(rows)

        readable = datetime.fromtimestamp(
            ts_cache.get(end, time.time()), tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"  Blocks {cur:>10,} → {end:<10,} ({readable})  "
            f"events: {len(logs):>4}  total: {total:,}"
        )

        cur = end + 1
        _save_cursor(cur)

        # Trim cache so it doesn't grow unboundedly across a long backfill.
        if len(ts_cache) > 50_000:
            ts_cache.clear()

        time.sleep(0.05)  # be polite to free-tier RPCs

    print(f"Done. Wrote {total:,} new rows to {OUTPUT_FILE}.")


if __name__ == "__main__":
    update_chain()
