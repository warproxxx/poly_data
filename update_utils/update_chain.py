"""
Polymarket CTF Exchange V2 OrderFilled event reader (HyperSync).

Streams OrderFilled logs from the CTF Exchange V2 contract on Polygon via
Envio's HyperSync and writes them to data/orderFilled.csv with v1-compatible
columns so process_live.py can stay close to its original form:

    timestamp, maker, makerAssetId, makerAmountFilled,
    taker, takerAssetId, takerAmountFilled, transactionHash

HyperSync returns block timestamps inline with logs, so there's no separate
eth_getBlock pass. Cursor (last block scanned) is persisted in
data/cursor_state.json.
"""

import asyncio
import csv
import json
import os

import hypersync
from dotenv import load_dotenv
from eth_abi import decode as abi_decode
from eth_utils import keccak
from hypersync import (
    BlockField,
    ClientConfig,
    FieldSelection,
    LogField,
    LogSelection,
    Query,
    StreamConfig,
)

load_dotenv()

# Polymarket CTF Exchange V2 on Polygon (deployed 2026-03-31).
# Migration from v1 occurred on 2026-04-28.
CTF_EXCHANGE_V2 = "0xe111180000d2663c0091e4f400237545b87b996b"
V2_GENESIS_BLOCK = 84_902_353

# OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)
# 3 indexed params (orderHash, maker, taker) + 7 in data.
ORDERFILLED_TOPIC = "0x" + keccak(
    text=(
        "OrderFilled(bytes32,address,address,uint8,uint256,"
        "uint256,uint256,uint256,bytes32,bytes32)"
    )
).hex()
_DATA_TYPES = ["uint8", "uint256", "uint256", "uint256", "uint256", "bytes32", "bytes32"]

OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "orderFilled.csv")
CURSOR_FILE = os.path.join(OUTPUT_DIR, "cursor_state.json")

# Reorg-safety buffer for Polygon.
CONFIRMATIONS = 20

DEFAULT_URL = "https://polygon.hypersync.xyz"

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


def _as_int(v):
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        return int(v, 16) if v.startswith("0x") else int(v)
    raise TypeError(f"unexpected numeric value: {v!r}")


def _hex_to_bytes(s: str) -> bytes:
    return bytes.fromhex(s[2:] if s.startswith("0x") else s)


def _decode_log(log, ts_by_block: dict) -> list:
    """Decode one HyperSync Log into a v1-shaped CSV row."""
    # topics: [event_sig, orderHash, maker, taker]
    topics = log.topics
    maker = "0x" + topics[2][-40:].lower()
    taker = "0x" + topics[3][-40:].lower()

    data_bytes = _hex_to_bytes(log.data)
    side, token_id, maker_amt, taker_amt, _fee, _builder, _metadata = abi_decode(
        _DATA_TYPES, data_bytes
    )

    # V2 `side` reflects the MAKER order's side. BUY=0, SELL=1.
    # process_live treats "0" as USDC and any other id as an outcome token.
    if side == 0:
        maker_asset_id = "0"
        taker_asset_id = str(token_id)
    else:
        maker_asset_id = str(token_id)
        taker_asset_id = "0"

    bn = _as_int(log.block_number)
    tx_hash = log.transaction_hash
    if not tx_hash.startswith("0x"):
        tx_hash = "0x" + tx_hash

    return [
        ts_by_block[bn],
        maker,
        maker_asset_id,
        str(maker_amt),
        taker,
        taker_asset_id,
        str(taker_amt),
        tx_hash,
    ]


def _build_query(from_block: int, to_block: int) -> Query:
    return Query(
        from_block=from_block,
        to_block=to_block + 1,  # HyperSync to_block is exclusive
        logs=[
            LogSelection(
                address=[CTF_EXCHANGE_V2],
                topics=[[ORDERFILLED_TOPIC]],
            )
        ],
        field_selection=FieldSelection(
            block=[BlockField.NUMBER, BlockField.TIMESTAMP],
            log=[
                LogField.BLOCK_NUMBER,
                LogField.TRANSACTION_HASH,
                LogField.TOPIC0,
                LogField.TOPIC1,
                LogField.TOPIC2,
                LogField.TOPIC3,
                LogField.DATA,
            ],
        ),
    )


async def _run() -> None:
    if not os.path.isdir(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    url = os.environ.get("POLYGON_HYPERSYNC_URL", DEFAULT_URL)
    token = os.environ.get("HYPERSYNC_API") or None
    if not token:
        raise RuntimeError(
            "HYPERSYNC_API is not set. HyperSync requires a bearer token "
            "(mandatory since 2025-11-03). Generate a free one with HyperSync "
            "product access at https://envio.dev/app/api-tokens and add it to "
            ".env as HYPERSYNC_API."
        )
    client = hypersync.HypersyncClient(ClientConfig(url=url, bearer_token=token))

    print(f"HyperSync: {url} (with token)")

    height = await client.get_height()
    safe_height = height - CONFIRMATIONS
    start_block = _load_cursor()

    print(f"Archive height: {height:,}  (safe: {safe_height:,} after {CONFIRMATIONS} confs)")
    print(f"Resuming from block {start_block:,}")

    if start_block > safe_height:
        print("Already up to date.")
        return

    new_file = not os.path.isfile(OUTPUT_FILE)
    if new_file:
        with open(OUTPUT_FILE, "w", newline="") as f:
            csv.writer(f).writerow(COLUMNS)

    query = _build_query(start_block, safe_height)
    receiver = await client.stream(query, StreamConfig())

    total = 0
    while True:
        res = await receiver.recv()
        if res is None:
            break

        blocks = res.data.blocks or []
        logs = res.data.logs or []

        ts_by_block = {_as_int(b.number): _as_int(b.timestamp) for b in blocks}

        if logs:
            rows = [_decode_log(log, ts_by_block) for log in logs]
            with open(OUTPUT_FILE, "a", newline="") as f:
                csv.writer(f).writerows(rows)
            total += len(rows)

        # res.next_block is the first block NOT yet covered → save directly.
        _save_cursor(res.next_block)

        print(
            f"  through block {res.next_block - 1:>10,}  "
            f"events: {len(logs):>5}  total: {total:,}"
        )

    print(f"Done. Wrote {total:,} new rows to {OUTPUT_FILE}.")


def update_chain() -> None:
    """Sync entrypoint so update.py's ThreadPoolExecutor can call it directly."""
    asyncio.run(_run())


if __name__ == "__main__":
    update_chain()
