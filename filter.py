#!/usr/bin/env python3
"""Polymarket orderFilled data filter — streams 78GB+ CSV via awk, never loads it into memory."""

import argparse
import csv
import io
import os
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timezone

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
ORDER_FILE = os.path.join(DATA_DIR, "goldsky", "orderFilled.csv")
MARKETS_FILE = os.path.join(DATA_DIR, "markets.csv")
FILTERED_DIR = os.path.join(DATA_DIR, "filtered")

# orderFilled columns: timestamp,maker,makerAssetId,makerAmountFilled,taker,takerAssetId,takerAmountFilled,transactionHash
# Index:                0         1     2            3                4     5            6                  7


def load_markets(search=None):
    """Load markets.csv into memory (~50MB). Returns {token_id: market_info}."""
    token_map = {}  # token_id -> {question, market_slug, ...}
    with open(MARKETS_FILE, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            q = row.get("question", "")
            if search and search.lower() not in q.lower():
                continue
            info = {"question": q, "slug": row.get("market_slug", ""), "volume": float(row.get("volume", 0) or 0)}
            for col in ("token1", "token2"):
                tid = row.get(col, "")
                if tid:
                    token_map[tid] = info
    return token_map


def build_awk_filter(args, token_ids=None):
    """Build an awk command list to filter orderFilled.csv. Returns (cmd_list, needs_python_token_filter, temp_file)."""
    conditions = []
    needs_python_filter = False
    tmp_file = None

    # Date filters — timestamp is column $1
    if args.date_from:
        ts = int(datetime.strptime(args.date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
        conditions.append(f"$1 >= {ts}")
    if args.date_to:
        ts = int(datetime.strptime(args.date_to, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
        conditions.append(f"$1 < {ts}")

    # Min USD filter
    if args.min_usd:
        threshold = int(args.min_usd * 1e6)
        conditions.append(f'($3=="0" && $4 >= {threshold}) || ($6=="0" && $7 >= {threshold})')

    # Token filter (specific token ID)
    if args.token:
        conditions.append(f'$3=="{args.token}" || $6=="{args.token}"')

    # For --search: write token IDs to temp file, load in awk
    token_cond = ""
    if token_ids is not None:
        tmp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False)
        for tid in token_ids:
            tmp_file.write(tid + "\n")
        tmp_file.close()
        token_cond = f'(($3 in t) || ($6 in t))'

    cond_parts = []
    if token_cond:
        cond_parts.append(token_cond)
    cond_parts.extend(f"({c})" for c in conditions)
    cond_str = " && ".join(cond_parts) if cond_parts else "1"

    if tmp_file:
        prog = f'BEGIN{{FS=","}} FILENAME==ARGV[1]{{t[$1]=1;next}} NR==1{{next}} {cond_str} {{print}}'
        cmd = ["awk", prog, tmp_file.name, ORDER_FILE]
    else:
        prog = f'BEGIN{{FS=","}} NR==1{{next}} {cond_str} {{print}}'
        cmd = ["awk", prog, ORDER_FILE]

    return cmd, needs_python_filter, tmp_file


def get_usd_amount(row_parts):
    """Extract USD amount from a parsed row. Returns float USD."""
    maker_asset = row_parts[2]
    taker_asset = row_parts[5]
    if maker_asset == "0":
        return int(row_parts[3]) / 1e6
    elif taker_asset == "0":
        return int(row_parts[6]) / 1e6
    return 0.0


def get_token_id(row_parts):
    """Get the non-zero token ID (the outcome token, not USDC)."""
    if row_parts[2] != "0":
        return row_parts[2]
    if row_parts[5] != "0":
        return row_parts[5]
    return ""


def run_filter(args):
    token_map = None
    token_ids = None

    # Load markets if needed
    if args.search or args.summary or args.top_markets:
        print(f"Loading markets.csv...", file=sys.stderr)
        if args.search:
            token_map = load_markets(search=args.search)
            token_ids = set(token_map.keys())
            print(f"  Found {len(token_ids)} token IDs matching '{args.search}' ({len(set(m['question'] for m in token_map.values()))} markets)", file=sys.stderr)
            if not token_ids:
                print("No markets match that search.", file=sys.stderr)
                return
        else:
            token_map = load_markets()
            token_ids = None

    awk_cmd, needs_python_filter, tmp_file = build_awk_filter(args, token_ids)

    print(f"Scanning {ORDER_FILE}...", file=sys.stderr)
    proc = subprocess.Popen(
        awk_cmd,
        stdout=subprocess.PIPE,
        text=True,
        bufsize=1024 * 1024,
    )

    header = "timestamp,maker,makerAssetId,makerAmountFilled,taker,takerAssetId,takerAmountFilled,transactionHash"

    # Prepare output
    out_file = None
    if args.output and not args.summary and not args.top_markets:
        out_path = args.output
        if not os.path.isabs(out_path):
            out_path = os.path.join(FILTERED_DIR, out_path)
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        out_file = open(out_path, "w")
        out_file.write(header + "\n")

    # Stats
    count = 0
    total_usd = 0.0
    market_volumes = defaultdict(float)  # question -> usd
    min_ts = float("inf")
    max_ts = 0
    start_time = time.time()

    try:
        for line in proc.stdout:
            line = line.rstrip("\n")
            parts = line.split(",", 7)
            if len(parts) < 8:
                continue

            # Python-side token filter for large token sets
            if needs_python_filter and token_ids:
                if parts[2] not in token_ids and parts[5] not in token_ids:
                    continue

            count += 1
            ts = int(parts[0])
            usd = get_usd_amount(parts)
            total_usd += usd

            if ts < min_ts:
                min_ts = ts
            if ts > max_ts:
                max_ts = ts

            if args.summary or args.top_markets:
                tid = get_token_id(parts)
                if token_map and tid in token_map:
                    market_volumes[token_map[tid]["question"]] += usd
                else:
                    market_volumes[tid] += usd

            if out_file:
                out_file.write(line + "\n")

            if count % 1_000_000 == 0:
                elapsed = time.time() - start_time
                print(f"  {count:,} rows processed ({elapsed:.0f}s)...", file=sys.stderr)

    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
    finally:
        proc.stdout.close()
        proc.wait()
        if out_file:
            out_file.close()
        if tmp_file:
            os.unlink(tmp_file.name)

    elapsed = time.time() - start_time

    # Print results
    if args.summary or args.top_markets:
        print(f"\n{'='*60}", file=sys.stderr)
        print(f"Results:", file=sys.stderr)
        print(f"  Rows:       {count:,}", file=sys.stderr)
        print(f"  USD Volume: ${total_usd:,.2f}", file=sys.stderr)
        if count > 0:
            print(f"  Date Range: {datetime.fromtimestamp(min_ts, tz=timezone.utc).strftime('%Y-%m-%d')} → {datetime.fromtimestamp(max_ts, tz=timezone.utc).strftime('%Y-%m-%d')}", file=sys.stderr)
            print(f"  Unique Markets: {len(market_volumes):,}", file=sys.stderr)

        n = args.top_markets or 10
        top = sorted(market_volumes.items(), key=lambda x: -x[1])[:n]
        if top:
            print(f"\n  Top {len(top)} Markets by Volume:", file=sys.stderr)
            for i, (q, vol) in enumerate(top, 1):
                label = q if len(q) < 70 else q[:67] + "..."
                print(f"    {i:>3}. ${vol:>14,.2f}  {label}", file=sys.stderr)
        print(f"\n  Time: {elapsed:.1f}s", file=sys.stderr)

        # If --top-markets with -o, write CSV
        if args.top_markets and args.output:
            out_path = args.output
            if not os.path.isabs(out_path):
                out_path = os.path.join(FILTERED_DIR, out_path)
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            with open(out_path, "w") as f:
                f.write("rank,volume_usd,market\n")
                for i, (q, vol) in enumerate(top, 1):
                    q_escaped = q.replace('"', '""')
                    f.write(f'{i},{vol:.2f},"{q_escaped}"\n')
            print(f"  Written to {out_path}", file=sys.stderr)
    else:
        print(f"\nDone: {count:,} rows, ${total_usd:,.2f} USD volume, {elapsed:.1f}s", file=sys.stderr)
        if out_file:
            out_path = args.output
            if not os.path.isabs(out_path):
                out_path = os.path.join(FILTERED_DIR, out_path)
            size = os.path.getsize(out_path)
            unit = "KB" if size < 1e6 else "MB" if size < 1e9 else "GB"
            divisor = 1e3 if size < 1e6 else 1e6 if size < 1e9 else 1e9
            print(f"  Output: {out_path} ({size/divisor:.1f} {unit})", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Filter Polymarket orderFilled data (78GB+)")
    parser.add_argument("--search", "-s", help="Filter by market keyword (searches question text)")
    parser.add_argument("--from", dest="date_from", help="Start date (YYYY-MM-DD, inclusive)")
    parser.add_argument("--to", dest="date_to", help="End date (YYYY-MM-DD, exclusive)")
    parser.add_argument("--min-usd", type=float, help="Minimum USD value per order")
    parser.add_argument("--token", help="Filter by specific token/asset ID")
    parser.add_argument("--output", "-o", help="Output CSV filename (saved to filtered/ dir)")
    parser.add_argument("--summary", action="store_true", help="Show stats only, don't output rows")
    parser.add_argument("--top-markets", type=int, metavar="N", help="Show top N markets by volume")
    args = parser.parse_args()

    if not args.summary and not args.top_markets and not args.output:
        parser.error("Specify -o OUTPUT, --summary, or --top-markets")

    if not os.path.exists(ORDER_FILE):
        print(f"Error: {ORDER_FILE} not found", file=sys.stderr)
        sys.exit(1)

    run_filter(args)


if __name__ == "__main__":
    main()
