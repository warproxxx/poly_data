#!/usr/bin/env python3
"""
Parallel time-range sync for Polymarket orderFilled events via Goldsky.

Splits the time range (last_synced → now) into N segments, runs parallel
workers to sync each segment, then merges results into the main CSV.

Usage:
    python3 parallel_sync.py --workers 5
    python3 parallel_sync.py --workers 2 --end-ts 1767000000  # test with small range
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from threading import Event

import requests

# ─── Config ───────────────────────────────────────────────────────────────────

QUERY_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"
BATCH_SIZE = 1000
STICKY_THRESHOLD = 100  # skip sticky follow-up if < this many records expected
COLUMNS = ['timestamp', 'maker', 'makerAssetId', 'makerAmountFilled',
           'taker', 'takerAssetId', 'takerAmountFilled', 'transactionHash']

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MAIN_CSV = os.path.join(BASE_DIR, 'goldsky', 'orderFilled.csv')
CURSOR_FILE = os.path.join(BASE_DIR, 'goldsky', 'cursor_state.json')
TEMP_DIR = os.path.join(BASE_DIR, 'goldsky', 'parallel_segments')
LOG_DIR = os.path.join(BASE_DIR, 'parallel_logs')

# Global shutdown event
shutdown_event = Event()


# ─── Helpers ──────────────────────────────────────────────────────────────────

def ts_to_str(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')


def get_last_timestamp():
    """Read last synced timestamp from cursor file or CSV tail."""
    if os.path.isfile(CURSOR_FILE):
        try:
            with open(CURSOR_FILE) as f:
                state = json.load(f)
            ts = state.get('last_timestamp', 0)
            if ts > 0:
                return ts
        except Exception:
            pass

    if os.path.isfile(MAIN_CSV):
        try:
            result = subprocess.run(['tail', '-1', MAIN_CSV],
                                    capture_output=True, text=True, timeout=10)
            line = result.stdout.strip()
            if line and not line.startswith('timestamp'):
                return int(line.split(',')[0])
        except Exception:
            pass
    return 0


def goldsky_query(session, where_clause, at_once=BATCH_SIZE):
    """Execute a single Goldsky GraphQL query. Returns list of events."""
    query = f'''{{
        orderFilledEvents(
            orderBy: timestamp, orderDirection: asc,
            first: {at_once},
            where: {{{where_clause}}}
        ) {{
            id timestamp maker makerAmountFilled makerAssetId
            taker takerAmountFilled takerAssetId transactionHash
        }}
    }}'''

    for attempt in range(5):
        if shutdown_event.is_set():
            return []
        try:
            resp = session.post(QUERY_URL, json={'query': query}, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if 'errors' in data:
                raise RuntimeError(data['errors'])
            return data.get('data', {}).get('orderFilledEvents', [])
        except Exception as e:
            if shutdown_event.is_set():
                return []
            wait = min(2 ** attempt, 30)
            print(f"  [retry {attempt+1}] {e} — waiting {wait}s")
            time.sleep(wait)
    return []


# ─── Worker ───────────────────────────────────────────────────────────────────

def sync_segment(worker_id, start_ts, end_ts):
    """
    Sync all orderFilled events in (start_ts, end_ts] to a temp CSV.
    Returns (worker_id, record_count, output_path).
    """
    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)

    out_path = os.path.join(TEMP_DIR, f'segment_{worker_id}.csv')
    log_path = os.path.join(LOG_DIR, f'worker_{worker_id}.log')

    log = open(log_path, 'w')
    csv_f = open(out_path, 'w')
    session = requests.Session()

    def logmsg(msg):
        line = f"[W{worker_id}] {msg}"
        log.write(line + '\n')
        log.flush()
        print(line)

    logmsg(f"Range: {ts_to_str(start_ts)} → {ts_to_str(end_ts)}")

    last_ts = start_ts
    last_id = None
    sticky_ts = None
    total = 0
    batches = 0

    try:
        while not shutdown_event.is_set():
            # Build where clause
            if sticky_ts is not None:
                where = f'timestamp: "{sticky_ts}", id_gt: "{last_id}"'
            else:
                where = f'timestamp_gt: "{last_ts}", timestamp_lte: "{end_ts}"'

            events = goldsky_query(session, where)
            if not events:
                if sticky_ts is not None:
                    # Done with sticky, advance
                    last_ts = sticky_ts
                    sticky_ts = None
                    last_id = None
                    continue
                break

            events.sort(key=lambda e: (int(e['timestamp']), e['id']))
            n = len(events)
            batch_last_ts = int(events[-1]['timestamp'])
            batch_first_ts = int(events[0]['timestamp'])
            batches += 1

            # Write to CSV
            seen = set()
            for ev in events:
                if ev['id'] in seen:
                    continue
                seen.add(ev['id'])
                csv_f.write(','.join(str(ev.get(c, '')) for c in COLUMNS) + '\n')
            csv_f.flush()
            total += len(seen)

            # Cursor logic with sticky optimization
            if n >= BATCH_SIZE:
                if batch_first_ts == batch_last_ts:
                    # All same timestamp — must paginate within it
                    sticky_ts = batch_last_ts
                    last_id = events[-1]['id']
                    logmsg(f"Batch {batches}: ts={batch_last_ts} n={n} [STICKY-SAME]")
                else:
                    # Full batch, mixed timestamps. Check if sticky follow-up is worth it.
                    # Instead of going sticky on every full batch, just advance past the
                    # second-to-last timestamp to avoid tiny follow-ups.
                    # Find the last "complete" timestamp (one before the boundary)
                    boundary_ts = batch_last_ts
                    safe_ts = batch_first_ts
                    for ev in events:
                        t = int(ev['timestamp'])
                        if t < boundary_ts:
                            safe_ts = t

                    # Count how many events are at the boundary timestamp
                    boundary_count = sum(1 for ev in events if int(ev['timestamp']) == boundary_ts)

                    if boundary_count >= STICKY_THRESHOLD:
                        # Many events at boundary — go sticky to get them all
                        sticky_ts = boundary_ts
                        last_id = events[-1]['id']
                        logmsg(f"Batch {batches}: ts={batch_first_ts}-{batch_last_ts} n={n} [STICKY bc={boundary_count}]")
                    else:
                        # Few events at boundary — just advance past the safe timestamp
                        # We already have all events up to boundary_ts from this batch
                        last_ts = safe_ts
                        sticky_ts = None
                        last_id = None
                        logmsg(f"Batch {batches}: ts={batch_first_ts}-{batch_last_ts} n={n} [SKIP-STICKY bc={boundary_count}]")
            else:
                # Not full — all events retrieved
                if sticky_ts is not None:
                    last_ts = sticky_ts
                    sticky_ts = None
                    last_id = None
                    logmsg(f"Batch {batches}: ts={batch_last_ts} n={n} [STICKY-DONE]")
                else:
                    last_ts = batch_last_ts
                    logmsg(f"Batch {batches}: ts={batch_last_ts} n={n}")

                if n < BATCH_SIZE and sticky_ts is None:
                    # Might be more data — only break if we've reached end_ts
                    if batch_last_ts >= end_ts:
                        break
                    # Otherwise keep going (sparse data region)

    except Exception as e:
        logmsg(f"ERROR: {e}")

    logmsg(f"Done: {total} records in {batches} batches")
    csv_f.close()
    log.close()
    session.close()

    return worker_id, total, out_path


# ─── Merge ────────────────────────────────────────────────────────────────────

def merge_segments(segment_files, record_counts):
    """Merge temp segment CSVs into the main orderFilled.csv in order."""
    # Segments are already in timestamp order (worker 0 = earliest range)
    # Just append them in order
    print(f"\nMerging {len(segment_files)} segments into {MAIN_CSV}...")

    total_new = 0
    with open(MAIN_CSV, 'a') as out:
        for wid in sorted(segment_files.keys()):
            path = segment_files[wid]
            if not os.path.isfile(path):
                continue
            count = 0
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        out.write(line + '\n')
                        count += 1
            total_new += count
            print(f"  Segment {wid}: {count} records appended")

    # Update cursor state
    if total_new > 0:
        # Get last timestamp from merged data
        result = subprocess.run(['tail', '-1', MAIN_CSV],
                                capture_output=True, text=True, timeout=10)
        last_line = result.stdout.strip()
        if last_line:
            last_ts = int(last_line.split(',')[0])
            with open(CURSOR_FILE, 'w') as f:
                json.dump({'last_timestamp': last_ts, 'last_id': None,
                           'sticky_timestamp': None}, f)
            print(f"  Cursor updated to {last_ts} ({ts_to_str(last_ts)})")

    print(f"  Total new records: {total_new}")

    # Cleanup temp files
    for path in segment_files.values():
        if os.path.isfile(path):
            os.remove(path)
    if os.path.isdir(TEMP_DIR) and not os.listdir(TEMP_DIR):
        os.rmdir(TEMP_DIR)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Parallel Goldsky sync')
    parser.add_argument('--workers', type=int, default=5, help='Number of parallel workers')
    parser.add_argument('--end-ts', type=int, default=None, help='End timestamp (default: now)')
    args = parser.parse_args()

    # Signal handling
    def handle_signal(sig, frame):
        print("\n⚠ Shutdown requested — finishing current batches...")
        shutdown_event.set()
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Determine time range
    start_ts = get_last_timestamp()
    end_ts = args.end_ts or int(time.time())

    if start_ts >= end_ts:
        print("Already up to date!")
        return

    gap = end_ts - start_ts
    print(f"{'='*60}")
    print(f"Parallel Goldsky Sync")
    print(f"  Range: {ts_to_str(start_ts)} → {ts_to_str(end_ts)}")
    print(f"  Gap:   {gap/86400:.1f} days")
    print(f"  Workers: {args.workers}")
    print(f"{'='*60}\n")

    # Split into segments
    segment_size = gap // args.workers
    segments = []
    for i in range(args.workers):
        seg_start = start_ts + i * segment_size
        seg_end = start_ts + (i + 1) * segment_size if i < args.workers - 1 else end_ts
        segments.append((i, seg_start, seg_end))
        print(f"  Worker {i}: {ts_to_str(seg_start)} → {ts_to_str(seg_end)} ({(seg_end-seg_start)/86400:.1f}d)")

    print()
    t0 = time.time()

    # Run workers in parallel
    segment_files = {}
    record_counts = {}

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(sync_segment, wid, s, e): wid
                   for wid, s, e in segments}

        for future in as_completed(futures):
            wid = futures[future]
            try:
                wid, count, path = future.result()
                segment_files[wid] = path
                record_counts[wid] = count
            except Exception as e:
                print(f"Worker {wid} failed: {e}")

    elapsed = time.time() - t0
    total = sum(record_counts.values())
    print(f"\nAll workers done in {elapsed:.1f}s — {total} records total")
    print(f"Throughput: {total/max(elapsed,1):.0f} records/sec")

    if not shutdown_event.is_set() and total > 0:
        merge_segments(segment_files, record_counts)
    elif shutdown_event.is_set():
        print("Shutdown — skipping merge. Temp files preserved in", TEMP_DIR)

    print("Done!")


if __name__ == '__main__':
    main()
