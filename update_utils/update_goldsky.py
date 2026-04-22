import os
import json
import pandas as pd
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from flatten_json import flatten
from datetime import datetime, timezone
import subprocess
import time
from update_utils.update_markets import update_markets

# Global runtime timestamp - set once when program starts
RUNTIME_TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')

# Columns to save
COLUMNS_TO_SAVE = ['timestamp', 'maker', 'makerAssetId', 'makerAmountFilled', 'taker', 'takerAssetId', 'takerAmountFilled', 'transactionHash']

if not os.path.isdir('goldsky'):
    os.mkdir('goldsky')

CURSOR_FILE = 'goldsky/cursor_state.json'
LOCK_FILE = 'goldsky/scraper.lock'

def _acquire_lock():
    if os.path.isfile(LOCK_FILE):
        try:
            with open(LOCK_FILE, 'r') as f:
                pid = int(f.read().strip())
            try:
                os.kill(pid, 0)
                return False  # Process still running
            except OSError:
                pass  # Stale lock — process is gone
        except Exception:
            pass
    with open(LOCK_FILE, 'w') as f:
        f.write(str(os.getpid()))
    return True

def _release_lock():
    try:
        if os.path.isfile(LOCK_FILE):
            os.remove(LOCK_FILE)
    except Exception:
        pass

def _write_buffer(buffer, output_file):
    """Write buffered dataframes to CSV in one operation."""
    if not buffer:
        return
    combined_df = pd.concat(buffer, ignore_index=True)
    # Deduplicate by transactionHash (id column is dropped before buffering)
    combined_df = combined_df.drop_duplicates(subset=['transactionHash'])
    # Only keep the columns we want to save
    cols = [c for c in COLUMNS_TO_SAVE if c in combined_df.columns]
    combined_df = combined_df[cols]
    if os.path.isfile(output_file):
        combined_df.to_csv(output_file, index=None, mode='a', header=None)
    else:
        combined_df.to_csv(output_file, index=None)
    print(f"  -> Wrote {len(combined_df)} records to disk")

def save_cursor(timestamp, last_id, sticky_timestamp=None):
    """Save cursor state to file for efficient resume."""
    state = {
        'last_timestamp': timestamp,
        'last_id': last_id,
        'sticky_timestamp': sticky_timestamp
    }
    with open(CURSOR_FILE, 'w') as f:
        json.dump(state, f)

def get_latest_cursor():
    """Get the latest cursor state for efficient resume.
    Returns (timestamp, last_id, sticky_timestamp) tuple."""
    # First try to load from cursor state file (most efficient)
    if os.path.isfile(CURSOR_FILE):
        try:
            with open(CURSOR_FILE, 'r') as f:
                state = json.load(f)
            timestamp = state.get('last_timestamp', 0)
            last_id = state.get('last_id')
            sticky_timestamp = state.get('sticky_timestamp')
            
            # Validate cursor state: if sticky_timestamp is set, last_id must also be set
            if sticky_timestamp is not None and last_id is None:
                print(f"Warning: Invalid cursor state (sticky_timestamp={sticky_timestamp} but last_id=None), clearing sticky state")
                sticky_timestamp = None
            
            if timestamp > 0:
                readable_time = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                print(f'Resuming from cursor state: timestamp {timestamp} ({readable_time}), id: {last_id}, sticky: {sticky_timestamp}')
                return timestamp, last_id, sticky_timestamp
        except Exception as e:
            print(f"Error reading cursor file: {e}")
    
    # Fallback: read from CSV file
    cache_file = 'goldsky/orderFilled.csv'
    
    if not os.path.isfile(cache_file):
        print("No existing file found, starting from beginning of time (timestamp 0)")
        return 0, None, None
    
    try:
        # Use tail to get the last line efficiently
        result = subprocess.run(['tail', '-n', '1', cache_file], capture_output=True, text=True, check=True)
        last_line = result.stdout.strip()
        if last_line:
            # Get header to find column indices
            header_result = subprocess.run(['head', '-n', '1', cache_file], capture_output=True, text=True, check=True)
            headers = header_result.stdout.strip().split(',')
            
            if 'timestamp' in headers:
                timestamp_index = headers.index('timestamp')
                # 'id' is the first column in COLUMNS_TO_SAVE but not saved; use transactionHash
                # The CSV columns are: timestamp,maker,makerAssetId,makerAmountFilled,taker,takerAssetId,takerAmountFilled,transactionHash
                # We need the Goldsky event id — it's not stored in the CSV.
                # Instead, use a sticky cursor at last_timestamp so we page past all
                # already-stored events at that second before advancing forward.
                # We set last_timestamp (non-sticky) to last_timestamp - 1 so the sticky
                # clause is: timestamp=last_timestamp, id_gt="" which returns all ids > ""
                # (i.e. all events at that second), then the loop exhausts them and advances.
                values = last_line.split(',')
                if len(values) > timestamp_index:
                    last_timestamp = int(values[timestamp_index])
                    readable_time = datetime.fromtimestamp(last_timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                    print(f'Resuming from CSV (no cursor file): timestamp {last_timestamp} ({readable_time})')
                    # Use sticky cursor at last_timestamp with id_gt="" to exhaust that
                    # second completely, then naturally advance — avoids false end-of-data
                    # that the old "timestamp - 1" approach caused.
                    return last_timestamp - 1, None, last_timestamp
    except Exception as e:
        print(f"Error reading latest file with tail: {e}")
        # Fallback to pandas
        try:
            df = pd.read_csv(cache_file)
            if len(df) > 0 and 'timestamp' in df.columns:
                last_timestamp = int(df.iloc[-1]['timestamp'])
                readable_time = datetime.fromtimestamp(last_timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                print(f'Resuming from CSV (no cursor file): timestamp {last_timestamp} ({readable_time})')
                return last_timestamp - 1, None, last_timestamp
        except Exception as e2:
            print(f"Error reading with pandas: {e2}")
    
    # Fallback to beginning of time
    print("Falling back to beginning of time (timestamp 0)")
    return 0, None, None

def scrape(at_once=1000):
    QUERY_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"
    print(f"Query URL: {QUERY_URL}")
    print(f"Runtime timestamp: {RUNTIME_TIMESTAMP}")

    # Get starting cursor from latest file (includes sticky state for perfect resume)
    last_timestamp, last_id, sticky_timestamp = get_latest_cursor()
    count = 0
    total_records = 0
    write_buffer = []  # Buffer batches in memory
    buffer_size = 50  # Write every N batches to reduce I/O

    print(f"\nStarting scrape for orderFilledEvents")

    output_file = 'goldsky/orderFilled.csv'
    print(f"Output file: {output_file}")
    print(f"Saving columns: {COLUMNS_TO_SAVE}")
    print(f"Batch size: {at_once}, Write buffer: {buffer_size} batches")

    # Create client once and reuse across batches
    transport = RequestsHTTPTransport(url=QUERY_URL, verify=True, retries=3, timeout=30)
    client = Client(transport=transport)

    while True:
        # Capture sticky state at start of loop to determine exit condition later
        is_sticky_query = sticky_timestamp is not None
        
        # Build the where clause based on cursor state
        if sticky_timestamp is not None:
            # We're in sticky mode: stay at this timestamp and paginate by id
            where_clause = f'timestamp: "{sticky_timestamp}", id_gt: "{last_id}"'
        else:
            # Normal mode: advance by timestamp
            where_clause = f'timestamp_gt: "{last_timestamp}"'

        q_string = '''query MyQuery {
                        orderFilledEvents(orderBy: timestamp, orderDirection: asc
                                             first: ''' + str(at_once) + '''
                                             where: {''' + where_clause + '''}) {
                            fee
                            id
                            maker
                            makerAmountFilled
                            makerAssetId
                            orderHash
                            taker
                            takerAmountFilled
                            takerAssetId
                            timestamp
                            transactionHash
                        }
                    }
                '''

        query = gql(q_string)

        try:
            res = client.execute(query)
        except Exception as e:
            print(f"Query error: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)
            # Recreate client on error to reset connection
            transport = RequestsHTTPTransport(url=QUERY_URL, verify=True, retries=3, timeout=30)
            client = Client(transport=transport)
            continue

        if not res['orderFilledEvents'] or len(res['orderFilledEvents']) == 0:
            if sticky_timestamp is not None:
                # Exhausted events at sticky timestamp, advance to next timestamp
                last_timestamp = sticky_timestamp
                sticky_timestamp = None
                last_id = None
                continue
            # Write any remaining buffered data before exiting
            if write_buffer:
                _write_buffer(write_buffer, output_file)
            print(f"No more data for orderFilledEvents")
            break

        df = pd.DataFrame([flatten(x) for x in res['orderFilledEvents']]).reset_index(drop=True)

        # Sort by timestamp and id for consistent ordering
        df = df.sort_values(['timestamp', 'id'], ascending=True).reset_index(drop=True)

        batch_last_timestamp = int(df.iloc[-1]['timestamp'])
        batch_last_id = df.iloc[-1]['id']
        batch_first_timestamp = int(df.iloc[0]['timestamp'])

        readable_time = datetime.fromtimestamp(batch_last_timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

        # Determine if we need sticky cursor for next iteration
        if len(df) >= at_once:
            # Batch is full - check if all events are at the same timestamp
            if batch_first_timestamp == batch_last_timestamp:
                # All events at same timestamp, need to continue paginating at this timestamp
                sticky_timestamp = batch_last_timestamp
                last_id = batch_last_id
                print(f"Batch {count + 1}: Timestamp {batch_last_timestamp} ({readable_time}), Records: {len(df)} [STICKY - continuing at same timestamp]")
            else:
                # Mixed timestamps - some events might be lost at the boundary timestamp
                # Stay sticky at the last timestamp to ensure we get all events
                sticky_timestamp = batch_last_timestamp
                last_id = batch_last_id
                print(f"Batch {count + 1}: Timestamps {batch_first_timestamp}-{batch_last_timestamp} ({readable_time}), Records: {len(df)} [STICKY - ensuring complete timestamp]")
        else:
            # Batch not full - we have all events, can advance normally
            if sticky_timestamp is not None:
                # We were in sticky mode, now exhausted - advance past this timestamp
                last_timestamp = sticky_timestamp
                sticky_timestamp = None
                last_id = None
                print(f"Batch {count + 1}: Timestamp {batch_last_timestamp} ({readable_time}), Records: {len(df)} [STICKY COMPLETE]")
                count += 1
                total_records += len(df)
                df = df[COLUMNS_TO_SAVE].copy()
                write_buffer.append(df)
                # Write buffer if it reaches threshold
                if len(write_buffer) >= buffer_size:
                    _write_buffer(write_buffer, output_file)
                    write_buffer = []
                save_cursor(last_timestamp, last_id, sticky_timestamp)
                continue  # skip break check — more data may exist at higher timestamps
            else:
                # Normal advancement
                last_timestamp = batch_last_timestamp
                print(f"Batch {count + 1}: Last timestamp {batch_last_timestamp} ({readable_time}), Records: {len(df)}")

        count += 1
        total_records += len(df)

        # Filter to only the columns we want to save
        df_to_save = df[COLUMNS_TO_SAVE].copy()
        write_buffer.append(df_to_save)

        # Write buffer if it reaches threshold
        if len(write_buffer) >= buffer_size:
            _write_buffer(write_buffer, output_file)
            write_buffer = []

        # Save cursor state for efficient resume (no duplicates on restart)
        save_cursor(last_timestamp, last_id, sticky_timestamp)

        if len(df) < at_once and not is_sticky_query:
            break

    # Clear cursor file on successful completion
    if os.path.isfile(CURSOR_FILE):
        os.remove(CURSOR_FILE)

    print(f"Finished scraping orderFilledEvents")
    print(f"Total new records: {total_records}")
    print(f"Output file: {output_file}")

def update_goldsky():
    """Run scraping for orderFilledEvents"""
    if not _acquire_lock():
        print(f"Another scraper instance is already running (PID in {LOCK_FILE}). Exiting.")
        return
    print(f"\n{'='*50}")
    print(f"Starting to scrape orderFilledEvents")
    print(f"Runtime: {RUNTIME_TIMESTAMP}")
    print(f"{'='*50}")
    try:
        scrape()
        print(f"Successfully completed orderFilledEvents")
    except Exception as e:
        print(f"Error scraping orderFilledEvents: {str(e)}")
    finally:
        _release_lock()