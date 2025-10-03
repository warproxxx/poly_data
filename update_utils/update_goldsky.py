import os
import polars as pl
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from flatten_json import flatten
from datetime import datetime, timezone
import time
from update_utils.update_markets import update_markets

# Global runtime timestamp - set once when program starts
RUNTIME_TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')

# Columns to save
COLUMNS_TO_SAVE = ['timestamp', 'maker', 'makerAssetId', 'makerAmountFilled', 'taker', 'takerAssetId', 'takerAmountFilled', 'transactionHash']

if not os.path.isdir('goldsky'):
    os.mkdir('goldsky')

def get_latest_timestamp():
    """Get the latest timestamp from orderFilled.parquet, or 0 if file doesn't exist"""
    cache_file = 'goldsky/orderFilled.parquet'

    if not os.path.isfile(cache_file):
        print("No existing file found, starting from beginning of time (timestamp 0)")
        return 0

    try:
        df = pl.read_parquet(cache_file)
        if len(df) > 0 and 'timestamp' in df.columns:
            last_timestamp = df['timestamp'].max()
            readable_time = datetime.fromtimestamp(int(last_timestamp), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            print(f'Resuming from timestamp {last_timestamp} ({readable_time})')
            return int(last_timestamp)
    except Exception as e:
        print(f"Error reading Parquet file: {e}")

    # Fallback to beginning of time
    print("Falling back to beginning of time (timestamp 0)")
    return 0

def scrape(at_once=1000):
    QUERY_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"
    print(f"Query URL: {QUERY_URL}")
    print(f"Runtime timestamp: {RUNTIME_TIMESTAMP}")

    # Get starting timestamp from latest file
    last_value = get_latest_timestamp()
    count = 0
    total_records = 0

    print(f"\nStarting scrape for orderFilledEvents")

    output_file = 'goldsky/orderFilled.parquet'
    print(f"Output file: {output_file}")
    print(f"Saving columns: {COLUMNS_TO_SAVE}")

    all_new_data = []

    while True:
        q_string = '''query MyQuery {
                        orderFilledEvents(orderBy: timestamp
                                             first: ''' + str(at_once) + '''
                                             where: {timestamp_gt: "''' + str(last_value) + '''"}) {
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
        transport = RequestsHTTPTransport(url=QUERY_URL, verify=True, retries=3)
        client = Client(transport=transport)

        try:
            res = client.execute(query)
        except Exception as e:
            print(f"Query error: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)
            continue

        if not res['orderFilledEvents'] or len(res['orderFilledEvents']) == 0:
            print(f"No more data for orderFilledEvents")
            break

        # Convert to Polars DataFrame
        batch_data = [flatten(x) for x in res['orderFilledEvents']]
        df = pl.DataFrame(batch_data)

        # Sort by timestamp and update last_value
        df = df.sort('timestamp')
        last_value = df['timestamp'].max()

        readable_time = datetime.fromtimestamp(int(last_value), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        print(f"Batch {count + 1}: Last timestamp {last_value} ({readable_time}), Records: {len(df)}")

        count += 1
        total_records += len(df)

        # Remove duplicates
        df = df.unique()

        # Filter to only the columns we want to save
        df_to_save = df.select(COLUMNS_TO_SAVE)

        # Accumulate data
        all_new_data.append(df_to_save)

        if len(df) < at_once:
            break

    # Save all accumulated data to Parquet
    if all_new_data:
        new_df = pl.concat(all_new_data)

        if os.path.isfile(output_file):
            existing_df = pl.read_parquet(output_file)
            combined_df = pl.concat([existing_df, new_df])
            combined_df.write_parquet(output_file, compression="zstd")
        else:
            new_df.write_parquet(output_file, compression="zstd")

    print(f"Finished scraping orderFilledEvents")
    print(f"Total new records: {total_records}")
    print(f"Output file: {output_file}")

def update_goldsky():
    """Run scraping for orderFilledEvents"""
    print(f"\n{'='*50}")
    print(f"Starting to scrape orderFilledEvents")
    print(f"Runtime: {RUNTIME_TIMESTAMP}")
    print(f"{'='*50}")
    try:
        scrape()
        print(f"Successfully completed orderFilledEvents")
    except Exception as e:
        print(f"Error scraping orderFilledEvents: {str(e)}")