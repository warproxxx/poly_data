import warnings
warnings.filterwarnings('ignore')

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
from poly_utils.utils import get_markets, update_missing_tokens
import subprocess
import pandas as pd

def get_processed_df(df_lazy):
    markets_df = get_markets()
    markets_df = markets_df.rename({'id': 'market_id'})

    # Convert to LazyFrame for streaming join support
    markets_long = (
        markets_df
        .select(["market_id", "token1", "token2"])
        .melt(id_vars="market_id", value_vars=["token1", "token2"],
            variable_name="side", value_name="asset_id")
    ).lazy() 

    # Identify the non-USDC asset for each trade
    df_lazy = df_lazy.with_columns(
        pl.when(pl.col("makerAssetId") != "0")
        .then(pl.col("makerAssetId"))
        .otherwise(pl.col("takerAssetId"))
        .alias("nonusdc_asset_id")
    )

    # Join on the non-USDC asset to recover market_id and side
    df_lazy = df_lazy.join(
        markets_long,
        left_on="nonusdc_asset_id",
        right_on="asset_id",
        how="left",
    )

    # Label asset columns and keep market_id
    df_lazy = df_lazy.with_columns([
        pl.when(pl.col("makerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("makerAsset"),
        pl.when(pl.col("takerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("takerAsset"),
        pl.col("market_id"),
    ])

    df_lazy = df_lazy.select(['timestamp', 'market_id', 'maker', 'makerAsset', 'makerAmountFilled', 'taker', 'takerAsset', 'takerAmountFilled', 'transactionHash'])

    df_lazy = df_lazy.with_columns([
        (pl.col("makerAmountFilled") / 10**6).alias("makerAmountFilled"),
        (pl.col("takerAmountFilled") / 10**6).alias("takerAmountFilled"),
    ])

    df_lazy = df_lazy.with_columns([
        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.lit("BUY"))
        .otherwise(pl.lit("SELL"))
        .alias("taker_direction"),

        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.lit("SELL"))
        .otherwise(pl.lit("BUY"))
        .alias("maker_direction"),
    ])

    df_lazy = df_lazy.with_columns([
        pl.when(pl.col("makerAsset") != "USDC")
        .then(pl.col("makerAsset"))
        .otherwise(pl.col("takerAsset"))
        .alias("nonusdc_side"),

        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.col("takerAmountFilled"))
        .otherwise(pl.col("makerAmountFilled"))
        .alias("usd_amount"),

        pl.when(pl.col("takerAsset") != "USDC")
        .then(pl.col("takerAmountFilled"))
        .otherwise(pl.col("makerAmountFilled"))
        .alias("token_amount"),

        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.col("takerAmountFilled") / pl.col("makerAmountFilled"))
        .otherwise(pl.col("makerAmountFilled") / pl.col("takerAmountFilled"))
        .cast(pl.Float64)
        .alias("price")
    ])

    df_lazy = df_lazy.select(['timestamp', 'market_id', 'maker', 'taker', 'nonusdc_side', 'maker_direction', 'taker_direction', 'price', 'usd_amount', 'token_amount', 'transactionHash'])
    return df_lazy


def process_live():
    processed_file = 'processed/trades.csv'

    print("=" * 60)
    print("Processing Live Trades")
    print("=" * 60)

    last_processed = None

    if os.path.exists(processed_file):
        print(f"Found existing processed file: {processed_file}")
        result = subprocess.run(['tail', '-n', '1', processed_file], capture_output=True, text=True)
        last_line = result.stdout.strip()
        if last_line:
            splitted = last_line.split(',')
            last_processed = {
                'timestamp': pd.to_datetime(splitted[0]),
                'transactionHash': splitted[-1],
                'maker': splitted[2],
                'taker': splitted[3],
            }
            print(f"Resuming from: {last_processed['timestamp']}")
            print(f"   Last hash: {last_processed['transactionHash'][:16]}...")
    else:
        print("No existing processed file found - processing from beginning")

    print(f"\nReading: goldsky/orderFilled.csv")

    schema_overrides = {
        "takerAssetId": pl.Utf8,
        "makerAssetId": pl.Utf8,
    }

    # Keep as LazyFrame for memory-efficient streaming processing
    df_lazy = pl.scan_csv("goldsky/orderFilled.csv", schema_overrides=schema_overrides)
    df_lazy = df_lazy.with_columns(
        pl.from_epoch(pl.col('timestamp'), time_unit='s').alias('timestamp')
    )

    if not os.path.isdir('processed'):
        os.makedirs('processed')
    op_file = 'processed/trades.csv'

    if last_processed is None:
        # Full processing: use sink_csv for streaming write, bypassing memory limits
        print("Processing ALL rows via streaming (memory safe)...")
        final_lazy = get_processed_df(df_lazy)
        final_lazy.sink_csv(op_file)
        print(f"Created new file: {op_file}")
    else:
        # Incremental processing: filter by timestamp first, then collect only recent rows
        print("Fetching and filtering recent rows...")
        
        # Only load rows with timestamp >= last_processed into memory (significant memory savings)
        df_recent = df_lazy.filter(
            pl.col('timestamp') >= last_processed['timestamp']
        ).collect()
        
        df_recent = df_recent.with_row_index()

        same_timestamp = df_recent.filter(
            (pl.col('timestamp') == last_processed['timestamp']) &
            (pl.col("transactionHash") == last_processed['transactionHash']) &
            (pl.col("maker") == last_processed['maker']) &
            (pl.col("taker") == last_processed['taker'])
        )

        if same_timestamp.is_empty():
            print("Last processed row not found in recent data; processing all filtered rows")
            df_process = df_recent.drop('index')
        else:
            # Find the last processed row and extract new data after it
            last_idx = same_timestamp.row(0)[0]
            df_process = df_recent.filter(pl.col('index') > last_idx).drop('index')

        print(f"Processing {len(df_process):,} new rows...")
        
        if len(df_process) > 0:
            # Append processed new rows to existing file
            new_df = get_processed_df(df_process.lazy()).collect()
            print(f"Appending {len(new_df):,} rows to {op_file}")
            with open(op_file, mode="a") as f:
                new_df.write_csv(f, include_header=False)
        else:
            print("No new rows to process.")

    print("=" * 60)
    print("Processing complete!")
    print("=" * 60)
    
if __name__ == "__main__":
    process_live()