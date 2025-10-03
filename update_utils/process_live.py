import warnings
warnings.filterwarnings('ignore')

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
from poly_utils.utils import get_markets, update_missing_tokens

def get_processed_df(df):
    markets_df = get_markets()
    markets_df = markets_df.rename({'id': 'market_id'})

    # 1) Make markets long: (market_id, side, asset_id) where side ∈ {"token1", "token2"}
    markets_long = (
        markets_df
        .select(["market_id", "token1", "token2"])
        .melt(id_vars="market_id", value_vars=["token1", "token2"],
            variable_name="side", value_name="asset_id")
    )

    # 2) Identify the non-USDC asset for each trade (the one that isn't 0)
    df = df.with_columns(
        pl.when(pl.col("makerAssetId") != "0")
        .then(pl.col("makerAssetId"))
        .otherwise(pl.col("takerAssetId"))
        .alias("nonusdc_asset_id")
    )

    # 3) Join once on that non-USDC asset to recover the market + side ("token1" or "token2")
    df = df.join(
        markets_long,
        left_on="nonusdc_asset_id",
        right_on="asset_id",
        how="left",
    )

    # 4) label columns and keep market_id
    df = df.with_columns([
        pl.when(pl.col("makerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("makerAsset"),
        pl.when(pl.col("takerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("takerAsset"),
        pl.col("market_id"),
    ])

    df = df[['timestamp', 'market_id', 'maker', 'makerAsset', 'makerAmountFilled', 'taker', 'takerAsset', 'takerAmountFilled', 'transactionHash']]

    df = df.with_columns([
        (pl.col("makerAmountFilled") / 10**6).alias("makerAmountFilled"),
        (pl.col("takerAmountFilled") / 10**6).alias("takerAmountFilled"),
    ])

    df = df.with_columns(
        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.lit("BUY"))
        .otherwise(pl.lit("SELL"))
        .alias("taker_direction")
    )

    df = df.with_columns([
        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.lit("BUY"))
        .otherwise(pl.lit("SELL"))
        .alias("taker_direction"),

        # reverse of taker_direction
        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.lit("SELL"))
        .otherwise(pl.lit("BUY"))
        .alias("maker_direction"),
    ])

    df = df.with_columns([
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


    df = df[['timestamp', 'market_id', 'maker', 'taker', 'nonusdc_side', 'maker_direction', 'taker_direction', 'price', 'usd_amount', 'token_amount', 'transactionHash']]
    return df



def process_live():
    processed_file = 'processed/trades.parquet'

    print("=" * 60)
    print("🔄 Processing Live Trades")
    print("=" * 60)

    last_processed = {}

    if os.path.exists(processed_file):
        print(f"✓ Found existing processed file: {processed_file}")
        existing_trades = pl.read_parquet(processed_file)
        if len(existing_trades) > 0:
            last_row = existing_trades.tail(1)
            last_processed['timestamp'] = last_row['timestamp'][0]
            last_processed['transactionHash'] = last_row['transactionHash'][0]
            last_processed['maker'] = last_row['maker'][0]
            last_processed['taker'] = last_row['taker'][0]

            print(f"📍 Resuming from: {last_processed['timestamp']}")
            print(f"   Last hash: {last_processed['transactionHash'][:16]}...")
    else:
        print("⚠ No existing processed file found - processing from beginning")

    print(f"\n📂 Reading: goldsky/orderFilled.parquet")

    df = pl.read_parquet("goldsky/orderFilled.parquet")
    df = df.with_columns(
        pl.from_epoch(pl.col('timestamp'), time_unit='s').alias('timestamp')
    )

    print(f"✓ Loaded {len(df):,} rows")

    if last_processed:
        df = df.with_row_index()

        same_timestamp = df.filter(pl.col('timestamp') == last_processed['timestamp'])
        same_timestamp = same_timestamp.filter(
            (pl.col("transactionHash") == last_processed['transactionHash']) &
            (pl.col("maker") == last_processed['maker']) &
            (pl.col("taker") == last_processed['taker'])
        )

        if len(same_timestamp) > 0:
            df_process = df.filter(pl.col('index') > same_timestamp.row(0)[0])
            df_process = df_process.drop('index')
        else:
            df_process = df.drop('index')
    else:
        df_process = df

    print(f"⚙️  Processing {len(df_process):,} new rows...")

    new_df = get_processed_df(df_process)

    if not os.path.isdir('processed'):
        os.makedirs('processed')

    op_file = 'processed/trades.parquet'

    if not os.path.isfile(op_file):
        new_df.write_parquet(op_file, compression="zstd")
        print(f"✓ Created new file: processed/trades.parquet")
    else:
        print(f"✓ Appending {len(new_df):,} rows to processed/trades.parquet")
        existing_df = pl.read_parquet(op_file)
        combined_df = pl.concat([existing_df, new_df])
        combined_df.write_parquet(op_file, compression="zstd")

    print("=" * 60)
    print("✅ Processing complete!")
    print("=" * 60)
    
if __name__ == "__main__":
    process_live()