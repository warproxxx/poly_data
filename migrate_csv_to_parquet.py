#!/usr/bin/env python3
"""
Migration script to convert existing CSV files to Parquet format.

This script helps users transition from the old CSV-based data storage
to the new Parquet format, which offers better performance and compression.

Usage:
    python migrate_csv_to_parquet.py
"""

import os
import polars as pl
from pathlib import Path


def migrate_file(csv_path: str, parquet_path: str, schema_overrides: dict = None):
    """
    Migrate a single CSV file to Parquet format.

    Args:
        csv_path: Path to source CSV file
        parquet_path: Path to destination Parquet file
        schema_overrides: Optional schema overrides for CSV reading
    """
    if not os.path.exists(csv_path):
        print(f"⚠️  CSV file not found: {csv_path} - Skipping")
        return False

    if os.path.exists(parquet_path):
        response = input(f"⚠️  Parquet file already exists: {parquet_path}. Overwrite? (y/N): ")
        if response.lower() != 'y':
            print(f"Skipping {csv_path}")
            return False

    print(f"📄 Migrating: {csv_path} → {parquet_path}")

    try:
        # Read CSV
        if schema_overrides:
            df = pl.read_csv(csv_path, schema_overrides=schema_overrides)
        else:
            df = pl.read_csv(csv_path)

        print(f"   Loaded {len(df):,} rows from CSV")

        # Write Parquet with compression
        df.write_parquet(parquet_path, compression="zstd")

        # Get file sizes
        csv_size = os.path.getsize(csv_path) / (1024 * 1024)  # MB
        parquet_size = os.path.getsize(parquet_path) / (1024 * 1024)  # MB
        compression_ratio = (1 - parquet_size / csv_size) * 100

        print(f"   ✓ Created Parquet file")
        print(f"   📊 CSV: {csv_size:.2f} MB → Parquet: {parquet_size:.2f} MB")
        print(f"   💾 Space saved: {compression_ratio:.1f}%")

        return True

    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def main():
    print("=" * 60)
    print("CSV to Parquet Migration Tool")
    print("=" * 60)
    print()

    # Define files to migrate with their schema overrides
    migrations = [
        {
            'csv': 'markets.csv',
            'parquet': 'markets.parquet',
            'schema': {'token1': pl.Utf8, 'token2': pl.Utf8}
        },
        {
            'csv': 'missing_markets.csv',
            'parquet': 'missing_markets.parquet',
            'schema': {'token1': pl.Utf8, 'token2': pl.Utf8}
        },
        {
            'csv': 'goldsky/orderFilled.csv',
            'parquet': 'goldsky/orderFilled.parquet',
            'schema': {'makerAssetId': pl.Utf8, 'takerAssetId': pl.Utf8}
        },
        {
            'csv': 'processed/trades.csv',
            'parquet': 'processed/trades.parquet',
            'schema': None
        }
    ]

    total_csv_size = 0
    total_parquet_size = 0
    successful_migrations = 0

    for migration in migrations:
        csv_path = migration['csv']
        parquet_path = migration['parquet']
        schema = migration['schema']

        if migrate_file(csv_path, parquet_path, schema):
            successful_migrations += 1
            if os.path.exists(csv_path):
                total_csv_size += os.path.getsize(csv_path)
            if os.path.exists(parquet_path):
                total_parquet_size += os.path.getsize(parquet_path)

        print()

    print("=" * 60)
    print("Migration Summary")
    print("=" * 60)
    print(f"✓ Successfully migrated: {successful_migrations} files")

    if total_csv_size > 0:
        total_csv_mb = total_csv_size / (1024 * 1024)
        total_parquet_mb = total_parquet_size / (1024 * 1024)
        total_saved = (1 - total_parquet_size / total_csv_size) * 100

        print(f"📊 Total CSV size: {total_csv_mb:.2f} MB")
        print(f"📊 Total Parquet size: {total_parquet_mb:.2f} MB")
        print(f"💾 Total space saved: {total_saved:.1f}%")

    print()
    print("Next steps:")
    print("1. Verify the Parquet files contain the expected data")
    print("2. Run your pipeline to confirm it works with Parquet")
    print("3. Once verified, you can safely delete the old CSV files")
    print()


if __name__ == "__main__":
    main()
