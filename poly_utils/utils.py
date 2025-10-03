import os
import csv
import json
import requests
import time
from typing import List
import polars as pl

PLATFORM_WALLETS = ['0xc5d563a36ae78145c45a50134d48a1215220f80a', '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e']


def get_markets(main_file: str = "markets.csv", missing_file: str = "missing_markets.csv"):
    """
    Load and combine markets from both files, deduplicate, and sort by createdAt
    Returns combined Polars DataFrame sorted by creation date
    """
    import polars as pl
    
    # Schema overrides for long token IDs
    schema_overrides = {
        "token1": pl.Utf8,      # 76-digit ids → strings
        "token2": pl.Utf8,
    }
    
    dfs = []
    
    # Load main markets file
    if os.path.exists(main_file):
        main_df = pl.scan_csv(main_file, schema_overrides=schema_overrides).collect(streaming=True)
        dfs.append(main_df)
        print(f"Loaded {len(main_df)} markets from {main_file}")
    
    # Load missing markets file
    if os.path.exists(missing_file):
        missing_df = pl.scan_csv(missing_file, schema_overrides=schema_overrides).collect(streaming=True)
        dfs.append(missing_df)
        print(f"Loaded {len(missing_df)} markets from {missing_file}")
    
    if not dfs:
        print("No market files found!")
        return pl.DataFrame()
    
    # Combine, deduplicate, and sort
    combined_df = (
        pl.concat(dfs)
        .unique(subset=['id'], keep='first')
        .sort('createdAt')
    )
    
    print(f"Combined total: {len(combined_df)} unique markets (sorted by createdAt)")
    return combined_df


def update_missing_tokens(missing_token_ids: List[str], csv_filename: str = "missing_markets.csv"):
    """
    Fetch market data for missing token IDs and save to separate CSV file
    
    Args:
        missing_token_ids: List of token IDs to fetch
        csv_filename: CSV file to save missing markets (default: missing_markets.csv)
    """
    if not missing_token_ids:
        print("No missing tokens to fetch")
        return
    
    print(f"Fetching {len(missing_token_ids)} missing tokens...")
    
    # Same headers as main markets.csv
    headers = [
        'createdAt', 'id', 'question', 'answer1', 'answer2', 'neg_risk', 
        'market_slug', 'token1', 'token2', 'condition_id', 'volume', 'ticker', 'closedTime'
    ]
    
    # Check if file exists to determine if we need headers
    file_exists = os.path.exists(csv_filename)
    
    new_markets = []
    processed_market_ids = set()
    
    # If file exists, read existing market IDs to avoid duplicates
    if file_exists:
        try:
            with open(csv_filename, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('id'):
                        processed_market_ids.add(row['id'])
            print(f"Found {len(processed_market_ids)} existing markets in {csv_filename}")
        except Exception as e:
            print(f"Error reading existing file: {e}")
    
    for token_id in missing_token_ids:
        print(f"Fetching market for token: {token_id}")
        
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                response = requests.get(
                    'https://gamma-api.polymarket.com/markets',
                    params={'clob_token_ids': token_id},
                    timeout=30
                )
                
                if response.status_code == 429:
                    print(f"Rate limited - waiting 10 seconds...")
                    time.sleep(10)
                    continue
                elif response.status_code != 200:
                    print(f"API error {response.status_code} for token {token_id}")
                    retry_count += 1
                    time.sleep(2)
                    continue
                
                markets = response.json()
                
                if not markets:
                    print(f"No market found for token {token_id}")
                    break
                
                market = markets[0]
                market_id = market.get('id', '')
                
                # Skip if we already have this market
                if market_id in processed_market_ids:
                    print(f"Market {market_id} already exists - skipping")
                    break
                
                # Parse clobTokenIds
                clob_tokens_str = market.get('clobTokenIds', '[]')
                if isinstance(clob_tokens_str, str):
                    clob_tokens = json.loads(clob_tokens_str)
                else:
                    clob_tokens = clob_tokens_str
                
                if len(clob_tokens) < 2:
                    print(f"Invalid token data for {token_id}")
                    break
                
                token1, token2 = clob_tokens[0], clob_tokens[1]
                
                # Parse outcomes
                outcomes_str = market.get('outcomes', '[]')
                if isinstance(outcomes_str, str):
                    outcomes = json.loads(outcomes_str)
                else:
                    outcomes = outcomes_str
                
                answer1 = outcomes[0] if len(outcomes) > 0 else 'YES'
                answer2 = outcomes[1] if len(outcomes) > 1 else 'NO'
                
                # Check for negative risk
                neg_risk = market.get('negRiskAugmented', False) or market.get('negRiskOther', False)
                
                # Get ticker from events if available
                ticker = ''
                if market.get('events') and len(market.get('events', [])) > 0:
                    ticker = market['events'][0].get('ticker', '')
                
                question_text = market.get('question', '') or market.get('title', '')
                
                # Create market row
                row = [
                    market.get('createdAt', ''),
                    market_id,
                    question_text,
                    answer1,
                    answer2,
                    neg_risk,
                    market.get('slug', ''),
                    token1,
                    token2,
                    market.get('conditionId', ''),
                    market.get('volume', ''),
                    ticker,
                    market.get('closedTime', '')
                ]
                
                new_markets.append(row)
                processed_market_ids.add(market_id)
                print(f"Successfully fetched market {market_id} for token {token_id}")
                break
                
            except Exception as e:
                print(f"Error fetching token {token_id}: {e}")
                retry_count += 1
                time.sleep(2)
        
        if retry_count >= max_retries:
            print(f"Failed to fetch token {token_id} after {max_retries} retries")
        
        # Small delay between requests
        time.sleep(0.5)
    
    if not new_markets:
        print("No new markets to add")
        return
    
    # Write new markets to file
    mode = 'a' if file_exists else 'w'
    with open(csv_filename, mode, newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write headers only if new file
        if not file_exists:
            writer.writerow(headers)
        
        writer.writerows(new_markets)
    
    print(f"Added {len(new_markets)} new markets to {csv_filename}")
    print(f"Total markets now in file: {len(processed_market_ids)}")

