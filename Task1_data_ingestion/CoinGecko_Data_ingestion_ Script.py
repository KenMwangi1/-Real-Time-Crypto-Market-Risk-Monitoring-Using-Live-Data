import requests
import pandas as pd
import time
import os
from datetime import datetime, timezone

# =========================
# CONFIGURATION
# =========================

API_URL = "https://api.coingecko.com/api/v3/coins/markets"

PARAMS = {
    "vs_currency": "usd",
    "ids": "bitcoin,ethereum,binancecoin,solana",
    "order": "market_cap_desc",
    "per_page": 100,
    "page": 1,
    "sparkline": "false"
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "raw_prices.csv")

FETCH_INTERVAL_SECONDS = 300  # 5 minutes

HEADERS = {
    "User-Agent": "market-risk-monitor/1.0"
}

# =========================
# FETCH FUNCTION
# =========================

def fetch_market_snapshot():
    response = requests.get(
        API_URL,
        params=PARAMS,
        headers=HEADERS,
        timeout=10
    )
    response.raise_for_status()
    data = response.json()

    timestamp = datetime.now(timezone.utc)

    records = []
    for coin in data:
        records.append({
            "asset_id": coin["id"],
            "timestamp": timestamp,
            "price_usd": coin["current_price"],
            "volume_usd": coin["total_volume"],
            "market_cap_usd": coin["market_cap"]
        })

    return pd.DataFrame(records)

# =========================
# SAFE APPEND FUNCTION
# =========================

def append_to_csv(df_new):
    """
    Append new records to CSV safely.
    Never overwrites existing data.
    """
    file_exists = os.path.exists(OUTPUT_FILE)

    df_new.to_csv(
        OUTPUT_FILE,
        mode="a",                 # append mode
        header=not file_exists,   # write header only once
        index=False
    )

# =========================
# MAIN LOOP
# =========================

def run_pipeline():
    print("Starting market data ingestion...")
    print(f"Saving data to: {OUTPUT_FILE}")
    print("Fetching data every 5 minutes.\n")

    try:
        while True:
            df_snapshot = fetch_market_snapshot()
            append_to_csv(df_snapshot)

            print(
                f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC] "
                f"Fetched {len(df_snapshot)} records."
            )

            time.sleep(FETCH_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nPipeline stopped gracefully.")

    except Exception as e:
        print(f"FATAL ERROR: {e}")

# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    run_pipeline()
