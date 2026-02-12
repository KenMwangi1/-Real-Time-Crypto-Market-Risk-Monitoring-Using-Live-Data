import requests
import pandas as pd
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

OUTPUT_FILE = "raw_prices.csv"

HEADERS = {
    "User-Agent": "market-risk-monitor/1.0"
}

# =========================
# FETCH DATA
# =========================

def fetch_market_snapshot():
    response = requests.get(
        API_URL,
        params=PARAMS,
        headers=HEADERS,
        timeout=15
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
    Append new records safely.
    Creates file if missing.
    Avoids total overwrite.
    """

    file_exists = os.path.exists(OUTPUT_FILE)

    try:
        df_new.to_csv(
            OUTPUT_FILE,
            mode="a",
            header=not file_exists,
            index=False
        )
    except Exception as e:
        print(f"ERROR writing to CSV: {e}")
        raise

# =========================
# MAIN EXECUTION
# =========================

def main():
    print("Starting single-run ingestion...")

    df_snapshot = fetch_market_snapshot()

    if df_snapshot.empty:
        print("No data returned from API.")
        return

    append_to_csv(df_snapshot)

    print(
        f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC] "
        f"Fetched {len(df_snapshot)} records successfully."
    )

if __name__ == "__main__":
    main()
