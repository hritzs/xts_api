"""
Script to check and fix parity between the local database and the broker's order book.

This script calls an API endpoint that triggers a manual synchronization process
for a specific trade. It finds orders that exist at the broker but are missing
from the local DB (and vice-versa) and updates the DB to match reality.

Usage:
    python check_parity.py <your_trade_uid>

Example:
    python check_parity.py ny240724100000a
"""
import argparse
import requests
import json
import config  # Assumes config.py has HOST and PORT

def check_trade_parity(trade_uid: str):
    """
    Calls the API to trigger a manual sync for a given trade_uid.
    """
    host = getattr(config, 'HOST', '127.0.0.1')
    port = getattr(config, 'PORT', 5000)

    # Handle '0.0.0.0' host for requests, as it's not a valid target address
    if host == '0.0.0.0':
        host = '127.0.0.1'

    url = f"http://{host}:{port}/api/trade/sync/{trade_uid}"

    print(f"Checking parity for trade: {trade_uid}")
    print(f"Contacting API endpoint: {url}")

    try:
        response = requests.post(url, timeout=30)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

        result = response.json()

        print("\n--- Sync Result ---")
        print(json.dumps(result, indent=2))

    except requests.exceptions.RequestException as e:
        print(f"\n❌ Error contacting the API: {e}")
        print("   Please ensure the main application server is running.")
    except json.JSONDecodeError:
        print("\n❌ Error: Failed to decode JSON response from the server.")
        print(f"   Response Text: {response.text}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check and fix parity between the local database and the broker's order book for a specific trade."
    )
    parser.add_argument(
        "trade_uid",
        type=str,
        help="The unique ID of the trade to check (e.g., 'ny240724100000a')."
    )

    args = parser.parse_args()
    check_trade_parity(args.trade_uid)