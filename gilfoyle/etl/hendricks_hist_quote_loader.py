"""
Data loader for historical quotes from Hendricks
"""
import os
import sys
import json
from datetime import datetime
import logging
import random
import requests

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gilfoyle._utils.get_path import get_path
from gilfoyle._utils.load_credentials import load_credentials


def split_tickers(tickers, max_size=3):
    """Split the tickers into sub-lists of a maximum size."""
    return [tickers[i : i + max_size] for i in range(0, len(tickers), max_size)]


def hendricks_hist_quote_loader(
    job_scope: str = "complete", load_year: int = datetime.now().year
):
    """
    Data loader for historical quotes from Hendricks
    """
    logging.info("Starting Hendricks historical quote loader method...")

    # Check if API key is set
    creds_path = get_path("creds")
    QT_HENDRICKS_API_KEY = load_credentials(creds_path, "hendricks_api")[0]
    if QT_HENDRICKS_API_KEY is None:
        print("Error: QT_HENDRICKS_API_KEY environment variable is not set.")
        return

    # Get ticker symbols from the JSON file
    job_ctrl_path = get_path("job_ctrl")
    with open(job_ctrl_path, encoding="utf-8") as f:
        data = json.load(f)
    cur_scope = data[job_scope]  # This should be a list of ticker symbols

    # Randomize current scope
    cur_scope = random.sample(cur_scope, len(cur_scope))

    # Set the dates for the specified year
    start_date = datetime(load_year, 1, 1)
    end_date = datetime(load_year, 12, 31)

    print(f"Start date: {start_date}")
    print(f"End date: {end_date}")

    # Convert times to ISO format
    start_date = start_date.strftime("%Y-%m-%dT00:00:00Z")
    end_date = end_date.strftime("%Y-%m-%dT23:59:59Z")

    # Loop through each ticker
    for ticker in cur_scope:
        logging.info(f"Processing ticker: {ticker}")
        # Create collection name with ticker prefix
        collection_name = f"{ticker}_rawQuotes"

        # Prepare the data payload
        data_payload = {
            "tickers": [ticker],
            "from_date": start_date,
            "to_date": end_date,
            "collection_name": collection_name,
        }

        # Define the headers
        headers = {
            "Content-Type": "application/json",
            "x-api-key": QT_HENDRICKS_API_KEY,
        }

        endpoint = "http://localhost:8001/hendricks/load_quotes"

        # Send the POST request to the Flask server
        try:
            response = requests.post(
                endpoint,
                json=data_payload,
                headers=headers,
                timeout=120000,
            )
            response.raise_for_status()

            # Print the response from the server
            print(f"Response Status Code for {ticker}: {response.status_code}")
            print(f"Response Text for {ticker}: {response.text}")

        except requests.exceptions.RequestException as e:
            logging.error(f"Error processing ticker {ticker}: {str(e)}")
            continue

    logging.info("Completed Hendricks historical quote loader method.")
