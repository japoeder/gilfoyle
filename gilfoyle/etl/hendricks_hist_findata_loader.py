"""
Data loader for Hendricks live quote data.
"""
import os
import sys

from datetime import datetime
import logging
import requests

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gilfoyle._utils.get_path import get_path
from gilfoyle._utils.load_credentials import load_credentials


def split_tickers(tickers, max_size=3):
    """Split the tickers into sub-lists of a maximum size."""
    return [tickers[i : i + max_size] for i in range(0, len(tickers), max_size)]


def hendricks_hist_findata_loader(
    job_scope: str = None,
    sources: str = None,
    load_year: int = None,
    endpoints: dict = None,
    daily_flag: bool = None,
):
    """
    Load historical financial data for the tickers in the job_ctrl file.
    """
    # Send to logging that we are starting the historical news loader
    logging.info("Starting Hendricks historical financial data loader method...")

    # Check if API key is set
    creds_path = get_path("creds")
    QT_HENDRICKS_API_KEY = load_credentials(creds_path, "hendricks_api")[0]
    if QT_HENDRICKS_API_KEY is None:
        print("Error: QT_HENDRICKS_API_KEY environment variable is not set.")
        return

    # Ticker and time processing handled by Airflow dags.
    cur_scope = job_scope

    if daily_flag is True:
        if load_year is None:
            load_year = datetime.now().year

    start_date = datetime(load_year, 1, 1)
    end_date = datetime(load_year, 12, 31)

    # convert times to ISO format
    start_date = start_date.strftime("%Y-%m-%dT00:00:00Z")
    end_date = end_date.strftime("%Y-%m-%dT23:59:59Z")

    print(f"Start date: {start_date}")
    print(f"End date: {end_date}")

    # Loop through each ticker
    for ticker in cur_scope:
        for ep, coll in endpoints.items():
            logging.info(f"Processing ticker: {ticker}")
            logging.info(f"Processing endpoint: {ep}")
            logging.info(f"Start date: {start_date}")
            logging.info(f"End date: {end_date}")

            metadata_collection = f"{ticker}_{coll}"

            data_payload = {
                "tickers": [ticker],
                "from_date": start_date,
                "to_date": end_date,
                "collection_name": metadata_collection,
                "sources": sources,
                "endpoint": ep,
                "daily_flag": daily_flag,
            }

            # Define the headers
            headers = {
                "Content-Type": "application/json",
                "x-api-key": QT_HENDRICKS_API_KEY,
            }

            endpoint = "http://localhost:8001/hendricks/load_fin_data"

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

        logging.info("Completed running Hendricks historical financial data loader.")
