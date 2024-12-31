"""
Data loader for Hendricks live quote data.
"""
import os
import sys
import json
from datetime import datetime
import logging
import requests

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gilfoyle._utils.get_path import get_path
from gilfoyle._utils.load_credentials import load_credentials


def hendricks_live_findata_loader(
    job_scope: str = None,
    sources: str = None,
    endpoints: dict = None,
    daily_flag: bool = None,
):
    """
    Load historical financial data for the tickers in the job_ctrl file.
    """
    # Send to logging that we are starting the historical news loader
    logging.info("Starting Hendricks live financial data loader method...")

    # Check if API key is set
    creds_path = get_path("creds")
    QT_HENDRICKS_API_KEY = load_credentials(creds_path, "hendricks_api")[0]
    if QT_HENDRICKS_API_KEY is None:
        print("Error: QT_HENDRICKS_API_KEY environment variable is not set.")
        return

    # Get ticker symbols from the JSON file
    job_ctrl_path = get_path("job_ctrl")
    with open(job_ctrl_path, "r", encoding="utf-8") as f:
        job = json.load(f)
    cur_scope = job[job_scope]  # This should be a list of ticker symbols

    # Set the current date in UTC
    current_date = datetime.now().strftime("%Y-%m-%dT00:00:00Z")
    end_date = datetime.now().strftime("%Y-%m-%dT23:59:59Z")

    print(f"Start date: {current_date}")
    print(f"End date: {end_date}")

    # Loop through each ticker
    for ticker in cur_scope:
        for ep, coll in endpoints.items():
            logging.info(f"Processing ticker: {ticker}")
            logging.info(f"Processing endpoint: {ep}")
            logging.info(f"Start date: {current_date}")
            logging.info(f"End date: {end_date}")

            metadata_collection = f"{ticker}_{coll}"

            data_payload = {
                "tickers": [ticker],
                "from_date": current_date,
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
