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

from quantum_trade_utilities.core.get_path import get_path
from quantum_trade_utilities.data.load_credentials import load_credentials


def split_tickers(tickers, max_size=3):
    """Split the tickers into sub-lists of a maximum size."""
    return [tickers[i : i + max_size] for i in range(0, len(tickers), max_size)]


def bachman_analysis(
    job_scope: str = None,
    sources: str = None,
    load_year: int = datetime.now().year,
    target_endpoints: dict = None,
    daily_fmp_flag: bool = None,
    hendricks_endpoint: str = None,
    live_load: bool = None,
    historical_load: bool = None,
    mongo_db: str = None,
    reddit_load: bool = None,
    subreddits: list = None,
    keywords: list = None,
):
    """
    - Load data for the tickers in the job_ctrl file.
    - Ingesting new sources should have a new endpoint for hendricks service
        and, if applicable, endpoint details like fmp_endpoints but assigned
        to a new dict.
    """
    # Send to logging that we are starting the historical news loader
    logging.info(f"Starting Hendricks ETL for {hendricks_endpoint} process...")

    # Check if API key is set
    creds_path = get_path("creds")
    QT_HENDRICKS_API_KEY = load_credentials(creds_path, "hendricks_api")[0]
    if QT_HENDRICKS_API_KEY is None:
        print("Error: QT_HENDRICKS_API_KEY environment variable is not set.")
        return

    # Ticker and time processing handled by Airflow dags.
    cur_scope = job_scope

    # Set the start and end dates for the year
    start_date = datetime(load_year, 1, 1)
    end_date = datetime(load_year, 12, 31)
    if load_year == datetime.now().year:
        start_date = datetime(load_year, 1, 1)
        end_date = datetime(load_year, datetime.now().month, datetime.now().day)

    # If live load is True, set the start and end dates to the current date
    if live_load is True:
        start_date = datetime.now()
        end_date = datetime.now()

    # convert times to ISO format
    start_date = start_date.strftime("%Y-%m-%dT00:00:00Z")
    end_date = end_date.strftime("%Y-%m-%dT23:59:59Z")

    print(f"Start date: {start_date}")
    print(f"End date: {end_date}")

    # Loop through each ticker
    for ticker in cur_scope:
        if hendricks_endpoint == "load_news":
            gridfs_bucket = f"{ticker}_news"
            ep_loop = {"N/A": "rawNews"}
        elif hendricks_endpoint == "load_quotes":
            ep_loop = {"N/A": "rawQuotes"}
            gridfs_bucket = None
        else:
            gridfs_bucket = None
            ep_loop = target_endpoints

        for ep, coll in ep_loop.items():
            logging.info(f"Processing ticker: {ticker}")
            logging.info(f"Initiating Hendricks endpoint: {hendricks_endpoint}")
            logging.info(f"Processing target endpoint: {ep}")
            logging.info(f"Start date: {start_date}")
            logging.info(f"End date: {end_date}")

            metadata_collection = f"{ticker}_{coll}"

            data_payload = {
                "tickers": [ticker],
                "from_date": start_date,
                "to_date": end_date,
                "collection_name": metadata_collection,
                "gridfs_bucket": gridfs_bucket,
                "sources": sources,
                "target_endpoint": ep,
                "daily_fmp_flag": daily_fmp_flag,
                "mongo_db": mongo_db,
                "reddit_load": reddit_load,
                "subreddits": subreddits,
                "keywords": keywords,
            }

            # Define the headers
            headers = {
                "Content-Type": "application/json",
                "x-api-key": QT_HENDRICKS_API_KEY,
            }

            endpoint = f"http://127.0.0.1:8711/hendricks/{hendricks_endpoint}"

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

        logging.info(f"Completed Hendricks ETL for {hendricks_endpoint} process...")

    return response.text
