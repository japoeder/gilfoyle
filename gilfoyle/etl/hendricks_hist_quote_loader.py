"""
Data loader for historical quotes from Hendricks
"""
import os
import sys
import json
from datetime import datetime, timedelta
import logging
import requests

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gilfoyle._utils.get_path import get_path
from gilfoyle._utils.load_credentials import load_credentials


def split_tickers(tickers, max_size=3):
    """Split the tickers into sub-lists of a maximum size."""
    return [tickers[i : i + max_size] for i in range(0, len(tickers), max_size)]


def hendricks_hist_quote_loader(job_scope: str = "complete"):
    """
    Data loader for historical quotes from Hendricks
    """
    # Send to logging that we are starting the historical quote loader
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

    # Set the end date to yesterday and start date to 2016
    start_date = datetime(2016, 1, 1)
    end_date = datetime.now() - timedelta(days=1)
    # end_date = datetime(2016, 12, 31)

    print(f"Start date: {start_date}")
    print(f"End date: {end_date}")

    # convert times to 2024-11-01T00:00:00Z format
    start_date = start_date.strftime("%Y-%m-%dT00:00:00Z")
    end_date = end_date.strftime("%Y-%m-%dT23:59:59Z")

    # cur_scope = ["AAPL"]
    # Loop through each batch of tickers
    for ticker in cur_scope:
        # Prepare the data payload
        data_payload = {
            "tickers": [ticker],
            "from_date": start_date,
            "to_date": end_date,
            "collection_name": "rawPriceColl",
        }

        # Define the headers
        headers = {
            "Content-Type": "application/json",
            "x-api-key": QT_HENDRICKS_API_KEY,
        }

        endpoint = "http://localhost:8001/hendricks/load_quotes"

        # Send the POST request to the Flask server
        try:
            # TODO: Abstract the endpoint
            response = requests.post(
                endpoint,
                json=data_payload,
                headers=headers,
                timeout=6000,
            )  # 10 seconds timeout
            response.raise_for_status()  # Raise an error for bad responses (4xx or 5xx)

            # Print the response from the server
            print("Response Status Code:", response.status_code)
            print("Response Text:", response.text)

            # Optionally print the JSON response
            try:
                print("JSON Response:", response.json())
            except ValueError:
                print("Response is not in JSON format.")
        except requests.exceptions.HTTPError as err:
            print(f"HTTP error occurred: {err}")
        except requests.exceptions.Timeout:
            print("The request timed out")
        except requests.exceptions.RequestException as err:
            print(f"An error occurred: {err}")
