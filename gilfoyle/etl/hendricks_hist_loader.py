"""
Data loader for historical quotes from Hendricks
"""
import os
import sys
import json
import time  # Import time for sleep functionality
from datetime import datetime, timedelta
import requests

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gilfoyle._utils.get_path import get_path
from gilfoyle._utils.load_credentials import load_credentials


def split_tickers(tickers, max_size=3):
    """Split the tickers into sub-lists of a maximum size."""
    return [tickers[i : i + max_size] for i in range(0, len(tickers), max_size)]


def hendricks_hist_loader(job_scope: str = "complete"):
    """
    Data loader for historical quotes from Hendricks
    """
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
    comp_load = data[job_scope]  # This should be a list of ticker symbols

    # Split tickers into sub-lists of max 3 tickers each
    ticker_batches = split_tickers(comp_load)  # Use comp_load directly

    # Set the end date to yesterday
    end_date = (datetime.now() - timedelta(days=1)).strftime(
        "%Y-%m-%dT23:59:59Z"
    )  # Start from yesterday
    cur_yr = int(datetime.strptime(end_date, "%Y-%m-%dT23:59:59Z").year)

    for y in (2016, cur_yr + 1):
        for m in range(1, 13):  # Iterate over each month
            start_date = (
                f"{y}-{m:02d}-01T00:00:00Z"  # Start from the first day of the month
            )
            if m == 12:
                loop_ed = f"{y}-12-31T23:59:59Z"  # End of December
            else:
                loop_ed = f"{y}-{m + 1:02d}-01T00:00:00Z"  # Start of the next month

            # Loop through each batch of tickers
            for ticker_batch in ticker_batches:
                # Prepare the data payload
                data_payload = {
                    "tickers": ticker_batch,
                    "from_date": start_date,
                    "to_date": loop_ed,
                    "collection_name": "rawPriceColl",
                }

                # Define the headers
                headers = {
                    "Content-Type": "application/json",
                    "x-api-key": QT_HENDRICKS_API_KEY,
                }

                # Send the POST request to the Flask server
                try:
                    # TODO: Abstract the endpoint
                    response = requests.post(
                        "https://poederhome.myvnc.com:8001/hendricks/load_quotes/",
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

                time.sleep(2)  # Delay for 2 seconds between iterations
