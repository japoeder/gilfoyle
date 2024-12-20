"""
Data loader for Hendricks live quote data.
"""
import os
import sys
import json
from datetime import datetime
import requests

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gilfoyle._utils.get_path import get_path
from gilfoyle._utils.load_credentials import load_credentials


def hendricks_live_loader(job_scope: str = "complete"):
    """
    Load live quote data for the tickers in the job_ctrl file.
    """
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
    comp_load = job[job_scope]  # This should be a list of ticker symbols

    # Split tickers into sub-lists of max 3 tickers each
    tickers = comp_load  # Use comp_load directly

    # Set the current date
    # TODO: Need to go through every timestamp and make sure it's in UTC
    current_date = datetime.now().strftime(
        "%Y-%m-%dT00:00:00Z"
    )  # Start from the current day
    end_date = datetime.now().strftime("%Y-%m-%dT23:59:59Z")  # End of the current day

    # Prepare the data payload
    data = {
        "tickers": tickers,
        "from_date": current_date,
        "to_date": end_date,
        "collection_name": "rawPriceColl",
    }

    # Define the headers
    headers = {"Content-Type": "application/json", "x-api-key": QT_HENDRICKS_API_KEY}

    endpoint = "http://localhost:8001/hendricks/load_quotes"

    # Send the POST request to the Flask server
    try:
        response = requests.post(
            endpoint,
            json=data,
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
