"""
Data loader for Hendricks live quote data.
"""
import os
import sys
import json
import shutil

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


class ResponseObj:
    """
    Response object for the file embeddings process.
    """

    def __init__(self, text: dict):
        self.text = text


def file_embeddings(
    source: str = None,
    doc_type: str = None,
    entity_type: str = None,
    device: str = None,
):
    """
    - Load data for the tickers in the job_ctrl file.
    - Ingesting new sources should have a new endpoint for hendricks service
        and, if applicable, endpoint details like fmp_endpoints but assigned
        to a new dict.
    """

    # Send to logging that we are starting the historical news loader
    logging.info("Starting Bachman file embeddings process...")

    # Check if API key is set
    creds_path = get_path("creds")
    QT_BACHMAN_API_KEY = load_credentials(creds_path, "bachman_api")[0]
    if QT_BACHMAN_API_KEY is None:
        print("Error: QT_BACHMAN_API_KEY environment variable is not set.")
        return

    # Get doc path for file processing
    doc_workspace = get_path("doc_workspace")
    embed_path = os.path.join(doc_workspace, "embed")
    analyze_path = os.path.join(doc_workspace, "analyze")
    # complete_path = os.path.join(doc_workspace, "complete")
    tmp_path = os.path.join(doc_workspace, "tmp")

    # create a list of files in the doc_workspace
    embed_files = os.listdir(embed_path)

    # Remove unwanted .DS_Store files
    embed_files = [file for file in embed_files if not file.startswith(".DS_Store")]

    # create a list of files in the doc_workspace
    # analyze_files = os.listdir(analyze_path)
    # complete_files = os.listdir(complete_path)

    for file in embed_files:
        name_parts = (file.split(".")[0]).split("_")
        doc_id = file.split(".")[0]
        ticker = name_parts[0]
        report_name = name_parts[1]
        fiscal_period = name_parts[2]
        publish_date = name_parts[3]

        # Deconstruct the fiscal period
        fiscal_year = int(fiscal_period[:4])
        period_type = fiscal_period[4:]

        # Convert the publish date to a datetime object
        publish_year = int(publish_date[:4])
        publish_month = int(publish_date[4:6])
        publish_day = int(publish_date[6:8])

        # Convert the publish date to a datetime object
        publish_date = datetime(publish_year, publish_month, publish_day)

        # Process file endpoint
        bachman_endpoint = "process_file"

        logging.info(f"Processing ticker: {ticker}")
        logging.info(f"Initiating Bachman endpoint: {bachman_endpoint}")

        data_payload = {
            "file_path": os.path.join(embed_path, file),
            "temp_dir": tmp_path,
            "collection_name": doc_type,
            "cleanup": True,
            "skip_if_exists": True,
            "metadata": {
                "embedding_config": {
                    "vectorstore": {
                        "provider": "huggingface",
                        "model": "BAAI/bge-large-en-v1.5",
                    },
                    "device": device,
                },
                "source": source,
                "doc_id": doc_id,
                "doc_type": doc_type,
                "ticker": ticker,
                "e_type": entity_type,
                "report_name": report_name,
                "fiscal_year": fiscal_year,
                "period_type": period_type,
                "publish_date": str(publish_date),
            },
        }

        # Define the headers
        headers = {
            "Content-Type": "application/json",
            "x-api-key": QT_BACHMAN_API_KEY,
        }

        endpoint = f"http://127.0.0.1:8713/bachman/{bachman_endpoint}"

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

            # Move the file to the analyze directory
            shutil.move(
                os.path.join(embed_path, file), os.path.join(analyze_path, file)
            )

        except requests.exceptions.RequestException as e:
            logging.error(f"Error processing ticker {ticker}: {str(e)}")
            continue

    logging.info("Completed Bachman file embeddings process...")

    text = {
        "status": "success",
        "message": "File embeddings process completed successfully.",
        "failed_tickers": [],
    }
    response = ResponseObj(text=json.dumps(text))

    return response.text
