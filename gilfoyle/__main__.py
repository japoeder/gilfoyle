"""
Main entry point for Gilfoyle.
"""

import sys
import os
import signal
import logging
from functools import wraps
from datetime import datetime
from flask import Flask, request, jsonify
from dotenv import load_dotenv

load_dotenv()

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gilfoyle.etl.ticker_expansion import ticker_expansion
from gilfoyle.etl.run_etl import RunEtl
from gilfoyle._utils.logging_config import setup_logging
from gilfoyle._utils.get_path import get_path
from gilfoyle._utils.load_credentials import load_credentials


app = Flask(__name__)


def handle_sigterm(*args):
    """Handle SIGTERM signal."""
    print("Received SIGTERM, shutting down gracefully...")
    # Perform any cleanup here
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_sigterm)

# Set up logging configuration
setup_logging()

# Check if API key is set
creds_path = get_path("creds")
QT_GILFOYLE_API_KEY = load_credentials(creds_path, "gilfoyle_api")[0]
if QT_GILFOYLE_API_KEY is None:
    print("Error: QT_GILFOYLE_API_KEY environment variable is not set.")


def requires_api_key(f):
    """Decorator to require an API key for a route."""

    @wraps(f)
    def decorated(*args, **kwargs):
        try:
            api_key = request.headers.get("x-api-key")
            if not api_key:
                return (
                    jsonify(
                        {
                            "status": "error",
                            "message": "API key is missing",
                            "error_type": "authentication",
                        }
                    ),
                    401,
                )

            if api_key != QT_GILFOYLE_API_KEY:
                logging.warning(f"Invalid API key attempt: {api_key[:8]}...")
                return (
                    jsonify(
                        {
                            "status": "error",
                            "message": "Invalid API key",
                            "error_type": "authentication",
                        }
                    ),
                    403,
                )

            return f(*args, **kwargs)

        except Exception as e:
            logging.error(f"Authentication error: {str(e)}")
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Authentication system error",
                        "error_type": "system",
                    }
                ),
                500,
            )

    return decorated


# TODO: update to return meta data in response (can prob. use timestame to control)
@app.route("/gilfoyle/etl_quote_expansion", methods=["POST"])
@requires_api_key
def etl_quote_expansion():
    """Endpoint to load a new stock ticker into the database."""
    data = request.json
    ticker_symbol = data.get("ticker_symbol")
    collection_name = data.get("collection_name", "rawPriceColl")
    ticker_expansion()
    return (
        jsonify(
            {
                "status": f"{ticker_symbol} dataframe loaded into {collection_name} collection."
            }
        ),
        202,
    )


@app.route("/gilfoyle/etl_quote_loader", methods=["POST"])
@requires_api_key
def etl_quote_loader():
    """Endpoint to load a new stock ticker into the database."""
    try:
        load_type = None
        data = request.json
        live_load = data.get("live_load")
        historical_load = data.get("historical_load")
        job_scope = data.get("job_scope")
        load_year = data.get("load_year")

        if not load_year:
            load_year = datetime.now().year

        etl_obj = RunEtl(
            live_load=live_load,
            historical_load=historical_load,
            job_scope=job_scope,
            load_year=load_year,
        )
        if not job_scope:
            job_scope = "comp_load"

        etl_obj.initiate_hendricks_quote_load()

        if live_load:
            load_type = "live"
        elif historical_load:
            load_type = "historical"

        return (
            jsonify(
                {"status": f"Hendricks quote loader completed for {load_type} load."}
            ),
            202,
        )
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return (
            jsonify({"status": "error", "message": str(e)}),
            500,
        )


@app.route("/gilfoyle/etl_news_loader", methods=["POST"])
@requires_api_key
def etl_news_loader():
    """Endpoint to load a new stock ticker into the database."""
    try:
        load_type = None
        data = request.json
        live_load = data.get("live_load")
        historical_load = data.get("historical_load")
        load_year = data.get("load_year")

        if not load_year:
            load_year = datetime.now().year

        job_scope = data.get("job_scope")
        if not job_scope:
            job_scope = "comp_load"

        sources = data.get("sources")
        if not sources:
            sources = ["fmp"]
        logging.info(f"Passing sources: {sources}")

        etl_obj = RunEtl(
            live_load=live_load,
            historical_load=historical_load,
            job_scope=job_scope,
            sources=sources,
            load_year=load_year,
        )

        results = etl_obj.initiate_hendricks_news_load()

        if live_load:
            load_type = "live"
        elif historical_load:
            load_type = "historical"

        return (
            jsonify(
                {
                    "status": f"Hendricks news loader completed for {load_type} load.",
                    "successful_sources": results["successful_sources"],
                    "failed_sources": results["failed_sources"],
                }
            ),
            202,
        )
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return (
            jsonify({"status": "error", "message": str(e)}),
            500,
        )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8002)
