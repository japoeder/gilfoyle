from flask import Flask, request, jsonify
from dotenv import load_dotenv
load_dotenv()
import sys
import os
# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gilfoyle.ticker_expansion import ticker_expansion
import logging
import pandas as pd
import argparse
import asyncio


app = Flask(__name__)
# Configure logging
# logging.basicConfig(filename=f'app.log', level=logging.DEBUG,
#                     format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')

# # Add console handler for logging
# console_handler = logging.StreamHandler()
# console_handler.setLevel(logging.DEBUG)
# formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')
# console_handler.setFormatter(formatter)
# logging.getLogger().addHandler(console_handler)

# logging.debug("This is a test log message.")

#TODO: update to return meta data in response (can prob. use timestame to control)
@app.route("/method_1", methods=["POST"])
def run_expansion():
    """Endpoint to load a new stock ticker into the database."""
    data = request.json
    ticker_symbol = data.get("ticker_symbol")
    collection_name = data.get("collection_name", "rawPriceColl")
    ticker_expansion()
    return jsonify({"status": f"{ticker_symbol} dataframe loaded into {collection_name} collection."}), 202



if __name__ == "__main__":
    #app.run(debug=True, host="0.0.0.0", port=8001)
    ticker_expansion()
    # load_historical_quote_alpacaAPI(
    #     ticker_symbol='AAPL',
    #     collection_name='rawPriceColl',
    #     from_date='2017-03-25T00:00',
    #     to_date='2017-04-3T23:59',
    #     batch_size=7500,
    #     creds_file_path="/Users/jpoeder/pydev/quantum_trade/_cred/creds.json"
    # )