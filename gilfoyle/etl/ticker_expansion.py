"""
Ticker expansion
"""

import os
from datetime import datetime, timedelta, timezone
import json

from gilfoyle._utils.mongo_conn import mongo_conn

# from gilfoyle._utils.load_credentials import load_credentials
from gilfoyle._utils.mongo_coll_verification import confirm_mongo_collect_exists


def ticker_expansion():
    """
    Ticker expansion
    """
    # Load the control JSON file
    try:
        with open(
            f'{os.getenv("JOB_CTRLS")}/mlops.json', "r", encoding="utf-8"
        ) as file:
            control_data = json.load(file)
            tickers_to_process = control_data.get("expansion", [])
    except FileNotFoundError:
        tickers_to_process = []

    # Ensure the collection exists
    confirm_mongo_collect_exists("rawPriceColl")
    confirm_mongo_collect_exists("expandedPriceColl")

    # Connect to MongoDB
    db = mongo_conn()
    raw_collection = db["rawPriceColl"]
    expanded_collection = db["expandedPriceColl"]

    # Create a compound index on 'timestamp' and 'ticker'
    raw_collection.create_index([("timestamp", 1), ("ticker", 1)], unique=True)
    expanded_collection.create_index([("timestamp", 1), ("ticker", 1)], unique=True)

    # Define market hours
    market_start = datetime.strptime("04:00", "%H:%M").time()
    market_end = datetime.strptime("20:00", "%H:%M").time()

    # Loop through each ticker
    tickers = tickers_to_process or raw_collection.distinct("ticker")
    for ticker in tickers:
        print(f"Processing ticker: {ticker}")  # Print the ticker being processed

        # Use aggregation to get distinct dates
        pipeline = [
            {"$match": {"ticker": ticker}},
            {
                "$group": {
                    "_id": {
                        "$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}
                    }
                }
            },
        ]
        days = [
            datetime.strptime(doc["_id"], "%Y-%m-%d").date()
            for doc in raw_collection.aggregate(pipeline)
        ]

        for day in days:
            # Get all minutes for the day
            start_of_day = datetime.combine(day, market_start)
            end_of_day = datetime.combine(day, market_end)
            existing_minutes = raw_collection.find(
                {
                    "ticker": ticker,
                    "timestamp": {"$gte": start_of_day, "$lte": end_of_day},
                }
            )
            existing_times = {doc["timestamp"].time() for doc in existing_minutes}

            # Generate all possible minutes in the trading day
            current_time = start_of_day
            while current_time <= end_of_day:
                if current_time.time() not in existing_times:
                    # Impute missing minute
                    last_close = get_last_close(raw_collection, ticker, current_time)
                    imputed_data = {
                        "ticker": ticker,
                        "timestamp": current_time,
                        "open": last_close,
                        "low": last_close,
                        "high": last_close,
                        "close": last_close,
                        "volume": 0,
                        "trade_count": 0,
                        "vwap": 0,
                        "created_at": datetime.now(timezone.utc),
                        "imputed": 1,
                    }
                    expanded_collection.insert_one(imputed_data)
                    print(f"Imputed data for {ticker} on {current_time}")
                else:
                    # Update existing document with imputed value of 0
                    expanded_collection.update_one(
                        {"ticker": ticker, "timestamp": current_time},
                        {"$set": {"imputed": 0}},
                        upsert=False,
                    )
                current_time += timedelta(minutes=1)


def get_last_close(collection, ticker, current_time):
    """
    Get the last close value before the current time
    """
    # Find the last close value before the current time
    previous_data = collection.find_one(
        {"ticker": ticker, "timestamp": {"$lt": current_time}}, sort=[("timestamp", -1)]
    )
    return previous_data["close"] if previous_data else 0
