"""
Load Alpaca API credentials from JSON file.
"""
import json
import os
import sys
import dotenv

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gilfoyle._utils.get_path import get_path


def load_credentials(file_path, data_type):
    """
    Load Alpaca API credentials from JSON file.
    """

    # Load environment variables from the .env file
    dotenv_path = get_path("env")
    dotenv.load_dotenv(dotenv_path)

    if data_type == "alpaca_paper_trade":
        with open(file_path, "r", encoding="utf-8") as file:
            creds = json.load(file)
        alpaca_creds = creds["alpaca_paper_api"]
        return (
            alpaca_creds["API_KEY"],
            alpaca_creds["API_SECRET"],
            alpaca_creds["PAPER_URL"],
        )

    elif data_type == "alpaca_live_trade":
        with open(file_path, "r", encoding="utf-8") as file:
            creds = json.load(file)
        alpaca_creds = creds["alpaca_live_api"]
        return (
            alpaca_creds["API_KEY"],
            alpaca_creds["API_SECRET"],
            alpaca_creds["LIVE_URL"],
        )

    elif data_type == "alpaca_news":
        with open(file_path, "r", encoding="utf-8") as file:
            creds = json.load(file)
        news_creds = creds["alpaca_news_api"]
        return (
            news_creds["API_KEY"],
            news_creds["API_SECRET"],
        )

    if data_type == "fmp_api":
        with open(file_path, "r", encoding="utf-8") as file:
            creds = json.load(file)
        fmp_creds = creds["fmp_api"]
        return (
            fmp_creds["API_KEY"],
            fmp_creds["BASE_URL"],
        )

    if data_type == "fmp_api":
        with open(file_path, "r", encoding="utf-8") as file:
            creds = json.load(file)
        fmp_creds = creds["fmp_api"]
        return (
            fmp_creds["API_KEY"],
            fmp_creds["BASE_URL"],
        )

    if data_type == "hendricks_api":
        with open(file_path, "r", encoding="utf-8") as file:
            creds = json.load(file)
        hendricks_creds = creds["hendricks_api"]
        return (hendricks_creds["API_KEY"],)

    if data_type == "gilfoyle_api":
        with open(file_path, "r", encoding="utf-8") as file:
            creds = json.load(file)
        gilfoyle_creds = creds["gilfoyle_api"]
        return (gilfoyle_creds["API_KEY"],)

    else:
        raise ValueError(f"Invalid data type: {data_type}")
