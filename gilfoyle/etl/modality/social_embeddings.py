"""
Data loader for social media content embeddings.
"""
import os
import sys
from datetime import datetime, timedelta, timezone
import logging
import json

# import requests
from pymongo import MongoClient

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from quantum_trade_utilities.core.get_path import get_path
from quantum_trade_utilities.data.load_credentials import load_credentials
from quantum_trade_utilities.data.mongo_conn import mongo_conn
from quantum_trade_utilities.data.mongo_coll_verification import (
    confirm_mongo_collect_exists,
)

# Set up logging
logging.basicConfig(level=logging.WARNING)  # Set to WARNING to suppress DEBUG messages
logger = logging.getLogger("pymongo")
logger.setLevel(logging.WARNING)  # Suppress pymongo debug messages


class ResponseObj:
    """Response object for the social embeddings process."""

    def __init__(self, text: dict):
        self.text = text


def get_mongo_connection():
    """Establish MongoDB connection using credentials."""
    creds_path = get_path("creds")
    mongo_creds = load_credentials(creds_path, "mongodb")
    client = MongoClient(
        mongo_creds[0]
    )  # Assuming the connection string is first credential
    return client


def process_content(post_data, comments_data):
    """Process post and comments as separate content items."""
    content_items = []

    # Process post if it has content
    if post_data.get("title") or post_data.get("selftext"):
        post_text = f"Title: {post_data.get('title', '')}\n\nContent: {post_data.get('selftext', '')}".strip()
        if post_text:
            content_items.append(
                {
                    "text": post_text,
                    "type": "post",
                    "id": post_data.get("unique_id"),
                    "timestamp": post_data.get("timestamp"),
                }
            )

    # Process comments
    filtered_comments = [
        comment
        for comment in comments_data
        if abs(comment.get("score", 0)) >= 0  # Adjust score threshold as needed
    ]

    # Sort comments by depth
    sorted_comments = sorted(filtered_comments, key=lambda x: x.get("depth", 0))

    # Process each comment separately
    for comment in sorted_comments:
        comment_text = comment.get("body", "").strip()
        if comment_text:
            content_items.append(
                {
                    "text": f"Comment (depth {comment.get('depth', 0)}): {comment_text}",
                    "type": "comment",
                    "id": comment.get("unique_id"),
                    "post_id": post_data.get("unique_id"),
                    "timestamp": comment.get("timestamp"),
                    "depth": comment.get("depth", 0),
                }
            )

    return content_items if content_items else None


def social_embeddings(
    job_scope: str = "live",  # 'live' or 'historical'
    mongo_db: str = "socialDB",
    source: str = "social",
    doc_type: str = "reddit_content",
    entity_type: str = "org",
    device: str = "cpu",
):
    """Process social media content for embeddings."""
    logging.info("Starting social embeddings process...")

    # Get the database connection
    db = mongo_conn(mongo_db=mongo_db)

    # Get credentials and connection
    creds_path = get_path("creds")
    QT_BACHMAN_API_KEY = load_credentials(creds_path, "bachman_api")[0]
    if not QT_BACHMAN_API_KEY:
        logging.error("Bachman API key not found")
        return None

    for ticker in job_scope:
        # Get the collection name
        posts_collection_ref = f"{ticker}_redditPosts"
        comments_collection_ref = f"{ticker}_redditComments"
        embedded_collection_ref = f"{ticker}_redditEmbedded"

        # Ensure the collection exists
        confirm_mongo_collect_exists(posts_collection_ref, mongo_db)
        confirm_mongo_collect_exists(comments_collection_ref, mongo_db)
        confirm_mongo_collect_exists(embedded_collection_ref, mongo_db)

        # Get the collection
        posts_collection = db[posts_collection_ref]
        comments_collection = db[comments_collection_ref]
        embedded_collection = db[embedded_collection_ref]

        current_timestamp = datetime.now(timezone.utc)

        # Define time filter based on job scope
        time_filter = (
            {"timestamp": {"$gte": current_timestamp - timedelta(hours=1)}}
            if job_scope == "live"
            else {}
        )

        # Get posts that haven't been processed
        posts_cursor = posts_collection.find(
            {
                **time_filter,
                "unique_id": {"$nin": list(embedded_collection.distinct("post_id"))},
            }
        )

        for post in posts_cursor:
            # Get related comments
            comments = list(
                comments_collection.find({"post_UID": post["unique_id"], **time_filter})
            )

            # Process content
            content_items = process_content(post, comments)
            if not content_items:
                continue

            # # Process each content item separately
            # for item in content_items:
            #     data_payload = {
            #         "text": item['text'],
            #         "collection_name": "social_content_v1",
            #         "skip_if_exists": True,
            #         "metadata": {
            #             "embedding_config": {
            #                 "vectorstore": {
            #                     "provider": "huggingface",
            #                     "model": "BAAI/bge-large-en-v1.5"
            #                 },
            #                 "device": "cpu"
            #             },
            #             "source": "reddit",
            #             "doc_id": item['id'],
            #             "parent_id": item.get('post_id'),  # Only present for comments
            #             "content_type": item['type'],
            #             "doc_type": "reddit_content",
            #             "ticker": ticker,
            #             "e_type": "org",
            #             "timestamp": current_timestamp.isoformat(),
            #             "content_timestamp": item['timestamp'].isoformat(),
            #             "depth": item.get('depth')  # Only present for comments
            #         }
            #     }

            #     try:
            #         response = requests.post(
            #             "http://127.0.0.1:8713/bachman/process_text",
            #             json=data_payload,
            #             headers={
            #                 "Content-Type": "application/json",
            #                 "x-api-key": QT_BACHMAN_API_KEY
            #             },
            #             timeout=120000
            #         )
            #         response.raise_for_status()

            #         # Record successful embedding only for posts
            #         if item['type'] == 'post':
            #             embedded_collection.insert_one({
            #                 "post_id": item['id'],
            #                 "embedded_at": current_timestamp,
            #                 "post_timestamp": item['timestamp']
            #             })

            #         logging.info(f"Successfully processed {item['type']} {item['id']} for {ticker}")

            #     except requests.exceptions.RequestException as e:
            #         logging.error(f"Error processing {item['type']} {item['id']} for {ticker}: {str(e)}")
            #         continue

    text = {
        "status": "success",
        "message": "Social embeddings process completed successfully.",
    }
    return ResponseObj(text=json.dumps(text)).text
