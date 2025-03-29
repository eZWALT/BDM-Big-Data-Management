import json
import os
import pprint
import time
import urllib.parse
from typing import List, Tuple

import requests
from loguru import logger

if __name__ == "__main__":
    import sys

    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from src.utils.config import ConfigManager

# ===-----------------------------------------------------------------------===#
# Twitter API Client                                                          #
#                                                                             #
# Author: Mateja Zatezalo                                                     #
# ===-----------------------------------------------------------------------===#


class TwitterAPIClient:
    def __init__(self, api_key: str = None, base_url: str = None):
        self.config_manager = ConfigManager(config_path="config/api.yaml")
        if not api_key:
            credentials = self.config_manager.get_api_credentials("twitter")
            self.bearer_token = api_key or credentials.get("api_key")
        else:
            self.bearer_token = api_key

        if not self.bearer_token:
            raise ValueError("Api Key must be provided for Twitter authentication.")
        self.base_url = (
            base_url or self.config_manager.get_api_base_url("twitter") or os.getenv("TWITTER_BASE_URL") or None
        )

    def _get_endpoint(self, endpoint: str):
        """Construct the full endpoint URL."""
        if not self.base_url:
            raise ValueError("Base URL is not set.")
        return urllib.parse.urljoin(self.base_url, endpoint)

    def _wait_rate_limit(self, response: requests.Response):
        """Wait for the rate limit to reset if the request returned status 429."""
        if response.status_code == 429:
            reset_time = int(response.headers.get("X-Rate-Limit-Reset", time.time())) + 5
            sleep_time = reset_time - time.time()
            if sleep_time > 0:
                logger.warning(f"[TWITTER API CLIENT] Limit exceeded. Sleeping for {sleep_time} seconds.")
                time.sleep(sleep_time)

    def _request(
        self, method: str, endpoint: str, params: dict = None, json: dict = None, handle_rate_limit: bool = True
    ):
        """
        Make a request to the Twitter API.

        Returns:
            data (Any): The JSON response from the API.
        """
        url = self._get_endpoint(endpoint)
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        response = requests.request(method, url, headers=headers, params=params, json=json)
        if handle_rate_limit:
            while response.status_code == 429:
                self._wait_rate_limit(response)
                response = requests.request(method, url, headers=headers, params=params, json=json)
        response.raise_for_status()  # Raise an error for bad responses
        return response.json()

    def fetch_tweets(self, query: str, limit: int = None) -> Tuple[List[dict], str]:
        """Fetch recent tweets based on a keyword."""
        # Example JSON Response: (for me to know how it looks like)
        # {
        #     "data": [
        #         {
        #         "id": "1234567890",
        #         "text": "Sample tweet text here...",
        #         "created_at": "2021-07-01T12:00:00.000Z",
        #         "public_metrics": {
        #             "retweet_count": 100,
        #             "reply_count": 50,
        #             "like_count": 500
        #         }
        #         }
        #     ],
        #     "meta": {
        #         "result_count": 1,
        #         "newest_id": "1234567890",
        #         "oldest_id": "1234567890",
        #         "next_token": "b26v89c19zqg8o3fos5t9w8kd"
        #     }
        # }
        # Specify tweet fields to be included in the response
        # can include 'lang' as in language as well etc.
        tweet_fields = "author_id,created_at,public_metrics"
        params = {"query": query, "tweet.fields": tweet_fields}
        if limit:
            params["max_results"] = str(limit)
        response = self._request("GET", "tweets/search/recent", params=params)
        posts = response.get("data", [])
        total_posts = len(posts)
        while limit is not None and total_posts < limit and response.get("meta", {}).get("next_token"):
            params["token"] = response.get("meta", {}).get("next_token")
            if limit:
                params["max_results"] = str(limit - total_posts)
            response = self._request("GET", "tweets/search/recent", params=params)
            posts.extend(response.get("data", []))
            total_posts += len(response.get("data", []))
        return posts, response.get("meta", {}).get("next_token")

    # Fetch replies to tweets to later analyze the sentiment
    def fetch_replies(self, recent_url, tweet_id, author_id, bearer_token):

        # Query for getting tweets referenced to original tweet (reply)
        # tweet_id is the ID of original tweet
        query = f"conversation_id:{tweet_id} to:{author_id}"
        tweet_fields = "created_at,author_id"
        headers = {"Authorization": f"Bearer {bearer_token}"}
        params = {"query": query, "tweet.fields": tweet_fields, "max_results": "10"}

        response = requests.get(recent_url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            self._wait_rate_limit(response)
            # Retry the request after rate limit handling
            return self.fetch_replies(recent_url, tweet_id, author_id, bearer_token)
        else:
            print(response.status_code, response.text)


def main():

    # URL for recent tweets
    recent_url = "https://api.twitter.com/2/tweets/search/recent"

    # Initialize the Twitter API client
    twitter_client = TwitterAPIClient()

    # Fetch historical data
    tweets = twitter_client.fetch_tweets("AirJordan", limit=5)

    print("Fetched tweets:")
    pprint.pprint(tweets)

    # Fetch replies to the tweets
    tweet_replies = {}
    if "data" in tweets:
        for tweet in tweets["data"]:
            replies = twitter_client.fetch_replies(
                recent_url, tweet["author_id"], tweet["id"], twitter_client.bearer_token
            )  # Fetching replies using the tweet ID
            tweet_replies[tweet["id"]] = replies
            print("Tweet ID:", tweet["id"], "Replies:", replies)
    else:
        print("No data found or incorrect data structure.")


if __name__ == "__main__":
    main()
