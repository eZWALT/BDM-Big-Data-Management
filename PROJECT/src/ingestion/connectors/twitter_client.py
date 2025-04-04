import os
import pprint
import time
import urllib.parse
from typing import List, Optional, Tuple, TypedDict

import requests
from loguru import logger

if __name__ == "__main__":
    import sys

    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from src.ingestion.connectors.models.twitter import *
from src.utils.config import ConfigManager

# ===-----------------------------------------------------------------------===#
# Twitter API Client                                                           #
#                                                                              #
# Author: Mateja Zatezalo, Marc Pracerisa                                      #
# ===-----------------------------------------------------------------------===#


class TwitterAPIClient:
    def __init__(self, api_key: str = None, base_url: str = None):
        self.config_manager = ConfigManager(config_path="configuration/api.yaml")
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
        self, method: str, endpoint: str, params: dict = None, json: dict = None, handle_rate_limit: bool = True, 
        strict_raise: bool = False
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
        try:
            if strict_raise:
                response.raise_for_status()  # Raise an error for bad responses
        except Exception as e:
            try:
                reason = response.text
            except Exception:
                reason = None
            if reason:
                raise requests.RequestException(
                    f"Twitter API request failed: {response.status_code} {response.reason} {reason}"
                ) from e
            else:
                raise e
        return response.json()

    def fetch_tweets(
        self,
        query: str,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
    ) -> Tuple[List[TweetData], Optional[str]]:
        """Fetch recent tweets based on a keyword."""
        # Specify tweet fields to be included in the response
        # can include 'lang' as in language as well etc.
        tweet_fields = "author_id,created_at,public_metrics"
        params = {"query": query, "tweet.fields": tweet_fields}
        if limit:
            if not 10 <= limit <= 100:
                raise ValueError("Limit must be between 10 and 100.")
            params["max_results"] = str(limit)
        if cursor:
            params["pagination_token"] = cursor
        if since:
            params["start_time"] = since
        if until:
            params["end_time"] = until
        response: TwitterResponse = self._request("GET", "tweets/search/recent", params=params)
        posts = response.get("data", [])
        total_posts = len(posts)
        while limit is None or total_posts < limit and response.get("meta", {}).get("next_token"):
            params["pagination_token"] = response.get("meta", {}).get("next_token")
            if limit:
                params["max_results"] = str(limit - total_posts)
            response: TwitterResponse = self._request("GET", "tweets/search/recent", params=params)
            posts.extend(response.get("data", []))
            total_posts += len(response.get("data", []))
        return posts, response.get("meta", {}).get("next_token")

    def fetch_replies(self, tweet_id: str, author_id: str, limit: int = None) -> Tuple[List[dict], str]:
        """Fetch replies to a specific tweet."""
        # NOTE: Twitter API is too slow for fetching replies, so this will most likely not be used
        # Query for getting tweets referenced to original tweet (reply)
        # tweet_id is the ID of original tweet
        query = f"conversation_id:{tweet_id} to:{author_id}"
        tweet_fields = "created_at,author_id"
        params = {"query": query, "tweet.fields": tweet_fields}
        if limit:
            params["max_results"] = str(limit)

        response = self._request("GET", "tweets/search/recent", params=params)
        replies = response.get("data", [])
        total_replies = len(replies)
        while limit is not None and total_replies < limit and response.get("meta", {}).get("next_token"):
            params["token"] = response.get("meta", {}).get("next_token")
            if limit:
                params["max_results"] = str(limit - total_replies)
            response = self._request("GET", "tweets/search/recent", params=params)
            replies.extend(response.get("data", []))
            total_replies += len(response.get("data", []))
        return replies, response.get("meta", {}).get("next_token")


def main():
    # Initialize the Twitter API client
    twitter_client = TwitterAPIClient()

    # Fetch historical data
    tweets = twitter_client.fetch_tweets("AirJordan", limit=10)

    print("Fetched tweets:")
    pprint.pprint(tweets)

    # Fetch replies to the tweets
    tweet_replies = {}
    for tweet in tweets:
        replies = twitter_client.fetch_replies(tweet["id"], tweet["author_id"])
        tweet_replies[tweet["id"]] = replies
        print("Tweet ID:", tweet["id"], "Replies:", replies)


if __name__ == "__main__":
    main()