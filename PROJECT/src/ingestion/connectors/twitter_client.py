from src.utils.client import APIClient

import requests
import os
import json
import time

# ===-----------------------------------------------------------------------===#
# Twitter API Client                                                          #
#                                                                             #
# Author: Mateja Zatezalo                                                     #
# ===-----------------------------------------------------------------------===#

class TwitterAPIClient():
  def __init__(self, consumer_key, consumer_secret, bearer_token, access_token, access_token_secret):
      self.consumer_key = os.getenv('consumer_key')
      self.consumer_secret = os.getenv('consumer_secret')
      self.bearer_token = os.getenv('bearer_token')
      self.access_token = os.getenv('access_token')
      self.access_token_secret = os.getenv('access_token_secret')

  # Fetch tweets related to certain keyword
  def fetch_tweets(self, recent_url, keyword, bearer_token):

      # Specify tweet fields to be included in the response
      # can include 'lang' as in language as well etc.
      tweet_fields = "author_id,created_at,public_metrics"      

      headers = {"Authorization": f"Bearer {bearer_token}"}
      params = {"query": keyword, "max_results": "10", "tweet.fields": tweet_fields}

      response = requests.get(recent_url, headers=headers, params=params)
      if response.status_code == 200:
          return response.json()
      elif response.status_code == 429:
          self.handle_rate_limit(response)
          # Retry the request after rate limit handling
          return self.fetch_tweets(recent_url, keyword, bearer_token) 
      else:
          return response.status_code, response.text
      
  """
  Example JSON Response: (for me to know how it looks like)
  {
    "data": [
      {
        "id": "1234567890",
        "text": "Sample tweet text here...",
        "created_at": "2021-07-01T12:00:00.000Z",
        "public_metrics": {
          "retweet_count": 100,
          "reply_count": 50,
          "like_count": 500
        }
      }
    ],
    "meta": {
      "result_count": 1,
      "newest_id": "1234567890",
      "oldest_id": "1234567890",
      "next_token": "b26v89c19zqg8o3fos5t9w8kd"
    }
  }
  """

  # Fetch replies to tweets to later analyze the sentiment 
  def fetch_replies(self, recent_url, tweet_id, author_id, bearer_token):

      # Query for getting tweets referenced to original tweet (reply)
      # tweet_id is the ID of original tweet
      query = f"conversation_id:{tweet_id} to:{author_id}"
      tweet_fields = "created_at,author_id"
      headers = {"Authorization": f"Bearer {bearer_token}"}
      params = {
          "query": query,
          "tweet.fields": tweet_fields,
          "max_results": "10"
      }

      response = requests.get(recent_url, headers=headers, params=params)
      if response.status_code == 200:
          return response.json()
      elif response.status_code == 429:
          self.handle_rate_limit(response)
          # Retry the request after rate limit handling
          return self.fetch_replies(recent_url, tweet_id, author_id, bearer_token)
      else:
          print(response.status_code, response.text)

  # Use this function after catching a 'Too many requests' response
  def handle_rate_limit(self, response):
      if response.status_code == 429:
          reset_time = int(response.headers.get('X-Rate-Limit-Reset', time.time())) + 5  
          sleep_time = reset_time - time.time()
          if sleep_time > 0:
              print(f"Limit exceeded. Sleeping for {sleep_time} seconds.")
              time.sleep(sleep_time)

    
def main():

    # URL for recent tweets
    recent_url = "https://api.twitter.com/2/tweets/search/recent"

    # Initialize the Twitter API client
    twitter_client = TwitterAPIClient(
        consumer_key=os.getenv('consumer_key'), 
        consumer_secret=os.getenv('consumer_secret'),
        bearer_token=os.getenv('bearer_token'),
        access_token=os.getenv('access_token'), 
        access_token_secret=os.getenv('access_token_secret')
    )
    
    keywords = ['Jordan', 'Nike']
    
    # Fetch historical data
    tweets = twitter_client.fetch_tweets(recent_url, "AirJordan", twitter_client.bearer_token)
    #print(tweets)
    if tweets and 'data' in tweets:
        print(json.dumps(tweets['data'][0], indent=4))  

    # Fetch replies to the tweets
    tweet_replies = {}
    if 'data' in tweets:
      for tweet in tweets['data']:  
          replies = twitter_client.fetch_replies(recent_url, tweet['author_id'], tweet['id'], 
              twitter_client.bearer_token)  # Fetching replies using the tweet ID
          tweet_replies[tweet['id']] = replies
          print("Tweet ID:", tweet['id'], "Replies:", replies)
    else:
        print("No data found or incorrect data structure.")

if __name__ == "__main__":
    main()
