# from src.utils.client import APIClient

import requests
import os
import tweepy
import json

# ===-----------------------------------------------------------------------===#
# Twitter API Client                                                          #
#                                                                             #
# Author: Mateja Zatezalo                                                     #
# ===-----------------------------------------------------------------------===#

#class TwitterAPIClient(APIClient):
    #pass

class MyStream(tweepy.StreamingClient):
    def on_tweet(self, tweet):
        print(tweet.text)
        # Additional logic to process the tweet and possibly store in the real-time database

# def setup_stream(auth, keywords, bearer_token):
#     stream = MyStream(bearer_token)
#     stream.add_rules(tweepy.StreamRule("WaterJordan OR Nike"))
#     stream.filter()

# Fetch tweets related to certain keyword
def fetch_tweets(recent_url, keyword, bearer_token):

    # Specify tweet fields to be included in the response
    # can include 'lang' as in language as well etc.
    tweet_fields = "tweet.fields=author_id,created_at,public_metrics"      

    headers = {"Authorization": f"Bearer {bearer_token}"}
    params = {"query": keyword, "max_results": "10", "tweet_fields": tweet_fields}

    response = requests.get(recent_url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
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
def fetch_replies(recent_url, tweet_id, author_id, bearer_token):

    # Query for getting tweets referenced to original tweet (reply)
    # tweet_id is the ID of original tweet
    query = f"conversation_id:{tweet_id} to:{author_id}"
    tweet_fields = "tweet.fields=created_at,author_id"
    headers = {"Authorization": f"Bearer {bearer_token}"}
    params = {
        "query": query,
        "tweet_fields": tweet_fields,
        "max_results": "100"
    }

    response = requests.get(recent_url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(response.status_code, response.text)

    
def main():
    # API keys and tokens
    consumer_key = 'GCVwCztxrvicfjLhoiOK7HgKu'
    consumer_secret = 'mwbvm9nDsHOZ1l59MPw5kvFdcgYr8ooQRjVh8LAc8U88sBdV7K'
    bearer_token = 'AAAAAAAAAAAAAAAAAAAAAJ05zgEAAAAAXrxXMn8qfKBzpo7uKVrS6I0iPDM%3DctS3xKFHN5Uee9H544KiEuX7RQBhJFKqzgNlKk7hcPc18Noqta'
    access_token_secret = 'q7BMvNzxbBW4jeUvTFXBLd55TAvLuEldfxCQfRYAEVBpG'
    access_token = '1894441990378721280-2Nml1hRlQtKsG4JvZ0BOp86y6N1wim'

    # URL for recent tweets
    recent_url = "https://api.twitter.com/2/tweets/search/recent"

    # Authenticate to Twitter
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    
    # Setup real-time tweet stream
    keywords = ['Jordan', 'Nike']
    # setup_stream(auth, keywords)
    
    # Fetch historical data
    tweets = fetch_tweets(recent_url, "Jordan", bearer_token)
    #print(tweets)
    if tweets and 'data' in tweets:
        print(json.dumps(tweets['data'][0], indent=4))  # Pretty print the first tweet

    # Fetch replies to the tweets
    tweet_replies = {}
    print("Tweet: ", tweets[0])
    #for tweet in tweets:
        #replies = fetch_replies(tweet['id'], bearer_token)
        #tweet_replies[tweet['id']] = replies


if __name__ == "__main__":
    main()
