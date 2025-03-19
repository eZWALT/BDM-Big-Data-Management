from src.utils.client import APIClient

import requests
import os
import tweepy

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
def fetch_tweets(keyword, bearer_token):

    # URL for recent tweets
    url = "https://api.twitter.com/2/tweets/search/recent"

    # Specify tweet fields to be included in the response
    # can include 'lang' as in language as well etc.
    tweet_fields = "tweet.fields=author_id,created_at,public_metrics"      

    headers = {"Authorization": f"Bearer {bearer_token}"}
    params = {"query": keyword, "max_results": "10", "tweet_fields": tweet_fields}

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        return response.status_code, response.text
    
def main():
    # API keys and tokens
    consumer_key = 'GCVwCztxrvicfjLhoiOK7HgKu'
    consumer_secret = 'mwbvm9nDsHOZ1l59MPw5kvFdcgYr8ooQRjVh8LAc8U88sBdV7K'
    bearer_token = 'AAAAAAAAAAAAAAAAAAAAAJ05zgEAAAAAXrxXMn8qfKBzpo7uKVrS6I0iPDM%3DctS3xKFHN5Uee9H544KiEuX7RQBhJFKqzgNlKk7hcPc18Noqta'
    access_token_secret = 'q7BMvNzxbBW4jeUvTFXBLd55TAvLuEldfxCQfRYAEVBpG'
    access_token = '1894441990378721280-2Nml1hRlQtKsG4JvZ0BOp86y6N1wim'

    # Authenticate to Twitter
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    
    # Setup real-time tweet stream
    keywords = ['Jordan', 'Nike']
    # setup_stream(auth, keywords)
    
    # Fetch historical data
    tweets = fetch_tweets("Jordan", bearer_token)
    print(tweets)


if __name__ == "__main__":
    main()
