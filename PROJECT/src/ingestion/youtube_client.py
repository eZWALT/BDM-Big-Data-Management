from src.utils.client import APIClient
from src.utils.config import ConfigManager

import requests
import os 

popular_endpoints = {
    "search": "https://www.googleapis.com/youtube/v3/search",
    "video": "https://www.googleapis.com/youtube/v3/videos",
    "channels": "https://www.googleapis.com/youtube/v3/channels",
    "playlists": "https://www.googleapis.com/youtube/v3/playlists", 
    "comments": "https://www.googleapis.com/youtube/v3/commentThreads",
    "subscriptions": "https://www.googleapis.com/youtube/v3/subscriptions", 
    "videoCategories": "https://www.googleapis.com/youtube/v3/videoCategories"
}


class YoutubeAPIClient(APIClient):
    def __init__(self):
        super().__init__()
        self.api_key = None
        
    def authenticate(self):
        pass
    
    def connect(self):
        pass 
    
    def fetch(self, base: str, endpoint: str, params: dict = None):
        pass
        

# Local main setup to test the class
if __name__ == "__main__":

    YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")  # Store API key in environment variables
    SEARCH_QUERY = "data science"

    def search_youtube(query, max_results=5):
        url = "https://www.googleapis.com/youtube/v3/search"
        params = {
            "part": "snippet",
            "q": query,
            "type": "video",
            "maxResults": max_results,
            "key": YOUTUBE_API_KEY
        }
        
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            return [
                {
                    "title": item["snippet"]["title"],
                    "url": f"https://www.youtube.com/watch?v={item['id']['videoId']}",
                    "channel": item["snippet"]["channelTitle"],
                }
                for item in data["items"]
            ]
        else:
            raise Exception(f"Error: {response.status_code}, {response.json()}")

    # Fetch results
    videos = search_youtube(SEARCH_QUERY)
    for video in videos:
        print(video)