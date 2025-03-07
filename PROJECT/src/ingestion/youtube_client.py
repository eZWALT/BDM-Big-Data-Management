from src.utils.client import APIClient

import requests
import os 
import sys
import time

from loguru import logger
from pprint import pprint

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
        self.api_name = "youtube"
        self.base_url = self.config_manager.get_api_base_url(self.api_name)
        self.authenticate()
        self.connect()

    
    def authenticate(self):
        self.api_key = self.config_manager.get_api_credentials(self.api_name)["api_key"] 
        logger.success("Successfully authenticated to Youtube!")
        
    def connect(self):
        logger.success("Successfully conncted to Youtube API!")

    
    def fetch(self, base: str, endpoint: str, params: dict = None, max_retries: int=3, backoff: int=2) -> dict:
        url = base + endpoint
        retry_count = 0 
        
        while retry_count < max_retries:
            response = requests.get(url, params=params)
            response.raise_for_status()

            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"Request failed with status code {response.status_code}. Retrying {retry_count + 1}/{max_retries}...")
                retry_count += 1
                wait_time = backoff ** retry_count  # Exponential backoff
                time.sleep(wait_time) 
        raise Exception(f"API request failed after {max_retries} retries")

    def retrieve_comments(self, video_id: str, max_results: int = 10):
        url = self.base_url + self.config_manager.get_api_endpoints(self.api_name)["comments"]
        params = {
            "part": "snippet",
            "videoId": video_id,
            "maxResults": max_results,
            "key": self.api_key
        }
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            comments = []
            
            for item in data.get('items', []):
                comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
                author = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
                comments.append({"author": author, "comment": comment})
            
            return comments

        except requests.RequestException as e:
            logger.error(f"Error fetching comments for video {video_id}: {e}")
            return []
        
    # Helper function to retrieve videoID's for extracting further information                
    def retrieve_videos(self, params: dict = None, query_shorts: bool = False) -> list:
        endpoint = "search"
        
        #default parameters
        if params is None:
            params = {
                "part": "snippet",
                "q": "fake ass jordans",
                "type": "short" if query_shorts else "video",
                "maxResults": 1,
                "order": "relevance",
                "videoCaption": "any",
                "videoDuration": "any",
                "key": self.api_key
            }
        #Sanity check over user input
        if not query_shorts and (params.get("VideoDuration", "") == "short" or params.get("type") == "short"):
            logger.error("Invalid Youtube request, it shouldn't have shorts in the header")
            raise Exception(f"API request failed due to malformation of the header paramters (Invalid shorts)")
         

        data = self.fetch(self.base_url, "search", params)
        # TODO: Save this schema as configuration or smth
        return [
            {
                    "title": item["snippet"]["title"],
                    "url": f"https://www.youtube.com/watch?v={item['id']['videoId']}",
                    "videoId": item["id"]["videoId"],
                    "channel": item["snippet"]["channelTitle"],
                    "year": item["snippet"]["publishedAt"],
                    "short": query_shorts
            }
            for item in data["items"]
        ]

    def extract_comments_from_recommended(self, params: dict = None, max_comments: int = 10):
        """Fetch recommended videos and their comments"""
        videos = self.retrieve_videos(params)
        all_comments = {}

        for video in videos:
            video_id = video["videoId"]
            comments = self.retrieve_comments(video_id)
            all_comments[video_id] = comments

        return all_comments
    
    def extract_captions_from_recommended(self, params: dict = None):
        videos = self.retrieve_videos(params)
        all_captions = {}
        
        # TODO: Figure out how to extract captions
        # TODO: Figure out if shorts have captions or not
        for video in videos:
            video_id = video["videoId"]
            captions = self.retrieve_captions(video_id)
            

        
# TODO: Define a standard unified youtube format (SCHEMA?)
if __name__ == "__main__":
    yt_ingestor = YoutubeAPIClient()
    video_data = yt_ingestor.retrieve_videos(query_shorts=True)
    pprint(video_data, width=1)
    comment_data = yt_ingestor.extract_comments_from_recommended(max_comments=10)
    pprint(comment_data, width=1)