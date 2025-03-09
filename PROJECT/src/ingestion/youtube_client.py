from src.utils.client import APIClient
import requests
import time
import os
from loguru import logger
from pprint import pprint
from pytube import YouTube

from typing import *

# Youtube API connector
#
# Retrieve methods call the youtube API
# Extract methods are often wrappers
#
# this class extracts video metadata, comments, thumbnails,
# videos and audio. CAPTIONS are not available through API


class YoutubeAPIClient(APIClient):
    def __init__(self):
        super().__init__()
        self.api_name = "youtube"
        self.base_url = self.config_manager.get_api_base_url(self.api_name)
        self.authenticate()
        self.connect()

    def authenticate(self):
        self.api_key = self.config_manager.get_api_credentials(self.api_name)["api_key"]
        logger.success("Successfully authenticated to YouTube!")

    def connect(self):
        logger.success("Successfully connected to YouTube API!")

    def fetch(
        self,
        endpoint: str,
        api_params: dict,
        max_retries: int = 3,
        backoff: int = 2,
        strict_raise: bool = False,
    ) -> dict:
        url = f"{self.base_url}{endpoint}"
        retry_count = 0

        while retry_count < max_retries:
            response = requests.get(url, params=api_params)
            # If more severe usage of HTTP requests is needed
            # then response.raise_for_status() is more appropiate
            if strict_raise:
                response.raise_for_status()
            if response.status_code == 200:
                return response.json()

            logger.warning(
                f"Request failed (Status {response.status_code}), Retrying {retry_count+1}/{max_retries}..."
            )
            retry_count += 1
            time.sleep(backoff**retry_count)  # Exponential backoff

        raise Exception(
            f"API request failed after {max_retries} retries on URL {url} with params {api_params}"
        )

    def retrieve_videos_basic_data(
        self, query: str, max_results: int = 10, use_shorts: bool = True
    ):
        """Fetches basic video metadata from search endpoint"""
        video_query_params = {
            "part": "snippet",
            "q": query,
            "type": ("short" if use_shorts else "video"),
            "maxResults": max_results,
            "order": "relevance",
            "videoCaption": "any",
            "videoDuration": "any",
            "key": self.api_key,
        }

        data = self.fetch("search", video_query_params)

        return [
            # TODO: SAVE THIS BASIC VIDEO METADATA SCHEMA
            {
                "title": item["snippet"]["title"],
                "url": f"https://www.youtube.com/watch?v={item['id']['videoId']}",
                "videoId": item["id"]["videoId"],
                "channel": item["snippet"]["channelTitle"],
                "year": item["snippet"]["publishedAt"],
                "short": use_shorts,
            }
            for item in data.get("items", [])
        ]

    def retrieve_video_statistics(self, video_ids: list):
        """Fetch additional statistical metadata using videos endpoint"""
        if not video_ids:
            return []

        params = {
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(video_ids),  # Pass all video IDs as CSV
            "key": self.api_key,
        }

        data = self.fetch("videos", params)

        # TODO: SAVE THIS DETAILED METADATA SCHEMA
        metadata = {
            item["id"]: {
                "description": item["snippet"].get("description", ""),
                "tags": item["snippet"].get("tags", []),
                "viewCount": int(item["statistics"].get("viewCount", 0)),
                "likeCount": int(item["statistics"].get("likeCount", 0)),
                "commentCount": int(item["statistics"].get("commentCount", 0)),
                "duration": item["contentDetails"]["duration"],
                "definition": item["contentDetails"]["definition"],
                "thumbnails": item["snippet"]["thumbnails"]
                .get("high", {})
                .get("url", ""),
            }
            for item in data.get("items", [])
        }

        return metadata

    def retrieve_videos(
        self, query: str, max_results: int = 10, use_shorts: bool = True
    ):
        """Combine basic and statistics metadata retrieval"""
        videos = self.retrieve_videos_basic_data(
            query=query, max_results=max_results, use_shorts=use_shorts
        )
        video_ids = [video["videoId"] for video in videos]

        metadata = self.retrieve_video_statistics(video_ids)

        for video in videos:
            video.update(metadata.get(video["videoId"], {}))  # Merge data

        return videos

    def retrieve_top_level_comments(self, video_id: str, max_results: int = 10):
        """Fetches top-level comments for a video."""
        api_params = {
            "part": "snippet",
            "videoId": video_id,
            "maxResults": max_results,
            "key": self.api_key,
        }

        try:
            data = self.fetch("commentThreads", api_params)
            # TODO: SAVE THIS COMMENT SCHEMA
            comments = [
                {
                    "channel_name": item["snippet"]["topLevelComment"]["snippet"][
                        "authorDisplayName"
                    ],
                    "channel_id": item["snippet"]["topLevelComment"]["snippet"]
                    .get("authorChannelId", {})
                    .get("value", "Unknown"),
                    "comment": item["snippet"]["topLevelComment"]["snippet"][
                        "textDisplay"
                    ],
                    "likes": item["snippet"]["topLevelComment"]["snippet"].get(
                        "likeCount", 0
                    ),
                    "replies": item["snippet"].get("totalReplyCount", 0),
                    "published_at": item["snippet"]["topLevelComment"]["snippet"].get(
                        "publishedAt", "Unknown"
                    ),
                }
                for item in data.get("items", [])
            ]

            if not comments:
                logger.warning(f"No top-level comments found for video {video_id}")
            return comments

        except requests.RequestException as e:
            logger.error(f"Error fetching comments for video {video_id}: {e}")
            return []

    def extract_comments_from_videos(self, videos: list, max_comments: int = 10):
        """Extracts comments from a given list of video metadata."""
        return {
            video["videoId"]: self.retrieve_top_level_comments(
                video["videoId"], max_comments
            )
            for video in videos
        }

    def extract_captions_from_videos(
        self, videos: List[Dict], flatten: bool = False
    ) -> Dict[str, str]:
        """Extracts all captions from a given list of video metadata"""
        all_captions = {}
        for video in videos:
            video_id = video["videoId"]
            caption_track_id = self.retrieve_caption_track(video_id)
            if caption_track_id:
                caption_content = self.download_caption(caption_track_id)
                if flatten:
                    all_captions[video_id] = caption_content if caption_content else ""
                else:
                    all_captions[video_id] = (
                        {"caption": caption_content} if caption_content else {}
                    )

        return all_captions

    def extract_video_thumbnails(self, videos: list, output_folder="thumbnails"):
        """Extracts high-quality thumbnails and saves them as image files."""
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        for video in videos:
            video_id = video["videoId"]
            thumbnail_url = video["thumbnails"]

            if thumbnail_url:
                file_path = os.path.join(output_folder, f"{video_id}_thumbnail.jpg")
                self.download_image(thumbnail_url, file_path)
                video["thumbnail_file_path"] = file_path

        return videos

    # Helper method
    def download_image(self, image_url: str, save_path: str):
        """Downloads and saves the image from the provided URL."""
        try:
            response = requests.get(image_url, stream=True)
            if response.status_code == 200:
                with open(save_path, "wb") as file:
                    for chunk in response.iter_content(1024):
                        file.write(chunk)
                logger.success(f"Downloaded image to {save_path}")
            else:
                logger.warning(f"Failed to download image from {image_url}")
        except requests.RequestException as e:
            logger.error(f"Error downloading image: {e}")

    def retrieve_audio(self, video_id: str, output_folder="videos") -> str:
        """Retrieve audio from a YouTube video."""
        yt = YouTube(f"https://www.youtube.com/watch?v={video_id}")
        audio_stream = yt.streams.filter(only_audio=True, file_extension="webm").first()
        return audio_stream.download(output_folder)

    def retrieve_video(
        self, video_id: str, output_folder="videos", format="mp4", resolution="low"
    ) -> str:
        """Retrieve a YouTube video with specific format and resolution."""
        yt = YouTube(f"https://www.youtube.com/watch?v={video_id}")
        stream = yt.streams.filter(file_extension=format)

        if resolution.lower() == "low":
            stream = stream.order_by("resolution").asc().first()
        else:
            stream = (
                stream.filter(res=resolution).first()
                or stream.order_by("resolution").desc().first()
            )

        return stream.download(output_folder)

    def extract_audio_from_videos(
        self, video_ids: List[str], output_folder="audios"
    ) -> Dict[str, str]:
        """Extract audio from a list of YouTube videos."""
        return {
            video_id: self.retrieve_audio(video_id, output_folder)
            for video_id in video_ids
        }

    def extract_video_from_videos(
        self,
        video_ids: List[str],
        output_folder="videos",
        format="mp4",
        resolution="low",
    ) -> Dict[str, str]:
        """Extract video with a specific format and resolution from a list of YouTube videos."""
        return {
            video_id: self.retrieve_video(video_id, output_folder, format, resolution)
            for video_id in video_ids
        }


# basic usage of this Youtube API class :)
if __name__ == "__main__":
    yt_ingestor = YoutubeAPIClient()

    # Retrieve videos metadata (In this case long videos)
    videos = yt_ingestor.retrieve_videos(
        query="Fellas in Paris", max_results=2, use_shorts=False
    )
    # Extract comments from the retrieved videos metadata
    comment_data = yt_ingestor.extract_comments_from_videos(videos, max_comments=5)
    #spprint(comment_data)

    # Request the thumbnails from the retrieved videos metadata (External requests)
    thumbnail_data = yt_ingestor.extract_video_thumbnails(
        videos, output_folder="resources/tests/thumbnails"
    )
    
    # TODO: audios and videos from pytube still doesn't work
    
    # Request the audios from the retrieved videos metadata (External requests)
    audio_data = yt_ingestor.extract_audio_from_videos(
        videos, output_folder="resources/tests/audios"
    )
    # Request the videos from the retrieved videos metadata (External requests)
    video_data = yt_ingestor.extract_video_from_videos(
        videos, output_folder="resources/tests/videos"
    )
