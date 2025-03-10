from src.utils.client import APIClient
import requests
import time
import random
import os
from typing import *

from pathlib import Path
import json

from loguru import logger
from pprint import pprint
from pytubefix import YouTube
from pytubefix.cli import on_progress

# ===-----------------------------------------------------------------------===#
# YouTube API Client                                                          #
#                                                                             #
# This class provides an interface to interact with the YouTube Data API,     #
# enabling the retrieval of video metadata, comments, thumbnails, videos,     #
# and audio. Captions are not available via the API and are retrieved         #
# using pytubefix.                                                            #
#                                                                             #
# The class is structured around:                                             #
#   - **Retrieve Methods**: Direct API calls to fetch raw data.               #
#   - **Extract Methods**: High-level wrappers that process, store, and       #
#     return structured data while generating files as needed.                #
#                                                                             #
# Designed for extensibility, this client supports metadata extraction,       #
# media downloads, and API authentication with exponential backoff for        #
# robustness.                                                                 #
#                                                                             #
# Author: Walter J.T.V                                                        #
# ===-----------------------------------------------------------------------===#


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
        """General method to fetch data from YouTube API with retry logic."""
        url = f"{self.base_url}{endpoint}"
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(url, params=api_params)
                if strict_raise:
                    response.raise_for_status()
                if response.status_code == 200:
                    return response.json()
                logger.warning(
                    f"[Fetch] Attempt {attempt}/{max_retries} failed (Status {response.status_code})"
                )
                time.sleep(backoff**attempt)
            except requests.RequestException as e:
                logger.error(f"[Fetch] API request error: {e}")
        raise Exception(
            f"[Fetch] API request failed after {max_retries} retries: {url}"
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

    def retrieve_top_level_comments(self, video_id: str, max_results: int = 10):
        """Fetches top-level comments for a video."""
        api_params = {
            "part": "snippet",
            "videoId": video_id,
            "maxResults": max_results,
            "key": self.api_key,
            "order": "relevance",
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

    # These 3 retrieval methods work with pytube scrapping rather than the youtube API
    def retry_pytubefix_operation(
        self, operation, *args, max_retries=3, backoff=2, **kwargs
    ):
        """Helper method to retry pytubefix operations with exponential backoff."""
        retry_count = 0
        while retry_count < max_retries:
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                retry_count += 1
                logger.warning(
                    f"Operation failed (Attempt {retry_count}/{max_retries}): {e}"
                )
                if retry_count >= max_retries:
                    logger.error(f"Operation failed after {max_retries} retries.")
                    raise e
                sleep_time = backoff**retry_count + random.uniform(
                    0, 1
                )  # Exponential backoff with jitter
                logger.info(f"Retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)

    def retrieve_audio(self, video_id: str, output_folder: str) -> str:
        """Retrieve audio from a YouTube video in m4a format."""
        try:
            yt = YouTube(f"https://www.youtube.com/watch?v={video_id}")
            ys = yt.streams.get_audio_only()
            self.retry_pytubefix_operation(
                ys.download, filename=f"{video_id}.mp3", output_path=output_folder
            )
            logger.success(f"Successfully downloaded audio for video {video_id}")
            return f"{output_folder}/{video_id}.mp3"
        except Exception as e:
            logger.error(f"Failed to download audio for {video_id}: {e}")
            return None

    def retrieve_video(self, video_id: str, output_folder: str) -> str:
        """Retrieve a YouTube video with the highest resolution available."""
        try:
            yt = YouTube(f"https://www.youtube.com/watch?v={video_id}")
            ys = yt.streams.get_highest_resolution()
            self.retry_pytubefix_operation(
                ys.download, filename=f"{video_id}.mp4", output_path=output_folder
            )
            logger.success(f"Successfully downloaded video for video {video_id}")
            return f"{output_folder}/{video_id}.mp4"
        except Exception as e:
            logger.error(f"Failed to download video for {video_id}: {e}")
            return None

    def retrieve_caption(self, video_id: str, output_folder: str):
        """Retrieve and download English captions from a YouTube video."""
        try:
            yt = YouTube(f"https://www.youtube.com/watch?v={video_id}")
            captions = {caption.code: caption for caption in yt.captions}
            en_caption = captions.get("a.en") or next(
                (
                    caption
                    for code, caption in captions.items()
                    if code.startswith("en")
                ),
                None,
            )

            if en_caption:
                self.retry_pytubefix_operation(
                    en_caption.download, title=video_id, output_path=output_folder
                )
                logger.success(f"Successfully downloaded captions for video {video_id}")
                return f"{output_folder}/{video_id}.srt"
            else:
                logger.warning(f"No English captions available for video {video_id}")
        except Exception as e:
            logger.error(f"Failed to retrieve captions for video {video_id}: {e}")

    def extract_videos(
        self,
        query: str,
        max_results: int = 10,
        use_shorts: bool = True,
        output_folder: str = "videos_metadata",
        save: bool = True,
    ):
        """Retrieve videos metadata, merge statistics, and save each video as a separate JSON file."""
        output_path = Path(output_folder)
        output_path.mkdir(parents=True, exist_ok=True)

        videos = self.retrieve_videos_basic_data(query, max_results, use_shorts)
        video_ids = [video["videoId"] for video in videos]
        metadata = self.retrieve_video_statistics(video_ids)

        for video in videos:
            video.update(metadata.get(video["videoId"], {}))
            if save:
                file_path = output_path / f"{video['videoId']}.json"
                file_path.write_text(
                    json.dumps(video, indent=4, ensure_ascii=False), encoding="utf-8"
                )
                logger.success(f"[Metadata] Saved video metadata: {file_path}")

        return videos

    def extract_comments_from_videos(
        self,
        videos: list,
        max_comments: int = 10,
        output_folder: str = "comments",
        save: bool = True,
    ):
        """Extracts comments from a given list of video metadata and saves them as JSON."""
        output_path = Path(output_folder)
        output_path.mkdir(parents=True, exist_ok=True)

        results = {
            video["videoId"]: self.retrieve_top_level_comments(
                video["videoId"], max_comments
            )
            for video in videos
        }

        if save:
            for video_id, comments in results.items():
                file_path = output_path / f"{video_id}.json"
                file_path.write_text(
                    json.dumps(comments, indent=4, ensure_ascii=False), encoding="utf-8"
                )
                logger.success(
                    f"[Comments] Saved {len(comments)} comments for {video_id}: {file_path}"
                )

        return results

    def extract_video_thumbnails(self, videos: list, output_folder="thumbnails"):
        """Extracts high-quality thumbnails and saves them as image files."""
        output_path = Path(output_folder)
        output_path.mkdir(parents=True, exist_ok=True)

        for video in videos:
            video_id = video["videoId"]
            thumbnail_url = video["thumbnails"]
            if thumbnail_url:
                file_path = output_path / f"{video_id}.jpg"
                self.download_image(thumbnail_url, file_path)
                logger.success(
                    f"[Thumbnails] Saved thumbnail for {video_id}: {file_path}"
                )
            else:
                logger.warning(f"[Thumbnails] No thumbnail available for {video_id}")

        return videos

    def download_image(self, image_url: str, save_path: Path):
        """Downloads and saves an image."""
        try:
            response = requests.get(image_url, stream=True)
            if response.status_code == 200:
                with save_path.open("wb") as file:
                    for chunk in response.iter_content(1024):
                        file.write(chunk)
                logger.success(f"[Download] Image saved: {save_path}")
            else:
                logger.warning(f"[Download] Failed to download image: {image_url}")
        except requests.RequestException as e:
            logger.error(f"[Download] Error downloading image: {e}")

    def extract_captions_from_videos(self, videos: List[dict], output_folder: str):
        """Extracts English captions from YouTube videos if available."""
        output_path = Path(output_folder)
        output_path.mkdir(parents=True, exist_ok=True)

        captions = {}
        for video in videos:
            video_id = video["videoId"]
            captions[video_id] = self.retrieve_caption(video_id, output_path)
            if captions[video_id]:
                logger.success(f"[Captions] Saved captions for {video_id}")
            else:
                logger.warning(f"[Captions] No captions found for {video_id}")

        return captions

    def extract_audio_from_videos(self, videos: List[dict], output_folder="audios"):
        """Extracts audio from YouTube videos."""
        output_path = Path(output_folder)
        output_path.mkdir(parents=True, exist_ok=True)

        for video in videos:
            video_id = video["videoId"]
            result = self.retrieve_audio(video_id, output_path)
            if result:
                logger.success(f"[Audio] Saved audio for {video_id}")

    def extract_video_from_videos(self, videos: List[dict], output_folder="videos"):
        """Extracts video with a specific format and resolution from YouTube."""
        output_path = Path(output_folder)
        output_path.mkdir(parents=True, exist_ok=True)

        for video in videos:
            video_id = video["videoId"]
            result = self.retrieve_video(video_id, output_path)
            if result:
                logger.success(f"[Video] Saved video for {video_id}")


# basic usage of this Youtube API class :)
if __name__ == "__main__":
    yt_ingestor = YoutubeAPIClient()

    # Define base output directory
    base_output = Path("resources/tests")
    base_output.mkdir(parents=True, exist_ok=True)

    paths = {
        "metadata": base_output / "metadata",
        "comments": base_output / "comments",
        "thumbnails": base_output / "thumbnails",
        "audios": base_output / "audios",
        "videos": base_output / "videos",
        "captions": base_output / "captions",
    }

    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)

    # Retrieve videos metadata
    videos = yt_ingestor.extract_videos(
        query="Chill Guy Jordans",
        max_results=3,
        use_shorts=False,
        output_folder=str(paths["metadata"]),
    )

    # Extract comments
    comment_data = yt_ingestor.extract_comments_from_videos(
        videos, max_comments=10, output_folder=str(paths["comments"])
    )

    # Download thumbnails
    thumbnail_data = yt_ingestor.extract_video_thumbnails(
        videos, output_folder=str(paths["thumbnails"])
    )

    # Extract audio files
    audio_data = yt_ingestor.extract_audio_from_videos(
        videos, output_folder=str(paths["audios"])
    )

    # Download videos
    video_data = yt_ingestor.extract_video_from_videos(
        videos, output_folder=str(paths["videos"])
    )

    # Download captions
    caption_data = yt_ingestor.extract_captions_from_videos(
        videos, output_folder=str(paths["captions"])
    )
