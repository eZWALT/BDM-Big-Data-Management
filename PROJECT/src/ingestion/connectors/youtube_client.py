import json
import os
import pprint
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import *

import requests
from loguru import logger
from pytubefix import Caption, Stream, YouTube

if __name__ == "__main__":
    import sys

    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from src.ingestion.connectors.models.youtube import *
from src.utils.config import ConfigManager

# ===-----------------------------------------------------------------------===#
# YouTube API Client                                                           #
#                                                                              #
# This class provides an interface to interact with the YouTube Data API,      #
# enabling the retrieval of video metadata, comments, thumbnails, videos,      #
# and audio. Captions are not available via the API and are retrieved          #
# using pytubefix.                                                             #
#                                                                              #
# The class is structured around:                                              #
#   - **Retrieve Methods**: Direct API calls to fetch raw data.                #
#   - **Extract Methods**: High-level wrappers that process, store, and        #
#     return structured data while generating files as needed.                 #
#                                                                              #
# Designed for extensibility, this client supports metadata extraction,        #
# media downloads, and API authentication with exponential backoff for         #
# robustness.                                                                  #
#                                                                              #
# Author: Walter J.T.V, Marc Parcerisa                                         #
# ===-----------------------------------------------------------------------===#


class MaxRetriesError(Exception):
    """Custom exception for maximum retries exceeded."""

    pass


class YoutubeAPIClient:
    def __init__(self):
        super().__init__()
        self.api_name = "youtube"
        self.config_manager = ConfigManager(config_path="configuration/api.yaml")
        self.base_url = self.config_manager.get_api_base_url(self.api_name)
        self.authenticate()

    def authenticate(self):
        self.api_key = self.config_manager.get_api_credentials(self.api_name)["api_key"]
        logger.success("Successfully authenticated to YouTube!")

    def fetch(
        self,
        endpoint: str,
        api_params: dict,
        max_retries: int = 2,
        backoff: int = 1,
        strict_raise: bool = False,
    ) -> dict:
        """General method to fetch data from YouTube API with retry logic."""
        url = f"{self.base_url}{endpoint}"
        error = Exception("Unknown error")
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(url, params=api_params)
                if strict_raise:
                    response.raise_for_status()
                if response.status_code == 200:
                    return response.json()
                error = Exception(
                    f"[Fetch] API request failed with status code {response.status_code}: {response.text}"
                )
                logger.warning(f"[Fetch] Attempt {attempt}/{max_retries} failed (Status {response.status_code})")
                time.sleep(backoff**attempt)
            except requests.RequestException as e:
                error = e
                raise requests.RequestException(f"[Fetch] API request error: {e}") from e
        raise MaxRetriesError(
            f"[Fetch] API request failed after {max_retries} retries for url {url}: {error}"
        ) from error

    def retrieve_videos_basic_data(
        self,
        query: str,
        limit: int = 10,
        cursor: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        order: str = "relevance",
    ) -> Tuple[List[VideoBasicData], Optional[str]]:
        """Fetches basic video metadata from search endpoint"""
        params = {
            "part": "snippet",
            "q": query,
            "type": "video",
            "maxResults": limit,
            "order": order,
            "videoCaption": "any",
            "videoDuration": "any",
            "key": self.api_key,
        }
        if cursor:
            params["pageToken"] = cursor
        if since:
            params["publishedAfter"] = since
        if until:
            params["publishedBefore"] = until

        response: VideoSearchResponse = self.fetch("search", params)
        posts = response.get("items", [])
        total_posts = len(posts)
        while limit is None or total_posts < limit and response.get("nextPageToken"):
            params["pageToken"] = response.get("nextPageToken")
            if limit:
                params["maxResults"] = limit - total_posts
            response: VideoSearchResponse = self.fetch("search", params)
            posts.extend(response.get("items", []))
            total_posts += len(response.get("items", []))

        return list(
            {
                "title": item["snippet"]["title"],
                "description": item["snippet"].get("description", ""),
                "url": f"https://www.youtube.com/watch?v={item['id']['videoId']}",
                "videoId": item["id"]["videoId"],
                "channel": item["snippet"]["channelTitle"],
                "publishedAt": item["snippet"]["publishedAt"],
                "tags": ",".join(item["snippet"].get("tags", [])),
                "thumbnail": item["snippet"]["thumbnails"].get("high", {}).get("url", ""),
            }
            for item in posts
        ), response.get("nextPageToken", None)

    def retrieve_video_statistics(self, video_ids: List[str]) -> dict[str, VideoStatistics]:
        """Fetch additional statistical metadata using videos endpoint"""
        params = {
            "part": "statistics,contentDetails",
            "id": ",".join(video_ids),  # Pass all video IDs as CSV
            "key": self.api_key,
        }

        response: VideoDetailsResponse = self.fetch("videos", params)
        items = response.get("items", [])
        while response.get("nextPageToken"):
            params["pageToken"] = response.get("nextPageToken")
            response: VideoDetailsResponse = self.fetch("search", params)
            items.extend(response.get("items", []))

        return {
            item["id"]: {
                "viewCount": int(item["statistics"].get("viewCount", 0)),
                "likeCount": int(item["statistics"].get("likeCount", 0)),
                "commentCount": int(item["statistics"].get("commentCount", 0)),
                "duration": item["contentDetails"]["duration"],
                "definition": item["contentDetails"]["definition"],
            }
            for item in items
        }

    def retrieve_top_level_comments(
        self, video_id: str, limit: Optional[int] = 10, cursor: Optional[str] = None, order: str = "relevance"
    ) -> Tuple[List[VideoComment], Optional[str]]:
        """Fetches top-level comments for a video."""
        api_params = {
            "part": "id,snippet",
            "videoId": video_id,
            "key": self.api_key,
            "order": order,
        }
        if limit:
            api_params["maxResults"] = limit
        if cursor:
            api_params["pageToken"] = cursor

        response: CommentThreadResponse = self.fetch("commentThreads", api_params, strict_raise=False)
        items = response.get("items", [])
        total_items = len(items)
        while limit is None or total_items < limit and response.get("nextPageToken"):
            api_params["pageToken"] = response.get("nextPageToken")
            if limit:
                api_params["maxResults"] = limit - total_items
            response: CommentThreadResponse = self.fetch("commentThreads", api_params, strict_raise=False)
            items.extend(response.get("items", []))
            total_items += len(response.get("items", []))

        return list(
            {
                "channelName": item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                "channelId": item["snippet"]["topLevelComment"]["snippet"]
                .get("authorChannelId", {})
                .get("value", "Unknown"),
                "comment": item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                "likes": item["snippet"]["topLevelComment"]["snippet"].get("likeCount", 0),
                "replies": item["snippet"].get("totalReplyCount", 0),
                "publishedAt": item["snippet"]["topLevelComment"]["snippet"].get("publishedAt", "Unknown"),
                "videoId": video_id,
                "threadId": item["id"],
            }
            for item in items
        ), response.get("nextPageToken", None)

    def get_audio_stream(self, video_id: str) -> Stream:
        """Retrieve the audio stream for a given video ID."""
        yt = YouTube(f"https://www.youtube.com/watch?v={video_id}")
        ys = yt.streams.get_audio_only()
        return ys

    def get_video_stream(self, video_id: str) -> Stream:
        """Retrieve the video stream for a given video ID."""
        yt = YouTube(f"https://www.youtube.com/watch?v={video_id}")
        ys = yt.streams.get_highest_resolution()
        return ys

    def get_caption_stream(self, video_id: str) -> Caption:
        yt = YouTube(f"https://www.youtube.com/watch?v={video_id}")
        captions = {caption.code: caption for caption in yt.captions}
        en_caption = captions.get("a.en") or next(
            (caption for code, caption in captions.items() if code.startswith("en")),
            None,
        )
        if not en_caption:
            raise ValueError(f"No English captions available for video {video_id}")
        return en_caption

    # ------------------------------------------------------------------------------------------------------------------

    # These 3 retrieval methods work with pytube scrapping rather than the youtube API
    def retry_pytubefix_operation(self, operation, *args, max_retries=3, backoff=2, **kwargs):
        """Helper method to retry pytubefix operations with exponential backoff."""
        retry_count = 0
        while retry_count < max_retries:
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                retry_count += 1
                logger.warning(f"Operation failed (Attempt {retry_count}/{max_retries}): {e}")
                if retry_count >= max_retries:
                    logger.error(f"Operation failed after {max_retries} retries.")
                    raise e
                sleep_time = backoff**retry_count + random.uniform(0, 1)  # Exponential backoff with jitter
                logger.info(f"Retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)

    def extract_audio(self, video_id: str, output_folder: str) -> str:
        """Retrieve audio from a YouTube video in m4a format."""
        ys = self.get_audio_stream(video_id)
        path = self.retry_pytubefix_operation(ys.download, filename=f"{video_id}.mp3", output_path=output_folder)
        return path

    def extract_video(self, video_id: str, output_folder: str) -> str:
        """Retrieve a YouTube video with the highest resolution available."""
        ys = self.get_video_stream(video_id)
        path = self.retry_pytubefix_operation(ys.download, filename=f"{video_id}.mp4", output_path=output_folder)
        return path

    def extract_captions(self, video_id: str, output_folder: str):
        """Retrieve and download English captions from a YouTube video."""
        try:
            en_caption = self.get_caption_stream(video_id)
        except ValueError as e:
            logger.warning(f"No captions available for video {video_id}: {e}")
            return None
        path = self.retry_pytubefix_operation(en_caption.download, title=video_id, output_path=output_folder)
        return path

    def extract_videos(
        self,
        query: str,
        max_results: int = 10,
        output_folder: str = "videos_metadata",
        save: bool = False,
        order: str = "relevance",
    ):
        """Retrieve videos metadata, merge statistics, and save each video as a separate JSON file."""
        output_path = Path(output_folder)
        output_path.mkdir(parents=True, exist_ok=True)

        videos, _ = self.retrieve_videos_basic_data(query, limit=max_results, order=order)
        video_ids = [video["videoId"] for video in videos]
        metadata = self.retrieve_video_statistics(video_ids)

        for video in videos:
            video.update(metadata.get(video["videoId"], {}))
            if save:
                file_path = output_path / f"{video['videoId']}.json"
                file_path.write_text(json.dumps(video, indent=4, ensure_ascii=False), encoding="utf-8")
                logger.success(f"[Metadata] Saved video metadata: {file_path}")

        return videos

    def extract_comments_from_videos(
        self,
        videos: list,
        max_comments: int = 10,
        output_folder: str = "comments",
        save: bool = False,
        order: str = "relevance",
    ):
        """Extracts comments from a given list of video metadata and saves them as JSON."""
        output_path = Path(output_folder)
        output_path.mkdir(parents=True, exist_ok=True)

        results = {
            video["videoId"]: self.retrieve_top_level_comments(video["videoId"], max_comments, order=order)[0]
            for video in videos
        }

        if save:
            for video_id, comments in results.items():
                file_path = output_path / f"{video_id}.json"
                file_path.write_text(json.dumps(comments, indent=4, ensure_ascii=False), encoding="utf-8")
                logger.success(f"[Comments] Saved {len(comments)} comments for {video_id}: {file_path}")

        return results

    def extract_video_thumbnails(self, videos: list, output_folder="thumbnails"):
        """Extracts high-quality thumbnails concurrently."""
        output_path = Path(output_folder)
        output_path.mkdir(parents=True, exist_ok=True)

        results = []
        with ThreadPoolExecutor() as executor:
            futures = []
            for video in videos:
                video_id = video["videoId"]
                thumbnail_url = video["thumbnail"]
                if thumbnail_url:
                    futures.append(executor.submit(self.download_image, thumbnail_url, output_path / f"{video_id}.jpg"))

            for future in as_completed(futures):
                future.result()  # Wait for all download tasks to complete

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

    def extract_captions_from_videos(self, videos: List[dict], output_folder: str) -> dict:
        """Extracts English captions from YouTube videos if available."""
        output_path = Path(output_folder)
        output_path.mkdir(parents=True, exist_ok=True)

        captions = {}
        for video in videos:
            video_id = video["videoId"]
            caption_path = self.extract_captions(video_id, output_path)
            if caption_path:
                captions[video_id] = caption_path
                logger.success(f"[Captions] Saved captions for {video_id}")
            else:
                logger.warning(f"[Captions] No captions found for {video_id}")

        return captions  # Only includes videos with successfully extracted captions

    def extract_audio_from_videos(self, videos: List[dict], output_folder="audios"):
        """Extracts audio from YouTube videos concurrently."""
        output_path = Path(output_folder)
        output_path.mkdir(parents=True, exist_ok=True)
        results = []

        with ThreadPoolExecutor() as executor:
            futures = []
            for video in videos:
                video_id = video["videoId"]
                futures.append(executor.submit(self.extract_audio, video_id, output_path))

            for future in as_completed(futures):
                result = future.result()  # Wait for the result
                if result:
                    results.append(
                        {
                            "video_id": video_id,
                            "audio_path": result,
                        }
                    )
                    logger.success(f"[Audio] Saved audio for {video_id}")

        return results

    def extract_video_from_videos(self, videos: List[dict], output_folder="videos"):
        """Extracts video with a specific format and resolution from YouTube concurrently."""
        output_path = Path(output_folder)
        output_path.mkdir(parents=True, exist_ok=True)
        results = []

        with ThreadPoolExecutor() as executor:
            futures = []
            for video in videos:
                video_id = video["videoId"]
                futures.append(executor.submit(self.extract_video, video_id, output_path))

            for future in as_completed(futures):
                result = future.result()  # Wait for the result
                if result:
                    results.append(
                        {
                            "video_id": video_id,
                            "video_path": result,
                        }
                    )
                    logger.success(f"[Video] Saved video for {video_id}")

        return results


# There are 2 order methods: "relevance" and "time"/"date"
# basic usage of this Youtube API class :)
if __name__ == "__main__":
    client = YoutubeAPIClient()

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
    videos = client.extract_videos(
        query="Machine Learning", max_results=3, output_folder=str(paths["metadata"]), order="relevance"
    )

    # Extract comments
    comment_data = client.extract_comments_from_videos(
        videos, max_comments=10, output_folder=str(paths["comments"]), order="time"
    )

    # Download thumbnails
    thumbnail_data = client.extract_video_thumbnails(videos, output_folder=str(paths["thumbnails"]))

    # Extract audio files
    audio_data = client.extract_audio_from_videos(videos, output_folder=str(paths["audios"]))

    # Download videos
    video_data = client.extract_video_from_videos(videos, output_folder=str(paths["videos"]))

    # Download captions
    caption_data = client.extract_captions_from_videos(videos, output_folder=str(paths["captions"]))
