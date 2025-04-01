import time
from typing import List

from loguru import logger

from src.ingestion.connectors.models.youtube import VideoBasicData
from src.ingestion.connectors.youtube_client import YoutubeAPIClient
from src.ingestion.stream import StreamProducer

# ===-----------------------------------------------------------------------===#
# Youtube Streaming Producer                                                   #
#                                                                              #
# This class represents a "Producer" for the Youtube API following the Kafka   #
# Producer-Consumer and publication/subscription patterns. It continuously     #
# polls the Youtube API for new videos matching a given query and sends them   #
# to a Kafka topic.                                                            #
#                                                                              #
# Author: Marc Parcerisa                                                       #
# ===-----------------------------------------------------------------------===#


class YoutubeStreamProducer(StreamProducer):
    def __init__(self, bootstrap_servers, topic):
        super().__init__(bootstrap_servers, topic)
        self._newest_video_id = None
        self.client = YoutubeAPIClient()

    def poll_new_videos(self, query: str, polling_size: int = 100) -> List[VideoBasicData]:
        """
        Poll the Youtube API for new videos matching the query.
        This method retrieves the most recent videos and returns them as a list.
        It also updates the internal state with the newest video ID.
        """
        if not self._newest_video_id:
            # If no video ID is set, fetch the latest video
            posts, cursor = self.client.retrieve_videos_basic_data(query=query, limit=1, order="date")  # Newest video
            if posts:
                self._newest_video_id = posts[0]["videoId"]
                logger.debug(
                    f"[YOUTUBE STREAM PRODUCER] Updated polling cursor for query '{query}' to '{self._newest_video_id}'."
                )
            else:
                logger.warning(f"[YOUTUBE STREAM PRODUCER] Could not update polling cursor for query '{query}'.")
            return []  # Don't return the first video, just set the ID, we'll return the next videos that are published
        else:
            # If a video ID is set, poll for new videos, and compare with the last one
            posts, cursor = self.client.retrieve_videos_basic_data(query=query, limit=polling_size, order="date")
            result = []
            for post in posts:
                if post["videoId"] == self._newest_video_id:
                    break
                result.append(post)
            else:
                # This will be executed if the loop is not broken, meaning we have more new videos
                # than the batch size. This means that we're losing videos.
                logger.warning(
                    f"[YOUTUBE STREAM PRODUCER] Rate of publishing for query {query} is too high and is outpacing the "
                    "polling rate. Consider increasing the batch size or the polling rate."
                )

            if result:
                self._newest_video_id = result[0]["videoId"]
                logger.debug(
                    f"[YOUTUBE STREAM PRODUCER] Updated polling cursor for query '{query}' to '{self._newest_video_id}'."
                )
            else:
                logger.debug(f"[YOUTUBE STREAM PRODUCER] No new videos for query '{query}'.")
            return result

    def _fetch_and_update_video_stats(self, videos: List[VideoBasicData]):
        """
        Download the video stats for the given videos and update their dictionaries.
        Perform the update in place.
        """
        stats = self.client.retrieve_video_statistics([video["videoId"] for video in videos])
        for video in videos:
            if video["videoId"] in stats:
                video["statistics"] = stats[video["videoId"]]

    def _fetch_and_update_video_captions(self, videos: List[VideoBasicData]):
        """
        Download the video captions (if available) for the given videos and update their dictionaries.
        Perform the update in place.
        """
        for video in videos:
            try:
                captions_stream = self.client.get_caption_stream(video["videoId"])
            except Exception as e:
                continue
            video["captions"] = captions_stream.generate_srt_captions()

    def produce(self, query: str, polling_size: int = 100, polling_interval: int = 60):
        while True:
            videos = self.poll_new_videos(query, polling_size)
            if videos:
                self._fetch_and_update_video_stats(videos)
                self._fetch_and_update_video_captions(videos)
                for video in videos:
                    self.send_message(video)
            time.sleep(polling_interval)
