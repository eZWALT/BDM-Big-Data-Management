import io
from datetime import datetime
from typing import Generator, List, Optional

import requests
from loguru import logger

from src.ingestion.connectors.models.youtube import VideoBasicData, VideoComment
from src.ingestion.connectors.youtube_client import YoutubeAPIClient

from . import BatchProducer, DBConnection, FilesDBConnection

# ===-----------------------------------------------------------------------===#
# YouTube Batch Producer                                                       #
#                                                                              #
# Author: Marc Parcerisa                                                       #
# ===-----------------------------------------------------------------------===#


class YoutubeBatchProducer(BatchProducer):
    def __init__(self):
        super().__init__()
        self.client = YoutubeAPIClient()

    def _video_data_generator(
        self, query: str, since: Optional[str] = None, until: Optional[str] = None, batch_size: int = 10
    ) -> Generator[List[VideoBasicData], None, None]:
        posts, cursor = self.client.retrieve_videos_basic_data(query=query, limit=batch_size, since=since, until=until)
        yield posts
        while cursor:
            posts, cursor = self.client.retrieve_videos_basic_data(
                query=query, limit=batch_size, since=since, until=until, cursor=cursor
            )
            yield posts

    def _fetch_and_update_video_stats(self, videos: List[VideoBasicData]):
        """
        Download the video stats for the given videos and update their dictionaries.
        Perform the update in place.
        """
        stats = self.client.retrieve_video_statistics([video["videoId"] for video in videos])
        for video in videos:
            if video["videoId"] in stats:
                video.update(stats[video["videoId"]])

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

    def _comments_generator(
        self, videos: List[VideoBasicData], batch_size: int = 10
    ) -> Generator[List[VideoComment], None, None]:
        for video in videos:
            video_id = video["videoId"]
            comments, cursor = self.client.retrieve_top_level_comments(video_id, limit=batch_size)
            yield comments
            while cursor:
                comments, cursor = self.client.retrieve_top_level_comments(video_id, limit=batch_size, cursor=cursor)
                yield comments

    def _fetch_and_load_thumbnail(self, video: VideoBasicData, db_connection: FilesDBConnection):
        """
        Download the video thumbnail and load it into the database.
        """
        thumbnail_url = video["thumbnail"]
        try:
            response = requests.get(thumbnail_url)
            response.raise_for_status()
            db_connection.add((video["videoId"] + ".jpg", io.BytesIO(response.content)))
        except requests.RequestException as e:
            logger.warning(f"Failed to download thumbnail for video {video['videoId']}: {e}")
            return

    def _fetch_and_load_video(self, video: VideoBasicData, db_connection: FilesDBConnection):
        """
        Download the video and load it into the database.
        """
        try:
            video_stream = self.client.get_video_stream(video["videoId"])
            video_buffer = io.BytesIO()
            video_stream.stream_to_buffer(video_buffer)
            video_buffer.seek(0)
            db_connection.add((video["videoId"] + ".mp4", video_buffer))
        except Exception as e:
            logger.warning(f"Failed to download video {video['videoId']}: {e}")
            return

    def _fetch_and_load_audio(self, video: VideoBasicData, db_connection: FilesDBConnection):
        """
        Download the audio and load it into the database.
        """
        try:
            audio_stream = self.client.get_audio_stream(video["videoId"])
            audio_buffer = io.BytesIO()
            audio_stream.stream_to_buffer(audio_buffer)
            audio_buffer.seek(0)
            db_connection.add((video["videoId"] + ".mp3", audio_buffer))
        except Exception as e:
            logger.warning(f"Failed to download audio {video['videoId']}: {e}")
            return

    def produce(
        self,
        query: str,
        utc_since: Optional[datetime],
        utc_until: Optional[datetime],
        videos_batch_size: int = 10,
        comments_batch_size: int = 10,
        video_data_db: DBConnection = None,
        comments_db: DBConnection = None,
        videos_db: DBConnection = None,
        audios_db: DBConnection = None,
        thumbnails_db: DBConnection = None,
    ):
        """
        Produce data from the YouTube API using the provided query.
        """
        if utc_since is not None:
            utc_since = utc_since.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if utc_until is not None:
            utc_until = utc_until.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        total_videos = 0
        total_comments = 0
        for video_minibatch in self._video_data_generator(
            query, since=utc_since, until=utc_until, batch_size=videos_batch_size
        ):
            self._fetch_and_update_video_stats(video_minibatch)
            self._fetch_and_update_video_captions(video_minibatch)
            video_data_db.add_many(video_minibatch)
            total_videos += len(video_minibatch)
            for comments_minibatch in self._comments_generator(video_minibatch, batch_size=comments_batch_size):
                comments_db.add_many(comments_minibatch)
                total_comments += len(comments_minibatch)
            for video in video_minibatch:
                self._fetch_and_load_thumbnail(video, thumbnails_db)
                self._fetch_and_load_video(video, videos_db)
                self._fetch_and_load_audio(video, audios_db)
        return total_videos + total_comments
