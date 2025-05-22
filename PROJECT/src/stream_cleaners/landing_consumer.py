from kafka import KafkaProducer
from loguru import logger
import json, re, os
from typing import Dict, Callable

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

from src.analysis.streaming import StreamConsumer
from src.stream_cleaners import (
    bluesky_post_stream,
    bluesky_like_stream,
    youtube_comm_stream,
    youtube_vid_stream,
)

cleaners = {
    "bluesky": {
        "posts": bluesky_post_stream,
        "likes": bluesky_like_stream,
    },
    "youtube": {
        "videos": youtube_vid_stream,
        "comments": youtube_comm_stream,
    },
}

class LandingConsumer(StreamConsumer):
    def __init__(self):
        super().__init__(id)
        # prepare a producer to landing-* topics
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode(""),
        )

    def process_message(self, message):
        topic = message.topic

        parts = topic.split("-")
        # topic: <source>-<query_hash>-<data_type>
        if len(parts) != 3:
            logger.warning(f"[{self.id}] unexpected topic format: {topic}")
            return

        _, source, query_hash, data_type = parts

        # lookup the right cleaner
        source_map = self.CLEANERS.get(source)
        if not source_map:
            logger.warning(f"[{self.id}] no cleaners for source: {source}")
            return

        cleaner = source_map.get(data_type)
        if not cleaner:
            logger.warning(f"[{self.id}] no cleaner for data_type: {data_type}")
            return

        raw = message.value
        cleaned = cleaner(raw)
        if cleaned is None:
            return

        # inject metadata so the next consumer can group by product
        cleaned["_meta"] = {
            "source":     source,
            "query_hash": query_hash,
            "type":       data_type,
        }

        out_topic = f"landing-{source}-{query_hash}-{data_type}"
        self.producer.send(out_topic, cleaned)
        logger.debug(f"[{self.id}] → {out_topic}: {cleaned.get('uri', cleaned)}")

    def close(self):
        super().close()
        self.producer.flush()
        self.producer.close()