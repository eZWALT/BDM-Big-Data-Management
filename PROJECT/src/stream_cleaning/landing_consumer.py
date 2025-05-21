from kafka import KafkaProducer
from loguru import logger
import json, re, os
from typing import Dict, Callable

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

from src.analysis.streaming import StreamConsumer
from stream_cleaning import (
    youtube_stream_cleaner,
    bluesky_stream_cleaner
)

RAW_PREFIXES = {
    "raw-bluesky-": bluesky_stream_cleaner,
    "raw-youtube-": youtube_stream_cleaner
}

class LandingConsumer(StreamConsumer):
    def __init__(self):
        super().__init__(id="landing")
        # subscribe to all topics
        self.subscribe(list(RAW_PREFIXES.keys()))
        # prepare a producer to landing-* topics
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        def process_message(self, message):
        topic = message.topic
        value = message.value

        # pick the cleaner based on raw- prefix
        for prefix, cleaner in RAW_PREFIXES.items():
            if topic.startswith(prefix):
                try:
                    cleaned = cleaner(value)
                except Exception:
                    logger.exception(f"[landing] failed cleaning record from {topic}")
                    return
                landing_topic = re.sub(rf"^{prefix}", "landing-", topic)
                self.producer.send(landing_topic, cleaned)
                logger.info(f"[landing] {topic} → {landing_topic}")
                return