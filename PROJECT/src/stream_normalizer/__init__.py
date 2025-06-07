import json
import multiprocessing
import os
from typing import Callable, Dict

from kafka import KafkaConsumer, KafkaProducer
from loguru import logger

from .bluesky_post import clean_bluesky_post
from .youtube_video import clean_youtube_video

CLEANERS: Dict[str, Callable[[Dict], Dict]] = {
    "bluesky-posts": clean_bluesky_post,
    "youtube-videos": clean_youtube_video,
}


class StreamNormalizer:
    def __init__(
        self,
        input_topic: str,
        output_topic: str,
        data_handler: str,
        bootstrap_servers: str = None,
        polling_timeout: int = 1000,
        id: str = "default_consumer_id",
    ):
        self.polling_timeout = polling_timeout
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.data_handler = data_handler
        self.id = id

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode(""),
        )
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",  # Start from the end (ideal for streaming)
            enable_auto_commit=False,
        )

        if not data_handler in CLEANERS:
            raise ValueError(f"Invalid data_handler: {data_handler}. Must be one of {list(CLEANERS.keys())}")
        self.cleaner = CLEANERS.get(data_handler)

    def start_polling(self, verbose: bool = False):
        """
        Get all messages by actively listening (polling) on the brokers
        of all given topics continuously
        """
        self.subscribe()
        logger.info(f"[{self.id}-CONSUMER] Polling for new messages...")
        while True:
            try:
                messages = self.consumer.poll(timeout_ms=self.polling_timeout)
                logger.debug(f"{len(messages)} messages fetched")
                if messages:
                    for topic_partition, records in messages.items():
                        for record in records:
                            if verbose:
                                logger.debug(
                                    f"Record fetched: {record}, by process: {multiprocessing.current_process().pid}"
                                )
                            self.process_message(record)
            except Exception as e:
                logger.error(f"[{self.id}-CONSUMER] Error while polling messages: {e}")
                logger.error(f"{e}")
            except KeyboardInterrupt:
                logger.info(f"[{self.id}-CONSUMER] KeyboardInterrupt received, shutting down...")
                break

        self.close()

    def subscribe(self):
        self.consumer.subscribe([self.input_topic])
        logger.info(f"[{self.id}-CONSUMER] Subscribed to topics {self.input_topic}")

    def process_message(self, message: dict):
        raw = message.value
        cleaned = self.cleaner(raw)
        if cleaned is None:
            logger.warning(f"[{self.id}] Skipping message with missing data: {raw}")
            return

        # inject metadata so the next consumer can group by product
        cleaned["_meta"] = {"source": self.data_handler}

        self.producer.send(self.output_topic, cleaned)
        logger.debug(f"[{self.id}] → {self.output_topic}: {cleaned.get('uri', cleaned)}")

    def close(self):
        self.producer.flush()
        self.producer.close()
        self.consumer.close()
