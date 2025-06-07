import json
import multiprocessing
import os
import time
from dataclasses import dataclass
from typing import Callable, Dict, List

import yaml
from kafka import KafkaConsumer, KafkaProducer
from loguru import logger

from src.ingestion.stream import KafkaAdmin
from src.utils.company import deserialize_companies_from_json
from src.utils.hash import hash_query
from src.utils.placeholders import replace_placeholders

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
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
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

    def process_message(self, message):
        raw = message.value
        cleaned = self.cleaner(raw)
        if cleaned is None:
            logger.warning(f"[{self.id}] Skipping message with missing data: {raw}")
            return

        # inject metadata so the next consumer can group by product
        cleaned["_meta"] = {"source": self.data_handler}

        self.producer.send(self.output_topic, value=cleaned)
        self.producer.flush()
        logger.debug(f"[{self.id}] → {self.output_topic}: {cleaned.get('uri', cleaned)}")

    def close(self):
        self.producer.flush()
        self.producer.close()
        self.consumer.close()

    def __del__(self):
        # Ensure the producer and consumer are closed when the object is deleted
        self.close()
        logger.info(f"[{self.id}-CONSUMER] Closed producer and consumer.")


class StreamNormalizerOrchestrator:

    @dataclass
    class NormalizerConfig:
        name: str
        normalizer: str
        input_topic_template: str
        output_topic_template: str

    @dataclass
    class StreamNormalizerDesired:
        data_handler: str
        input_topic: str
        output_topic: str
        id: str

    @dataclass
    class StreamNormalizerPod(StreamNormalizerDesired):
        proc: multiprocessing.Process

    def __init__(self, companies_file: str, stream_configuration_file: str):
        """
        Continuously poll the stream configuration file and the existing
        companies, and start a StreamNormalizer for each company, product,
        query, and data handler.
        """
        self.companies_file = companies_file
        self.stream_configuration_file = stream_configuration_file
        self.stream_normalizers: Dict[str, StreamNormalizerOrchestrator.StreamNormalizerPod] = {}
        self.kafka_admin = KafkaAdmin(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))

        self.kafka_config = self._load_kafka_config()

    @staticmethod
    def _load_stream_normalizer_configs(stream_configuration_file: str) -> List[NormalizerConfig]:
        """
        Load the stream configurations from the given YAML file.
        """
        with open(stream_configuration_file, "r") as f:
            return [StreamNormalizerOrchestrator.NormalizerConfig(**n) for n in yaml.safe_load(f)["normalizers"]]

    @staticmethod
    def _load_kafka_config() -> Dict[str, int]:
        """
        Load Kafka configuration from environment variables or defaults.
        """
        return {
            "num_partitions": int(os.getenv("KAFKA_NUM_PARTITIONS", 1)),
            "replication_factor": int(os.getenv("KAFKA_REPLICATION_FACTOR", 1)),
        }

    def compute_desired_state(self) -> List[StreamNormalizerPod]:
        """
        Compute the desired state of the stream normalizers based on the
        current companies and stream configurations.
        """
        companies = deserialize_companies_from_json(self.companies_file)
        stream_configurations = self._load_stream_normalizer_configs(self.stream_configuration_file)
        desired_state = []
        for company in companies:
            for product in company.products:
                for query in product.keywords:
                    for config in stream_configurations:
                        context = dict(
                            company_id=company.company_id,
                            product_id=product.name,
                            query_hash=hash_query(query),
                        )

                        input_topic = replace_placeholders(config.input_topic_template, **context)
                        output_topic = replace_placeholders(config.output_topic_template, **context)
                        pod = StreamNormalizerOrchestrator.StreamNormalizerDesired(
                            data_handler=config.normalizer,
                            input_topic=input_topic,
                            output_topic=output_topic,
                            id=f"{company.company_id}-{product.name}-{hash_query(query)}-{config.normalizer}",
                        )
                        desired_state.append(pod)
        return desired_state

    def reconcile(self):
        """
        Reconcile the current state of the stream normalizers with the desired state.
        Start new normalizers for any desired state that is not currently running.
        """
        desired_state = self.compute_desired_state()
        desired_ids = {desired.id for desired in desired_state}

        for desired in desired_state:
            if not desired.id in self.stream_normalizers or not self.stream_normalizers[desired.id].proc.is_alive():

                if not self.kafka_admin.topic_exists(desired.output_topic):
                    self.kafka_admin.create_topic(
                        desired.output_topic,
                        num_partitions=self.kafka_config["num_partitions"],
                        replication_factor=self.kafka_config["replication_factor"],
                    )
                # Start a new StreamNormalizer
                sn = StreamNormalizer(
                    input_topic=desired.input_topic,
                    output_topic=desired.output_topic,
                    data_handler=desired.data_handler,
                    id=desired.id,
                )
                process = multiprocessing.Process(target=sn.start_polling, kwargs={"verbose": True}, daemon=True)
                process.start()
                self.stream_normalizers[desired.id] = StreamNormalizerOrchestrator.StreamNormalizerPod(
                    data_handler=desired.data_handler,
                    input_topic=desired.input_topic,
                    output_topic=desired.output_topic,
                    id=desired.id,
                    proc=process,
                )

        # Stop any normalizers that are no longer desired
        remove_ids = set(self.stream_normalizers.keys()) - desired_ids
        if remove_ids:
            logger.info(f"Stopping StreamNormalizers: {', '.join(remove_ids)}")
            for remove_id in remove_ids:
                pod = self.stream_normalizers.pop(remove_id, None)
                if pod and pod.proc.is_alive():
                    logger.info(f"Terminating StreamNormalizer {remove_id}")
                    pod.proc.terminate()
                    pod.proc.join()
        logger.info("Reconciliation complete.")

    def run(self):
        """
        Continuously reconcile the stream normalizers with the desired state.
        """
        logger.info("Starting StreamNormalizerOrchestrator...")
        while True:
            try:
                self.reconcile()
                logger.info("Reconciliation complete, sleeping for 10 seconds...")
                time.sleep(10)
            except KeyboardInterrupt:
                logger.info("KeyboardInterrupt received, shutting down...")
                break
            except Exception as e:
                logger.error(f"Error during reconciliation: {e}")
