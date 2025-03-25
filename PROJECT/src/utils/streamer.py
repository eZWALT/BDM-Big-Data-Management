from kafka import KafkaProducer, KafkaConsumer
import time
import json
from loguru import logger
from abc import abstractmethod, ABC
from typing import List
from src.utils.config import ConfigManager

# ===----------------------------------------------------------------------===#
# Base Kafka Producers/Consumers                                              #
#                                                                             #
# This class represents a "Consumer" for the Youtube API following the Kafka  #
# Producer-Consumer and publication/subscription patterns. Fetches data from  #
# the broker kafka server managed by zookeeper from the producers
#                                                                             #
# Author: Walter J.T.V                                                        #
# ===----------------------------------------------------------------------===#


# This producer is identified by ID
class Producer(ABC):
    def __init__(self, id: str):
        self.cfg = ConfigManager("config/streaming.yaml")
        self.producer = KafkaProducer(
            bootstrap_servers=self.cfg._load_config()["kafka"]["bootstrap_servers"],
            value_serializer=(lambda v: json.dumps(v).encode("utf-8")),
        )
        self.id = id

    def send_message(self, topic: str, message: dict):
        self.producer.send(topic, value=message)

    # Produce messages. This must be implemented in the subclasses
    @abstractmethod
    def produce(self, *args, **kwargs):
        pass

    # Generic driver run method, this should be refactored with tasks
    @abstractmethod
    def run(self, *args, **kwargs):
        pass

    def close(self):
        self.producer.flush()
        self.producer.close()
        logger.info(f"[{self.id} - PRODUCER] Kafka producer closed.")


class Consumer(ABC):
    def __init__(self, id: str, polling_timeout: int = 1000):
        self.id = id
        self.cfg = ConfigManager(config_path="config/streaming.yaml")
        self.polling_timeout = polling_timeout
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.cfg["kafka"]["bootstrap_servers"],
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",  # Start from the beginning
            enable_auto_commit=False,
        )

    def subscribe(self, topics: List[str]):
        self.consumer.subscribe(topics)
        logger.info(f"{id}-[]")
