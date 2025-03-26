from kafka import KafkaProducer, KafkaConsumer
import time
import json
from loguru import logger
from abc import abstractmethod, ABC
from typing import List
from enum import Enum
import multiprocessing

from src.utils.config import ConfigManager

# ===----------------------------------------------------------------------===#
# Base Kafka Producers/Consumers                                              #
#                                                                             #
# This class represents a "Consumer" for the Youtube API following the Kafka  #
# Producer-Consumer and publication/subscription patterns. Fetches data from  #
# the broker kafka server managed by zookeeper from the producers             #
# NOTE: all times are represented using miliseconds rather than seconds       # 
#                                                                             #
# Stream Consumer --> reads from newly produced data (Streaming)              #              
# Read Consumer ----> reads from all produced data                            #              
#                                                                             #
# Author: Walter J.T.V                                                        #
# ===----------------------------------------------------------------------===#



# Enumeration for API data pulling strategy
class IngestionStrategy(Enum):
    RELEVANCE = "relevance"
    NEWEST = "newest"


# This producer is identified by ID
class Producer(ABC):
    # To stream-produce in real time 60s per request is more than enough
    def __init__(self, id: str, polling_timeout: int = 60000):
        self.cfg = ConfigManager("config/streaming.yaml")
        self.producer = KafkaProducer(
            bootstrap_servers=self.cfg._load_config()["kafka"]["bootstrap_servers"],
            value_serializer=(lambda v: json.dumps(v).encode("utf-8")),
        )
        self.id = id
        self.polling_timeout = polling_timeout

    def send_message(self, topic: str, message: dict):
        self.producer.send(topic, value=message)

    # Produce messages. This must be implemented in the subclasses and recieves a query
    # and an ingestion strategy as an input. More parameters can be added
    @abstractmethod
    def produce(self, query: str, strategy: IngestionStrategy = IngestionStrategy.RELEVANCE, *args, **kwargs):
        pass

    # Generic driver polling method that continiously generates data using "produce" method 
    # by a simple active polling loop
    @abstractmethod
    def poll(self, query: str,  strategy: IngestionStrategy = IngestionStrategy.NEWEST, *args, **kwargs):
        pass

    def close(self):
        self.producer.flush()
        self.producer.close()
        logger.info(f"[{self.id}-PRODUCER] Kafka producer closed.")


class Consumer(ABC):
    # Actively listen every 10 seconds
    def __init__(self, id: str, polling_timeout: int = 1000):
        self.id = id
        self.cfg = ConfigManager(config_path="config/streaming.yaml")
        self.polling_timeout = polling_timeout
        # We will 
        self.read_consumer = KafkaConsumer(
            bootstrap_servers=self.cfg._load_config()["kafka"]["bootstrap_servers"],
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",  # Start from the beggining
            enable_auto_commit=False,
        )
        self.stream_consumer = KafkaConsumer(
            bootstrap_servers=self.cfg._load_config()["kafka"]["bootstrap_servers"],
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",  # Start from the end (ideal for streaming)
            enable_auto_commit=False,
        )

    def subscribe(self, topics: List[str]):
        self.read_consumer.subscribe(topics)
        self.stream_consumer.subscribe(topics)
        logger.info(f"[{self.id}-CONSUMER] Subscribed to topics {topics}")
    
    
    # Get all messages that are stored currently in the brokers
    # for all topics associated with the consumer
    def consume(self):
        logger.info(f"[{self.id}-CONSUMER] Consuming all available messages for all topics")
        try:
            for topic_partition in self.read_consumer.assignment():
                # Seek to the beginning (0) for each partition
                self.read_consumer.seek_to_beginning(topic_partition)
            
            messages = self.read_consumer.poll(timeout_ms=self.polling_timeout)
            if messages:
                for topic_partition, records in messages.items():
                    for record in records:
                        self.process_message(record)
        except Exception as e:
            logger.error(f"[{self.id}-CONSUMER] Error while consuming messages: {e}")


    
    # Get all messages by actively listening (polling) on the brokers
    # of all given topics continiously
    def poll(self, verbose: bool = False):
        logger.info(f"[{self.id}-CONSUMER] Polling for new messages...")
        try:
            while True:

                messages = self.stream_consumer.poll(timeout_ms=self.polling_timeout)
                if messages:
                    for topic_partition, records in messages.items():
                        for record in records:
                            if verbose:
                                logger.debug(f"Record fetched: {record}, by process: {multiprocessing.current_process().pid}")
                            self.process_message(record)
        except Exception as e:
            logger.error(f"[{self.id}-CONSUMER] Error while polling messages: {e}")
            logger.error(f"{e}")
        finally:
            self.close()
    
    #Process behaviour of a single Kafka message 
    @abstractmethod
    def process_message(self, message):
        pass
        
    def close(self):
        self.read_consumer.close()
        self.stream_consumer.close()
        logger.info(f"[{self.id}-CONSUMER] Consumer closed.")
