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
# the broker kafka server managed by zookeeper from the producers             #
# NOTE: all times are represented using miliseconds rather than seconds       #                              
#                                                                             #
# Author: Walter J.T.V                                                        #
# ===----------------------------------------------------------------------===#


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
        logger.info(f"[{self.id}-PRODUCER] Kafka producer closed.")


class Consumer(ABC):
    # Actively listen every 10 seconds
    def __init__(self, id: str, polling_timeout: int = 10000):
        self.id = id
        self.cfg = ConfigManager(config_path="config/streaming.yaml")
        self.polling_timeout = polling_timeout
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.cfg._load_config()["kafka"]["bootstrap_servers"],
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",  # Start from the beginning
            enable_auto_commit=False,
        )

    def subscribe(self, topics: List[str]):
        self.consumer.subscribe(topics)
        logger.info(f"[{self.id}-CONSUMER] Subscribed to topics {topics}")
    
    
    # Get all messages that are stored currently in the brokers
    # for all topics associated with the consumer
    def consume(self):
        logger.info(f"[{self.id}-CONSUMER] Consuming all available messages for all topics")
        try:
            for topic_partition in self.consumer.assignment():
                # Seek to the beginning (0) for each partition
                self.consumer.seek_to_beginning(topic_partition)
            
            messages = self.consumer.poll(timeout_ms=self.polling_timeout)
            if messages:
                for topic_partition, records in messages.items():
                    for record in records:
                        self.process_message(record)
        except Exception as e:
            logger.error(f"[{self.id}-CONSUMER] Error while consuming messages: {e}")
        finally:
            self.close()

    
    # Get all messages by actively listening (polling) on the brokers
    # of all given topics continiously
    def poll(self, verbose: bool = False):
        logger.info(f"[{self.id}-CONSUMER] Polling for new messages...")
        try:
            while True:

                messages = self.consumer.poll(timeout_ms=self.polling_timeout)

                if messages:
                    for topic_partition, records in messages.items():
                        for record in records:
                            if verbose:
                                logger.debug(f"Record fetched: {record}")
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
        
    # Generic driver run method, this should be refactored with tasks
    # here real-time streaming (polling) logic can be implemented
    @abstractmethod
    def run(self, *args, **kwargs):
        pass

    def close(self):
        self.consumer.close()
        logger.info(f"[{self.id}-CONSUMER] Consumer closed.")
