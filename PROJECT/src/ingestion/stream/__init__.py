import importlib
import json
import os
import time
import traceback
from abc import ABC, abstractmethod
from hashlib import sha256
from multiprocessing import Process
from re import finditer
from typing import *

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from loguru import logger

from src.utils.hash import hash_query

if __name__ == "__main__":
    import os
    import sys

    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    sys.path.append(root)
    os.chdir(root)

from src.utils.config import ConfigManager
from src.utils.task import Task

# ===-----------------------------------------------------------------------===#
# Streaming Producing Tasks                                                    #
#                                                                              #
#                                                                              #
# Author: Marc Parcerisa, Walter J.T.V                                         #
# ===-----------------------------------------------------------------------===#

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


# This producer is identified by ID
class StreamProducer(ABC):
    # To stream-produce in real time 60s per request is more than enough
    def __init__(self, bootstrap_servers: str, topic: str):
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=(lambda v: json.dumps(v).encode("utf-8")),
        )
        self._topic = topic

    def send_message(self, message: dict):
        self._producer.send(self._topic, value=message)

    def produce_forever(self, query: str, *args, **kwargs):
        """
        Continuously produce data to the Kafka topic.
        This method runs indefinitely, producing data based on the provided query.
        It handles exceptions and restarts the producer if an error occurs.
        """
        matches = finditer(".+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)", self.__class__.__name__)
        cls_name = " ".join(m.group(0) for m in matches).upper()
        while True:
            try:
                logger.info(f"[{cls_name}] Starting stream producer for query '{query}'...")
                self.produce(query, *args, **kwargs)
            except KeyboardInterrupt as e:
                logger.info(f"[{cls_name}] Producer for query '{query}' interrupted. Exiting...")
                self.close()
                raise e
            except Exception as e:
                logger.warning(f"[{cls_name}] Error in producer for query '{query}': {e}. Restarting...")
                logger.debug(traceback.format_exc())
                time.sleep(5)  # Wait before restarting to avoid tight loop
                continue
            else:
                logger.info(f"[{cls_name}] Producer for query '{query}' finished successfully.")
                break
        self.close()

    @abstractmethod
    def produce(self, query: str, *args, **kwargs):
        """
        Produce data continuously to the Kafka topic.
        This method should be overridden by subclasses to implement specific
        data production logic.

        Children classes can make use of the send_message method to send messages
        to the Kafka topic.
        """
        pass

    def close(self):
        self._producer.flush()
        self._producer.close()

    def __del__(self):
        # Ensure the producer is closed and messages are flushed when the object is deleted
        self.close()


class DBConnectionConfig(TypedDict):
    type: Literal["db"]
    db_type: str
    kwargs: Optional[dict]


class ProducerConfig(TypedDict):
    name: str
    py_object: str
    kwargs: Optional[dict]


def _load_stream_producer(py_object: str) -> Type[StreamProducer]:
    """
    Load a batch producer class given a string in the following format:
    <module>:<class_name>. The module must be in the PYTHONPATH.
    """
    module_name, class_name = py_object.split(":")
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    if not isinstance(cls, type):
        raise ValueError(f"Object {cls} is not a class")
    return cls


def _format_topic(producer_name: str, query: str) -> str:
    """
    Format the name of the task based on the producer name, database name, and query.
    """
    return f"{producer_name}-{hash_query(query)}"


class StreamProduceTask(Task):
    def __init__(self):
        super().__init__()
        self.config = ConfigManager("configuration/stream.yaml")

        self.producer_configs: List[ProducerConfig] = self.config._load_config()["producers"]
        self.stream_producers: Dict[str, Tuple[Type[StreamProducer], dict]] = {}

        for producer_config in self.producer_configs:
            name = producer_config["name"]
            if name in self.stream_producers:
                raise ValueError(f"Duplicate producer name: {name}")
            self.stream_producers[name] = (
                _load_stream_producer(producer_config["py_object"]),
                producer_config.get("kwargs", {}),
            )

        self.kafka_config = self.config._load_config()["kafka"]
        self.kafka_admin = KafkaAdmin(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    def setup(self):
        pass

    def execute(self, queries: List[str]):
        processes: List[Process] = []
        for name, (producer, kwargs) in self.stream_producers.items():
            for query in queries:
                topic = _format_topic(name, query)
                if not self.kafka_admin.topic_exists(topic):
                    self.kafka_admin.create_topic(
                        topic=topic,
                        num_partitions=self.kafka_config["num_partitions"],
                        replication_factor=self.kafka_config["replication_factor"],
                    )
                process = Process(
                    target=producer(KAFKA_BOOTSTRAP_SERVERS, topic).produce_forever,
                    args=(query,),
                    kwargs=kwargs,
                    name=topic,
                    daemon=True,
                )
                process.start()
                processes.append(process)

        for process in processes:
            # Block indefinitely until someone kills the process
            process.join()


# ===-----------------------------------------------------------------------===#
# Kafka Admin Setup                                                            #
#                                                                              #
# The KafkaAdmin class is a child class of KafkaAdminClient from the Kafka     #
# library. It provides additional utility methods to manage Kafka topics,      #
# such as checking if a topic exists (`topic_exists`), creating a new topic    #
# (`create_topic`), and deleting topics (`delete_topics`). These methods       #
# enhance the functionality of the base KafkaAdminClient class, making it      #
# easier to manage Kafka topics in the StreamingProduceTask class.             #
#                                                                              #
# Author: Walter J.T.V, Marc Parcerisa                                         #
# ===-----------------------------------------------------------------------===#


class KafkaAdmin(KafkaAdminClient):
    def topic_exists(self, topic: str) -> bool:
        """
        Check if a topic exists in the Kafka cluster.
        """
        try:
            topics = self.list_topics()
            return topic in topics
        except Exception as e:
            logger.error(f"Error checking if topic exists: {e}")
            return False

    def create_topic(self, topic: str, num_partitions: int, replication_factor: int):
        """
        Create a new topic in the Kafka cluster.
        """
        try:
            if not self.topic_exists(topic):
                new_topic = NewTopic(
                    name=topic,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor,
                )
                self.create_topics(new_topics=[new_topic])
                logger.success(
                    f"Created topic '{topic}' with {num_partitions} partitions and replication factor {replication_factor}."
                )
            else:
                logger.warning(f"Topic '{topic}' already exists.")
        except Exception as e:
            logger.error(f"Error creating topic '{topic}': {e}")

    def delete_topics(self, topics: List[str]):
        try:
            existing_topics = set(self.list_topics())
            topics_to_delete = set(topics).intersection(existing_topics)
            if not topics_to_delete:
                logger.info("No topics to delete.")
                return
            super().delete_topics(topics_to_delete)
            logger.success(f"Deleted topics: {topics_to_delete}")

        except Exception as e:
            logger.error(f"Error deleting topics: {e}")


if __name__ == "__main__":
    # Example usage
    task = StreamProduceTask()
    task.execute(["machine"])
