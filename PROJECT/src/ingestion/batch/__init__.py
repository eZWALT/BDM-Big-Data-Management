import os
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from functools import cache, wraps
from re import finditer
from threading import Thread
from typing import *

from loguru import logger

if __name__ == "__main__":
    import os
    import sys

    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    sys.path.append(root)
    os.chdir(root)

from src.ingestion.batch.tables import TableConnection, TableConnectionConfig, connect_tables
from src.utils.config import ConfigManager
from src.utils.hash import hash_query
from src.utils.task import Task

# ===-----------------------------------------------------------------------===#
# Batch Producing Base Classes and Connection Abstractions                     #
#                                                                              #
# Author: Marc Parcerisa                                                       #
# ===-----------------------------------------------------------------------===#

P = ParamSpec("P")
R = TypeVar("R")
Rint = TypeVar("Rint", bound=int)


class BatchProducer(ABC):
    @abstractmethod
    def produce(self, query: str, utc_since: Optional[datetime], utc_until: Optional[datetime], **kwargs) -> int:
        """
        Produce data for the given query and load it into the database.
        This method should be overridden by subclasses to implement the specific data production logic.

        Returns:
            int: The number of elements produced.
        """
        pass

    @staticmethod
    def _log_start_and_end(
        func: Callable[Concatenate["BatchProducer", P], Rint],
    ) -> Callable[Concatenate["BatchProducer", P], Rint]:
        @wraps(func)
        def wrapper(self: "BatchProducer", *args: P.args, **kwargs: P.kwargs) -> Rint:
            query = kwargs.get("query", args[0] if args else None)
            since = kwargs.get("utc_since", args[1] if len(args) > 1 else None)
            until = kwargs.get("utc_until", args[2] if len(args) > 2 else None)
            # Split the PascalCase class name into words
            matches = finditer(".+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)", self.__class__.__name__)
            cls_name = " ".join(m.group(0) for m in matches).upper()
            logger.info(f"[{cls_name}] Producing post data for query '{query}' since {since} until {until}")
            start_time = time.time()
            result = func(self, *args, **kwargs)
            end_time = time.time()
            logger.success(
                f"[{cls_name}] Produced {result} elements for query '{query}' since {since} until {until} in {end_time - start_time:.2f} seconds"
            )
            return result

        return wrapper

    def __init_subclass__(cls):
        super().__init_subclass__()
        cls.produce = cls._log_start_and_end(cls.produce)


from src.ingestion.batch.bluesky_batch import BlueskyBatchProducer
from src.ingestion.batch.twitter_batch import TwitterBatchProducer
from src.ingestion.batch.youtube_batch import YoutubeBatchProducer

PRODUCER_MAP = {
    "bluesky": BlueskyBatchProducer,
    "twitter": TwitterBatchProducer,
    "youtube": YoutubeBatchProducer,
}


class ProducerConfig(TypedDict):
    name: str
    producer_type: str
    kwargs: Optional[Dict[str, Any]]
    tables: Optional[Dict[str, TableConnectionConfig]]


@cache
def load_producer_config(social_network: str) -> ProducerConfig:
    """
    Load a producer configuration from a YAML file.
    The YAML file should contain a list of producers with their names and configurations.
    """
    config = ConfigManager("configuration/batch.yaml")
    producers = config._load_config()
    if social_network not in producers:
        raise ValueError(f"Producer {social_network} not found in configuration.")
    return producers[social_network]


def _load_batch_producer(producer_type: str) -> Type[BatchProducer]:
    """
    Load a batch producer class based on the given type.
    """
    if producer_type not in PRODUCER_MAP:
        raise ValueError(f"Producer type '{producer_type}' not found.")
    producer_class = PRODUCER_MAP[producer_type]
    return producer_class


class BatchProduceTask(Task):
    def __init__(
        self,
        producer_config: ProducerConfig,
        query: str,
        utc_since: Optional[datetime] = None,
        utc_until: Optional[datetime] = None,
    ):
        super().__init__()
        self._producer_type = producer_config["producer_type"]
        self._name = producer_config["name"]
        self._kwargs = producer_config.get("kwargs", {})
        self._table_configs = producer_config.get("tables", {})
        self.query = query
        self.utc_since = utc_since
        self.utc_until = utc_until

        self.producer: Optional[BatchProducer] = None
        self._table_connections: Optional[Dict[str, TableConnection]] = None

    def setup(self):
        self.producer = _load_batch_producer(self._producer_type)()
        self._table_connections = connect_tables(self._table_configs, query_hash=hash_query(self.query))

    def _flush_db_connections(self):
        """
        Flush all database connections.
        """
        if self._table_connections is not None:
            for conn in self._table_connections.values():
                conn.flush()
                conn.close()
            self._table_connections = None

    def execute(self):
        self.producer.produce(self.query, self.utc_since, self.utc_until, **self._kwargs, **self._table_connections)
        self._flush_db_connections()


if __name__ == "__main__":
    queries = ["artificial intelligence"]
    utc_since = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    utc_until = datetime.now(tz=timezone.utc) - timedelta(seconds=11)  # Fucking Twitter API shit (max 17 days, min 10s)

    social_networks = ("bluesky",)  # "youtube", "twitter")

    config = ConfigManager("configuration/batch.yaml")
    producer_configs: Dict[str, ProducerConfig] = config._load_config()
    tasks: List[BatchProduceTask] = []
    for social_network in social_networks:
        producer_config = producer_configs[social_network]
        for query in queries:
            task = BatchProduceTask(
                producer_config=producer_config,
                query=query,
                utc_since=utc_since,
                utc_until=utc_until,
            )
            task.setTaskName(f"{producer_config['name']}-{hash_query(query)}")
            tasks.append(task)

    logger.info("Tasks instantiated, setting them up")
    for task in tasks:
        task.setup()

    logger.info("Tasks set up and ready to start. Pushing them to threads.")
    jobs: List[Thread] = []
    for task in tasks:
        thread = Thread(target=task.execute, name=task.task_name, daemon=True)
        thread.start()
        jobs.append(thread)

    logger.info("All tasks started, waiting for them to finish.")
    for job in jobs:
        job.join()
        logger.info(f"Task {job.name} finished.")
    logger.info("All tasks finished.")
    logger.info("Batch producer test finished.")
