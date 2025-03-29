import importlib
import json
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from functools import wraps
from hashlib import sha256
from re import finditer
from threading import Thread
from typing import (
    Any,
    Callable,
    Concatenate,
    Dict,
    List,
    Literal,
    Optional,
    ParamSpec,
    TextIO,
    Type,
    TypedDict,
    TypeVar,
)

from loguru import logger

if __name__ == "__main__":
    import os
    import sys

    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    sys.path.append(root)
    os.chdir(root)

from src.utils.config import ConfigManager
from src.utils.task import Task, TaskStatus

P = ParamSpec("P")
R = TypeVar("R")
Rint = TypeVar("Rint", bound=int)


class BatchProducer(ABC):
    @abstractmethod
    def produce(
        self,
        query: str,
        db_connection: "DBConnection",
        utc_since: Optional[datetime] = None,
        utc_until: Optional[datetime] = None,
        **kwargs,
    ) -> int:
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
            since = kwargs.get("utc_since", args[2] if len(args) > 2 else None)
            until = kwargs.get("utc_until", args[3] if len(args) > 3 else None)
            # Split the PascalCase class name into words
            matches = finditer(".+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)", self.__class__.__name__)
            cls_name = " ".join(m.group(0) for m in matches).upper()
            logger.info(f"[{cls_name}] Producing post data for query '{query}' since {since} until {until}")
            start_time = time.time()
            result = func(self, *args, **kwargs)
            end_time = time.time()
            logger.info(
                f"[{cls_name}] Produced {result} elements for query '{query}' since {since} until {until} in {end_time - start_time:.2f} seconds"
            )
            return result

        return wrapper

    def __init_subclass__(cls):
        super().__init_subclass__()
        cls.produce = cls._log_start_and_end(cls.produce)


class ProducerConfig(TypedDict):
    class DBConnectionConfig(TypedDict):
        type: str
        kwargs: Optional[dict]

    name: str
    py_object: str
    db: Optional[DBConnectionConfig] = {"type": "null"}
    kwargs: Optional[dict]


class LoadedProducerConfig(TypedDict):
    class LoadedDBConnectionConfig(TypedDict):
        name: str
        cls: Type["DBConnection"]
        kwargs: Optional[dict]

    name: str
    cls: Type[BatchProducer]
    kwargs: Optional[dict]
    db: LoadedDBConnectionConfig


def _load_batch_producer(py_object: str) -> Type[BatchProducer]:
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


class StreamingProduceTask(Task):
    def __init__(self):
        super().__init__()
        self.config = ConfigManager("config/batch.yaml")
        self.producer_configs: List[ProducerConfig] = self.config._load_config()["producers"]
        self.batch_producers: Dict[str, LoadedProducerConfig] = {}

        for producer_config in self.producer_configs:
            name = producer_config["name"]
            if name in self.batch_producers:
                raise ValueError(f"Duplicate producer name: {name}")

            db_connection_config = producer_config.get("db", {"db": "null"})
            self.batch_producers[name] = {
                "cls": _load_batch_producer(producer_config["py_object"]),
                "kwargs": producer_config.get("kwargs", {}),
                "db": {
                    "cls": _load_db_connection(db_connection_config["type"]),
                    "kwargs": db_connection_config.get("kwargs", {}),
                    "name": db_connection_config["type"],
                },
            }

    def setup(self):
        pass

    @staticmethod
    def _format_topic(
        producer_name: str,
        db_name: str,
        query: str,
        utc_since: Optional[datetime] = None,
        utc_until: Optional[datetime] = None,
    ) -> str:
        """
        Format the name of the task based on the producer name, database name, and query.
        """
        # Use sha256 to hash the query for a consistent length, and take the first 8 characters
        query_hash = sha256(query.encode("utf-8")).hexdigest()[:8]
        utc_until = utc_until or datetime.now(timezone.utc)
        utc_since = utc_since or datetime(1912, 6, 23, tzinfo=timezone.utc)  # Alan Turing's birthdate (because why not)
        since_str = utc_since.strftime("%Y%m%d%H%M%S")
        until_str = utc_until.strftime("%Y%m%d%H%M%S")
        return f"{producer_name}-{db_name}-{query_hash}-{since_str}-{until_str}"

    def execute(self, queries: List[str], utc_since: Optional[datetime] = None, utc_until: Optional[datetime] = None):
        self.task_status = TaskStatus.IN_PROGRESS

        start = time.time()
        processes: List[Thread] = []
        for name, producer in self.batch_producers.items():
            db = producer["db"]
            for query in queries:
                topic = self._format_topic(name, db["name"], query, utc_since, utc_until)
                connection = db["cls"](topic=topic, **db["kwargs"])
                process = Thread(
                    target=producer["cls"]().produce,
                    args=(query, connection, utc_since, utc_until),
                    kwargs=producer["kwargs"],
                    name=topic,
                )
                process.start()
                processes.append(process)

        # Wait for all threads to finish
        for thread in processes:
            thread.join()

        logger.info(f"[BATCH PRODUCER TASK] Batch producer finished. Total time: {time.time() - start:.2f} seconds")
        self.task_status = TaskStatus.COMPLETED


class DBConnection(ABC):
    """
    Abstract base class for database connections.
    """

    @abstractmethod
    def connect(self, bucket: str):
        """
        Connect to a database bucket.
        """
        pass

    @abstractmethod
    def close(self, bucket: Optional[str] = None):
        """
        Close a database bucket connection.
        If no bucket is specified, close all connections.
        """
        pass

    @abstractmethod
    def add(self, bucket: str, data: Any):
        """
        Add data to a bucket in the database.
        """
        pass

    @abstractmethod
    def add_many(self, bucket: str, data: List[Any]):
        """
        Add multiple data entries to a bucket in the database.
        """
        pass

    @abstractmethod
    def flush(self, bucket: Optional[str] = None):
        """
        Flush the data in the bucket to the database.
        If no bucket is specified, flush all connections.
        """
        pass


class JSONLFileConnection(DBConnection):
    """
    Class for JSONL database connection.
    """

    @staticmethod
    def _requires_bucket_open(
        method: Callable[Concatenate["JSONLFileConnection", str, P], R],
    ) -> Callable[Concatenate["JSONLFileConnection", str, P], R]:
        def wrapper(self: "JSONLFileConnection", bucket: str, *args: P.args, **kwargs: P.kwargs) -> R:
            if bucket not in self._buckets:
                raise ValueError(f"Bucket {bucket} is not connected.")
            return method(self, bucket, *args, **kwargs)

        return wrapper

    def __init__(self, topic: str, folder: str):
        self.folder = os.path.join(folder, topic)
        if not os.path.exists(self.folder):
            os.makedirs(self.folder)
        if not os.path.isdir(self.folder):
            raise NotADirectoryError(f"Folder {self.folder} is not a directory.")
        self._buckets: Dict[str, TextIO] = {}

    def _get_bucket_path(self, bucket: str) -> str:
        """
        Get the path of the bucket file.
        """
        return os.path.join(self.folder, f"{bucket}.jsonl")

    def connect(self, bucket: str):
        if bucket is not None:
            if bucket in self._buckets:
                raise ValueError(f"Bucket {self._get_bucket_path(bucket)} is already connected.")
            self._buckets[bucket] = open(self._get_bucket_path(bucket), "a")
            logger.info(f"Bucket {self._get_bucket_path(bucket)} opened with JSONL format for writing.")

    def close(self, bucket: Optional[str] = None):
        if bucket is not None:
            if bucket not in self._buckets:
                raise ValueError(f"Bucket {self._get_bucket_path(bucket)} is not connected.")
            self._buckets[bucket].close()
            del self._buckets[bucket]
            logger.info(f"Bucket {self._get_bucket_path(bucket)} closed.")
        else:
            for b in self._buckets.values():
                b.close()
                logger.info(f"Bucket {self._get_bucket_path(b.name)} closed.")
            self._buckets.clear()
            logger.info("All buckets closed.")

    @_requires_bucket_open
    def add(self, bucket: str, data: dict):
        if not isinstance(data, dict):
            raise ValueError("Data must be a dictionary.")
        self._buckets[bucket].write(json.dumps(data) + "\n")

    @_requires_bucket_open
    def add_many(self, bucket: str, data: List[dict]):
        if not all(isinstance(d, dict) for d in data):
            raise ValueError("All data entries must be dictionaries.")
        for entry in data:
            self._buckets[bucket].write(json.dumps(entry) + "\n")

    def flush(self, bucket: Optional[str] = None):
        if bucket is None:
            for b in self._buckets.values():
                b.flush()
        else:
            if bucket not in self._buckets:
                raise ValueError(f"Bucket {self._get_bucket_path(bucket)} is not connected.")
            self._buckets[bucket].flush()


class NullDatabaseConnection(DBConnection):
    """
    Class for a null database connection.
    """

    def connect(self, bucket: str):
        pass

    def close(self, bucket: Optional[str]):
        pass

    def add(self, bucket: str, data: Any):
        pass

    def add_many(self, bucket: str, data: List[Any]):
        pass

    def flush(self, bucket: Optional[str]):
        pass


DB_CONN_MAP = {
    "null": NullDatabaseConnection,
    "jsonl-file": JSONLFileConnection,
    # Add other database connections here
}


def _load_db_connection(db: Literal["null", "jsonl-file"]) -> Type[DBConnection]:
    """
    Load a database connection class given a db type name.
    """
    if db not in DB_CONN_MAP:
        raise ValueError(f"Database type {db} is not supported")
    return DB_CONN_MAP[db]


if __name__ == "__main__":
    # Example usage
    task = StreamingProduceTask()
    task.execute(["water jordan"], utc_since=datetime(2023, 1, 1), utc_until=datetime(2023, 12, 31))
