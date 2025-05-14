import importlib
import io
import json
import os
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from functools import wraps
from hashlib import sha256
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

from src.utils.config import ConfigManager
from src.utils.task import Task, TaskStatus

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


class DBConnectionConfig(TypedDict):
    type: Literal["db"]
    db_type: str
    kwargs: Optional[dict]


class ProducerConfig(TypedDict):
    name: str
    py_object: str
    kwargs: Optional[dict]


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


def hash_query(query: str) -> str:
    """
    Hash the query using SHA256 and return the first 8 characters.
    This is used to create a unique identifier for the query.
    """
    # Use sha256 to hash the query for a consistent length, and take the first 8 characters
    return sha256(query.encode("utf-8")).hexdigest()[:8]


class BatchProduceTask(Task):
    def __init__(
        self,
        producer_config: ProducerConfig,
        query: str,
        utc_since: Optional[datetime] = None,
        utc_until: Optional[datetime] = None,
    ):
        super().__init__()
        self._producer_py_object = producer_config["py_object"]
        self._producer_name = producer_config["name"]
        self._producer_kwargs = producer_config.get("kwargs", {})
        self.query = query
        self.utc_since = utc_since
        self.utc_until = utc_until

        self.producer: Optional[BatchProducer] = None
        self._prepared_kwargs: Optional[dict] = None
        self._db_connections: Optional[List[DBConnection]] = None

    def setup(self):
        self.producer = _load_batch_producer(self._producer_py_object)()
        self._prepared_kwargs, self._db_connections = _discover_db_connections(
            self._producer_kwargs, query_hash=hash_query(self.query)
        )

    def _flush_db_connections(self):
        """
        Flush all database connections.
        """
        if self._db_connections:
            for conn in self._db_connections:
                conn.flush()
                conn.close()
            self._db_connections = None

    def execute(self):
        self.producer.produce(self.query, self.utc_since, self.utc_until, **self._prepared_kwargs)
        self._flush_db_connections()


class DBConnection(ABC):
    """
    Abstract base class for database connections.
    """

    @abstractmethod
    def __init__(self, **kwargs):
        """
        Initialize the database connection.
        """
        pass

    @abstractmethod
    def close(self):
        """
        Close a database bucket connection.
        If no bucket is specified, close all connections.
        """
        pass

    @abstractmethod
    def add(self, data: Any):
        """
        Add data to a bucket in the database.
        """
        pass

    @abstractmethod
    def add_many(self, data: List[Any]):
        """
        Add multiple data entries to a bucket in the database.
        """
        pass

    @abstractmethod
    def flush(self):
        """
        Flush the data in the bucket to the database.
        If no bucket is specified, flush all connections.
        """
        pass

    def __del__(self):
        """
        Destructor for the database connection.
        """
        self.close()


class JSONLFileConnection(DBConnection):
    """
    Class for JSONL database connection.
    """

    def __init__(self, folder: os.PathLike, buffer_size: int = 1024 * 1024):
        self.folder_path = folder
        os.makedirs(self.folder_path, exist_ok=True)
        self.buffer_size = buffer_size
        self._buffer = io.StringIO()
        logger.info(f"File {self.folder_path} opened with JSONL format for writing.")

    def close(self):
        if self._buffer.tell() > 0:
            self.flush()
        logger.info(f"File {self.folder_path} closed.")

    def add(self, data: dict):
        if not isinstance(data, dict):
            raise ValueError("Data must be a dictionary.")
        self._buffer.write(json.dumps(data) + "\n")
        if self._buffer.tell() >= self.buffer_size:
            self.flush()

    def add_many(self, data: List[dict]):
        if not all(isinstance(d, dict) for d in data):
            raise ValueError("All data entries must be dictionaries.")
        for entry in data:
            self._buffer.write(json.dumps(entry) + "\n")
            if self._buffer.tell() >= self.buffer_size:
                self.flush()

    def flush(self):
        if self._buffer.tell() > 0:
            with open(os.path.join(self.folder_path, f"{int(time.monotonic())}.jsonl"), "a") as f:
                f.write(self._buffer.getvalue())
            self._buffer.close()
            self._buffer = io.StringIO()
            logger.info(f"Flushed data to {self.folder_path}.")
        else:
            logger.warning("Attempted flush of empty buffer, skipping.")


class FilesDBConnection(DBConnection):
    """
    Store files in a directory.
    """

    def __init__(self, folder: os.PathLike):
        self.folder = folder
        os.makedirs(self.folder, exist_ok=True)

    def close(self):
        pass

    def add(self, data: Tuple[str, IO]):
        """
        Add a file to the database.
        """
        file_name, file_data = data
        if not isinstance(file_data, (io.IOBase)):
            raise ValueError(f"Data must be a file-like object. Found: {type(file_data)}")
        # Write the data to a file in chunks to avoid memory issues
        with open(os.path.join(self.folder, file_name), "wb") as f:
            while True:
                chunk = file_data.read(1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)

    def add_many(self, data: List[Any]):
        """
        Add multiple files to the database.
        """
        for file_name, file_data in data:
            self.add((file_name, file_data))
        pass

    def flush(self):
        pass


class NullDatabaseConnection(DBConnection):
    """
    Class for a null database connection.
    """

    def __init__(self, **kwargs):
        pass

    def close(self):
        pass

    def add(self, data: Any):
        pass

    def add_many(self, data: List[Any]):
        pass

    def flush(self):
        pass


DB_CONN_MAP: dict[str, Type[DBConnection]] = {
    "null": NullDatabaseConnection,
    "jsonl": JSONLFileConnection,
    "files": FilesDBConnection,
    # Add other database connections here
}


def _discover_db_connections(kwargs: dict, **placeholder_values: Any) -> dict:
    """
    Discover and load database connections from the given kwargs.
    This function looks for any kwargs that are dictionaries with a "type" key
    and a "db_type" key. It then loads the corresponding database connection class
    and updates the kwargs with the loaded class and any additional arguments.

    Kwargs for the database may include placeholders for the kwargs passed
    to this function. For example, if the kwargs include a "topic" key with a value
    of "my_topic", and the database connection kwargs include key-value pairs like
    "db_type": "jsonl-file", "kwargs": {"file": "{topic}.jsonl"}, the resulting
    database connection will be a JSONLFileConnection with the file path set to
    "my_topic.jsonl".

    """

    def _is_db_connection(kwarg_value: Any) -> bool:
        if not isinstance(kwarg_value, dict):
            return False
        if not "type" in kwarg_value:
            return False
        if not kwarg_value["type"] == "db":
            return False
        if not "db_type" in kwarg_value:
            return False
        if not kwarg_value["db_type"] in DB_CONN_MAP:
            return False
        return True

    def _load_db_connection(db_data: DBConnectionConfig) -> DBConnection:
        """
        Load a database connection class given a db type name.
        """
        db_type = db_data["db_type"]
        db_kwargs = db_data.get("kwargs", {})
        if db_type not in DB_CONN_MAP:
            raise ValueError(f"Database type {db_type} is not supported.")

        def _replace_placeholders(object: Any):
            if isinstance(object, dict):
                return {k: _replace_placeholders(v) for k, v in object.items()}
            elif isinstance(object, list):
                return list(_replace_placeholders(v) for v in object)
            elif isinstance(object, str):
                for key, value in placeholder_values.items():
                    object = object.replace(f"{{{key}}}", str(value))
                return object
            else:
                return object

        return DB_CONN_MAP[db_type](**_replace_placeholders(db_kwargs))

    new_kwargs = {}
    db_connections = []
    for kwarg_name, kwarg_value in kwargs.items():
        if _is_db_connection(kwarg_value):
            conn = _load_db_connection(kwarg_value)
            new_kwargs[kwarg_name] = conn
            db_connections.append(conn)
        else:
            new_kwargs[kwarg_name] = kwarg_value

    return new_kwargs, db_connections


if __name__ == "__main__":
    queries = ["artificial intelligence", "machine learning"]
    utc_since = datetime.now(tz=timezone.utc) - timedelta(days=2)
    utc_until = datetime.now(tz=timezone.utc) - timedelta(seconds=11)  # Fucking Twitter API shit (max 17 days, min 10s)

    config = ConfigManager("configuration/batch.yaml")
    producer_configs: List[ProducerConfig] = config._load_config()["producers"]
    tasks: List[BatchProduceTask] = []
    for producer_config in producer_configs:
        for query in queries:
            task = BatchProduceTask(
                producer_config=producer_config,
                query=query,
                utc_since=utc_since,
                utc_until=utc_until,
            )
            task.setTaskName(f"{producer_config['name']}-{sha256(query.encode('utf-8')).hexdigest()[:8]}")
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
