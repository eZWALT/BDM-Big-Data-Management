import importlib
import io
import json
import os
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
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
            since = kwargs.get("utc_since", args[2] if len(args) > 2 else None)
            until = kwargs.get("utc_until", args[3] if len(args) > 3 else None)
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


class BatchProduceTask(Task):
    def __init__(self):
        super().__init__()
        self.config = ConfigManager("config/batch.yaml")
        self.producer_configs: List[ProducerConfig] = self.config._load_config()["producers"]
        self.batch_producers: Dict[str, Tuple[Type[BatchProducer], dict]] = {}

        for producer_config in self.producer_configs:
            name = producer_config["name"]
            if name in self.batch_producers:
                raise ValueError(f"Duplicate producer name: {name}")
            self.batch_producers[name] = (
                _load_batch_producer(producer_config["py_object"]),
                producer_config.get("kwargs", {}),
            )

    def setup(self):
        pass

    @staticmethod
    def _format_topic(
        producer_name: str,
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
        return f"{producer_name}-{query_hash}-{since_str}-{until_str}"

    def execute(self, queries: List[str], utc_since: Optional[datetime] = None, utc_until: Optional[datetime] = None):
        self.task_status = TaskStatus.IN_PROGRESS

        start = time.time()
        processes: List[Thread] = []
        for name, (producer, kwargs) in self.batch_producers.items():
            for query in queries:
                topic = self._format_topic(name, query, utc_since, utc_until)

                process = Thread(
                    target=producer().produce,
                    args=(query, utc_since, utc_until),
                    kwargs=_discover_db_connections(kwargs, topic=topic),
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


class JSONLFileConnection(DBConnection):
    """
    Class for JSONL database connection.
    """

    @staticmethod
    def _requires_file_open(
        method: Callable[Concatenate["JSONLFileConnection", P], R],
    ) -> Callable[Concatenate["JSONLFileConnection", P], R]:
        def wrapper(self: "JSONLFileConnection", *args: P.args, **kwargs: P.kwargs) -> R:
            if self._file is None:
                raise ValueError(f"JSONL Database {self.file_path} is not connected.")
            if not self._file.writable():
                raise ValueError(f"JSONL Database {self.file_path} is not writable.")
            if self._file.closed:
                raise ValueError(f"JSONL Database {self.file_path} is already closed.")
            if not self._file:
                raise ValueError(f"JSONL Database {self.file_path} is not open.")
            return method(self, *args, **kwargs)

        return wrapper

    def __init__(self, file: os.PathLike):
        self.file_path = file
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        if os.path.exists(self.file_path) and not os.path.isfile(self.file_path):
            raise FileExistsError(f"File {self.file_path} already exists and is not a file.")
        self._file = open(self.file_path, "a")
        logger.success(f"File {self.file_path} opened with JSONL format for writing.")

    @_requires_file_open
    def close(self):
        self._file.close()
        self._file = None
        logger.success(f"File {self.file_path} closed.")

    @_requires_file_open
    def add(self, data: dict):
        if not isinstance(data, dict):
            raise ValueError("Data must be a dictionary.")
        self._file.write(json.dumps(data) + "\n")

    @_requires_file_open
    def add_many(self, data: List[dict]):
        if not all(isinstance(d, dict) for d in data):
            raise ValueError("All data entries must be dictionaries.")
        for entry in data:
            self._file.write(json.dumps(entry) + "\n")

    @_requires_file_open
    def flush(self):
        self._file.flush()


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
    "jsonl-file": JSONLFileConnection,
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
    for kwarg_name, kwarg_value in kwargs.items():
        if _is_db_connection(kwarg_value):
            new_kwargs[kwarg_name] = _load_db_connection(kwarg_value)
        else:
            new_kwargs[kwarg_name] = kwarg_value

    return new_kwargs


if __name__ == "__main__":
    # Example usage
    task = BatchProduceTask()
    task.execute(["water jordan"], utc_since=datetime(2025, 3, 20), utc_until=datetime(2025, 3, 30))
