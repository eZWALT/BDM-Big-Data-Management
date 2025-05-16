import io
import json
import os
import time
from abc import ABC, abstractmethod
from typing import *

from loguru import logger
from minio import Minio

from src.utils.placeholders import replace_placeholders


class TableConnection(ABC):
    """
    Abstract base class for database connections.
    """

    def __init__(self, table: str, **kwargs: Any):
        """
        Initialize the database connection.
        """
        self.table = table

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
        if not self.is_closed():
            self.close()

    @abstractmethod
    def is_closed(self) -> bool:
        """
        Check if the database connection is closed.
        """
        return self.table is None


class JSONLTableConnection(TableConnection):
    """
    Class for JSONL database connection.
    """

    def __init__(self, table: str, buffer_size: int = 1024 * 1024):
        self.table = table
        os.makedirs(self.table, exist_ok=True)
        self.buffer_size = buffer_size
        self._file: io.TextIOWrapper = None
        self._open_new_file()

    def _open_new_file(self):
        if self._file is not None:
            self._file.flush()
            self._file.close()
        self._file = open(os.path.join(self.table, f"{int(time.monotonic())}.jsonl"), "a")

    def close(self):
        if self._file is not None:
            self._file.close()
            self._file = None
        logger.info(f"Closed JSONL file connection for {self.table}/{self.table}.")

    def add(self, data: dict):
        if not isinstance(data, dict):
            raise ValueError("Data must be a dictionary.")
        if self._file is None:
            raise ValueError("File is not open. Call _open_new_file() first.")
        self._file.write(json.dumps(data) + "\n")
        if self._file.tell() >= self.buffer_size:
            self._open_new_file()

    def add_many(self, data: List[dict]):
        if not all(isinstance(d, dict) for d in data):
            raise ValueError("All data entries must be dictionaries.")
        if self._file is None:
            raise ValueError("File is not open. Call _open_new_file() first.")
        for entry in data:
            self._file.write(json.dumps(entry) + "\n")
            if self._file.tell() >= self.buffer_size:
                self._open_new_file()

    def flush(self):
        if self._file.tell() > 0:
            self._file.flush()

    def is_closed(self) -> bool:
        """
        Check if the database connection is closed.
        """
        return self._file is None or self._file.closed


class JSONLMinIOTableConnection(TableConnection):
    """
    Class for JSONL database connection with minio.
    """

    def __init__(
        self,
        table: str,
        buffer_size: int = 1024 * 1024,
        minio_endpoint: str = "localhost:9000",
        minio_access_key: str = None,
        minio_secret_key: str = None,
    ):
        self.table = table
        self.buffer_size = buffer_size

        self.minio_client = Minio(
            minio_endpoint, access_key=minio_access_key, secret_key=minio_secret_key, secure=False
        )
        self.bucket_name, self.folder_path = table.split("/", 1)
        if not self.minio_client.bucket_exists(self.bucket_name):
            self.minio_client.make_bucket(self.bucket_name)
            logger.info(f"Created bucket {self.bucket_name} in MinIO.")
        else:
            logger.info(f"Bucket {self.bucket_name} already exists in MinIO.")

        self._buffer = io.StringIO()

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
            file_name = f"{int(time.monotonic())}.jsonl"
            self.minio_client.put_object(
                bucket_name=self.bucket_name,
                object_name=os.path.join(self.folder_path, file_name),
                data=io.BytesIO(self._buffer.getvalue().encode("utf-8")),
                length=self._buffer.tell(),
            )
            self._buffer.close()
            self._buffer = io.StringIO()
            logger.info(f"Flushed data to MinIO bucket {self.table}/{file_name}.")

    def close(self):
        if self._buffer.tell() > 0:
            self.flush()
        self._buffer.close()
        logger.info(f"Closed JSONL file connection for {self.table}/{self.table}.")

    def is_closed(self) -> bool:
        """
        Check if the database connection is closed.
        """
        return self._buffer.closed


class BlobTableConnection(TableConnection):
    """
    Store files in a directory.
    """

    def __init__(self, table: str):
        self.table = table
        os.makedirs(self.table, exist_ok=True)

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
        with open(os.path.join(self.table, file_name), "wb") as f:
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

    def is_closed(self) -> bool:
        """
        Check if the database connection is closed.
        """
        return self.table is None or not os.path.exists(self.table)


class BlobMinIOTableConnection(TableConnection):
    """
    Store files in a MinIO bucket.
    """

    def __init__(
        self,
        table: str,
        minio_endpoint: str = "localhost:9000",
        minio_access_key: str = None,
        minio_secret_key: str = None,
    ):
        self.table = table

        self.minio_client = Minio(
            minio_endpoint, access_key=minio_access_key, secret_key=minio_secret_key, secure=False
        )
        self.bucket_name, self.folder_path = table.split("/", 1)
        if not self.minio_client.bucket_exists(self.bucket_name):
            self.minio_client.make_bucket(self.bucket_name)
            logger.info(f"Created bucket {self.bucket_name} in MinIO.")
        else:
            logger.info(f"Bucket {self.bucket_name} already exists in MinIO.")

    def add(self, data: Tuple[str, IO]):
        """
        Add a file to the database.
        """
        file_name, file_data = data
        if not isinstance(file_data, (io.IOBase)):
            raise ValueError(f"Data must be a file-like object. Found: {type(file_data)}")
        # Write the data to a file in chunks to avoid memory issues
        self.minio_client.put_object(
            bucket_name=self.bucket_name,
            object_name=os.path.join(self.folder_path, file_name),
            data=file_data,
            length=-1,
            part_size=10 * 1024 * 1024,
        )

    def add_many(self, data: List[Any]):
        """
        Add multiple files to the database.
        """
        for file_name, file_data in data:
            self.add((file_name, file_data))

    def flush(self):
        """
        Flush the data in the bucket to the database.
        If no bucket is specified, flush all connections.
        """
        pass

    def close(self):
        """
        Close the database connection.
        """
        pass

    def is_closed(self) -> bool:
        """
        Check if the database connection is closed.
        """
        return self.table is None or not self.minio_client.bucket_exists(self.bucket_name)


class NullTableConnection(TableConnection):
    """
    Class for a null database connection.
    """

    def __init__(self, table: str, **kwargs):
        self.table = table

    def close(self):
        pass

    def add(self, data: Any):
        pass

    def add_many(self, data: List[Any]):
        pass

    def flush(self):
        pass

    def is_closed(self) -> bool:
        """
        Check if the database connection is closed.
        """
        return False


DB_CONN_MAP: dict[str, Type[TableConnection]] = {
    "null": NullTableConnection,
    "jsonl": JSONLTableConnection,
    "blob": BlobTableConnection,
    "jsonl-minio": JSONLMinIOTableConnection,
    "blob-minio": BlobMinIOTableConnection,
    # Add other database connections here
}


class TableConnectionConfig(TypedDict):
    db_type: str
    table: str
    kwargs: Optional[Dict[str, Any]]


def connect_table(table_config: TableConnectionConfig, **placeholder_values: Any) -> TableConnection:
    """
    Load a database connection class given a table configuration.
    """
    db_type = table_config["db_type"]
    db_table = table_config["table"]
    db_kwargs = table_config.get("kwargs", {})
    if db_type not in DB_CONN_MAP:
        raise ValueError(f"Database type {db_type} is not supported.")

    db_table = replace_placeholders(db_table, **placeholder_values)

    return DB_CONN_MAP[db_type](table=db_table, **replace_placeholders(db_kwargs, **placeholder_values))


def connect_tables(
    table_configs: Dict[str, TableConnectionConfig], **placeholder_values: Any
) -> Dict[str, TableConnection]:
    """
    Load database connections from the given table configurations.
    This function loads the corresponding database connection class
    and updates the kwargs with the loaded class and any additional arguments.
    """

    res: Dict[str, TableConnection] = {}
    for name, table_config in table_configs.items():
        db_connection = connect_table(table_config, **placeholder_values)
        res[name] = db_connection
    return res
