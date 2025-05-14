import os
from typing import Callable, Generator, List

from pyspark.sql import Row

from .blob_reader import read_blob
from .csv_reader import read_csv
from .json_reader import read_json
from .jsonl_reader import read_jsonl


def get_reader(folder_path: str) -> Generator[Row, None, None]:
    """
    Get the appropriate reader function based on the files inside the folder.
    """
    if not os.path.exists(folder_path):
        raise ValueError(f"Folder not found: {folder_path}")
    if not os.path.isdir(folder_path):
        raise ValueError(f"Expected a directory, but found a file: {folder_path}")
    if not os.listdir(folder_path):
        raise EmptyError(f"Folder is empty: {folder_path}")
    ext = os.path.splitext(os.listdir(folder_path)[0])[1]
    match ext:
        case ".jsonl":
            reader = read_jsonl
        case ".json":
            reader = read_json
        case ".csv":
            reader = read_csv
        case ".mp4" | ".mp3" | ".jpg":
            reader = read_blob
        case _:
            raise ValueError(f"Unsupported file format: {ext}")

    for file in os.listdir(folder_path):
        yield from reader(os.path.join(folder_path, file))


class EmptyError(Exception):
    """
    Custom exception for empty folders.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message
