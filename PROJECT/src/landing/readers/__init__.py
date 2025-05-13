import os
from typing import Callable, Generator, List

from pyspark.sql import Row

from .csv_reader import read_csv
from .json_reader import read_json
from .jsonl_reader import read_jsonl


def get_row_reader(file_path: str) -> Generator[Row, None, None]:
    """
    Get the appropriate reader function based on the file extension.
    """
    ext = os.path.splitext(file_path)[1]
    match ext:
        case ".jsonl":
            return read_jsonl(file_path)
        case ".json":
            return read_json(file_path)
        case ".csv":
            return read_csv(file_path)
        case _:
            raise ValueError(f"Unsupported file format: {ext}")
