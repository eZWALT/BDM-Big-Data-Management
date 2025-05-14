import json
from typing import Generator, List

from pyspark.sql import Row


def read_json(file_path: str) -> Generator[Row, None, None]:
    """
    Read a JSON file and return a list of Row objects.

    Args:
        file_path (str): Path to the JSONL file.

    Returns:
        Generator[Row]: List of Row objects representing the data in the JSONL file.
    """
    with open(file_path, "r") as f:
        raw_data = json.load(f)
        if not isinstance(raw_data, list):
            raise ValueError("JSON file must contain a list of records.")
        yield from map(lambda record: Row(**record), raw_data)
