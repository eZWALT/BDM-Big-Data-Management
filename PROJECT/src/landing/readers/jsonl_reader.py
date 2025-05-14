import json
from typing import Generator, List

from pyspark.sql import Row


def read_jsonl(file_path: str) -> Generator[Row, None, None]:
    """
    Read a JSONL file and return a list of Row objects.

    Args:
        file_path (str): Path to the JSONL file.

    Returns:
        Generator[Row]: List of Row objects representing the data in the JSONL file.
    """
    with open(file_path, "r") as f:
        yield from map(lambda line: Row(**json.loads(line)), f)
