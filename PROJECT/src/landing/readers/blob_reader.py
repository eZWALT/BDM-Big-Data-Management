import json
from typing import Generator, List

from pyspark.sql import Row


def read_blob(file_path: str) -> Generator[Row, None, None]:
    """
    Read a binary large object file and return a list with a single Row object.
    This object will yield a single Row object

    Args:
        file_path (str): Path to the BLOB file.

    Returns:
        Generator[Row]: List of Row objects representing the data in the BLOB file.
    """
    with open(file_path, "rb") as f:
        raw_data = f.read()
        yield Row(source_file_path=file_path, data=raw_data)
