import json
from csv import DictReader
from typing import Generator, List

from pyspark.sql import Row


def read_csv(file_path: str) -> Generator[Row, None, None]:
    """
    Read a CSV file and return a list of Row objects.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        List[Row]: List of Row objects representing the data in the CSV file.
    """
    with open(file_path, "r") as f:
        reader = DictReader(f)
        data = [Row(**row) for row in reader]
    return data
