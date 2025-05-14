import os
from typing import Callable, Generator, List

from pyspark.sql import Row

from .blob_reader import read_blob
from .csv_reader import read_csv
from .json_reader import read_json
from .jsonl_reader import read_jsonl


class Reader:
    def __init__(self, folder: os.PathLike) -> None:
        self.folder = folder
        if not os.path.exists(folder):
            raise ValueError(f"Folder not found: {folder}")
        if not os.path.isdir(folder):
            raise ValueError(f"Expected a directory, but found a file: {folder}")
        self.files = os.listdir(folder)
        self.files = [os.path.join(folder, file) for file in self.files]
        if len(self.files) > 0:
            ext = os.path.splitext(self.files[0])[1]
            match ext:
                case ".jsonl":
                    self.reader = read_jsonl
                case ".json":
                    self.reader = read_json
                case ".csv":
                    self.reader = read_csv
                case ".mp4" | ".mp3" | ".jpg":
                    self.reader = read_blob
                case _:
                    raise ValueError(f"Unsupported file format: {ext}")
        else:
            self.reader = None

    def __iter__(self) -> Generator[Row, None, None]:
        """
        Iterate over the files in the folder and yield rows from the reader.
        """
        for file in self.files:
            yield from self.reader(file)

    def __len__(self) -> int:
        """
        Return the number of files in the folder.
        """
        return len(self.files)

    def remove_files(self) -> None:
        """
        Remove all files in the folder.
        """
        for file in self.files:
            os.remove(file)
        self.files = []

    def empty(self) -> bool:
        """
        Check if the folder is empty.
        """
        return len(self.files) == 0
