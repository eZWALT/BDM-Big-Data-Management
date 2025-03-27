import time
from abc import ABC, abstractmethod
from threading import Thread
from typing import List, Optional, Type, TypedDict

from loguru import logger

from src.utils.config import ConfigManager
from src.utils.task import Task, TaskStatus


class BatchProducer(ABC):
    @abstractmethod
    def produce(self, query: str, db_connection, **kwargs):
        # TODO: Define the database connection!!!!
        pass


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
    module = __import__(module_name, fromlist=[class_name])
    cls = getattr(module, class_name)
    if not isinstance(cls, type):
        raise ValueError(f"Object {cls} is not a class")
    if not issubclass(cls, BatchProducer):
        raise ValueError(f"Class {cls} is not a subclass of BatchProducer")
    return cls


class StreamingProduceTask(Task):
    def __init__(self):
        super().__init__()
        self.config = ConfigManager("config/ingestion.yaml")
        self.producer_configs: List[ProducerConfig] = self.config._load_config()["producers"]
        self.batch_producers = {
            producer["name"]: (_load_batch_producer(producer["py_object"]), producer.get("kwargs", {}))
            for producer in self.producer_configs
        }

    def setup(self):
        pass

    def execute(self, queries: List[str]):
        self.task_status = TaskStatus.IN_PROGRESS

        start = time.time()
        processes: List[Thread] = []
        for producer_name, (producer, extra_args) in self.batch_producers.items():
            for query in queries:
                connection = None  # TODO: Define the database connection!!!!
                process = Thread(target=producer().produce, args=(query, connection), kwargs=extra_args)
                process.start()
                processes.append(process)

        # Wait for all threads to finish
        for thread in processes:
            thread.join()

        logger.info(f"[BATCH PRODUCER TASK] Batch producer finished. Total time: {time.time() - start:.2f} seconds")
        self.task_status = TaskStatus.COMPLETED
