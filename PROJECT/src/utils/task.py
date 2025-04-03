import time
from abc import ABC, abstractmethod
from enum import Enum
from functools import wraps
from re import finditer
from typing import Callable, Concatenate, ParamSpec, TypeVar

from loguru import logger

# ===-----------------------------------------------------------------------===#
# Task                                                                         #
#                                                                              #
# This abstract class defines the foundational structure for tasks within      #
# an orchestration framework, ensuring a standardized interface for all        #
# pipeline stages. Tasks can be customized by inheriting and implementing      #
# the abstract methods `setup()`, `execute()`, `status()`, and `cleanup()`.    #
# This class enforces modularity, scalability, and consistency in pipeline     #
# orchestration, providing a base for various task types. Also adds a bit of   #
# logging at the start of the execution, and keeps track of the status.        #
#                                                                              #
# Author: Walter J.T.V, Marc Parcerisa                                         #
# ===-----------------------------------------------------------------------===#

P = ParamSpec("P")
R = TypeVar("R")


# Enumeration for status of a task
class TaskStatus(Enum):
    PENDING = "Pending"
    IN_PROGRESS = "In Progress"
    COMPLETED = "Completed"
    FAILED = "Failed"
    CANCELLED = "Cancelled"


class Task(ABC):
    def __init__(self):
        super().__init__()
        self.task_status = TaskStatus.PENDING

    @abstractmethod
    def setup(self):
        pass

    @abstractmethod
    def execute(self):
        pass

    def status(self):
        return self.task_status

    @staticmethod
    def _handle_status(
        func: Callable[Concatenate["Task", P], R],
    ) -> Callable[Concatenate["Task", P], R]:
        @wraps(func)
        def wrapper(self: "Task", *args: P.args, **kwargs: P.kwargs) -> R:
            # Split the PascalCase class name into words
            matches = finditer(".+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)", self.__class__.__name__)
            cls_name = " ".join(m.group(0) for m in matches).upper()
            logger.info(f"[{cls_name}] Task starting...")
            self.task_status = TaskStatus.IN_PROGRESS
            start_time = time.time()
            try:
                result = func(self, *args, **kwargs)
            except KeyboardInterrupt as e:
                self.task_status = TaskStatus.CANCELLED
                end_time = time.time()
                logger.warning(f"[{cls_name}] Task cancelled after {end_time-start_time:.2f} seconds")
                raise e
            except Exception as e:
                self.task_status = TaskStatus.FAILED
                end_time = time.time()
                logger.error(f"[{cls_name}] Task failed after {end_time-start_time:.2f} seconds: {e}")
                raise e
            else:
                self.task_status = TaskStatus.COMPLETED
                end_time = time.time()
                logger.success(f"[{cls_name}] Task completed after {end_time - start_time:.2f} seconds")
            return result

        return wrapper

    def __init_subclass__(cls):
        super().__init_subclass__()
        cls.execute = cls._handle_status(cls.execute)
