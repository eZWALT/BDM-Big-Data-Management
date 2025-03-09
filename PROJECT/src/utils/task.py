from abc import ABC, abstractmethod
from enum import Enum


# Enumeration for status of a task
class TaskStatus(Enum):
    PENDING = "Pending"
    IN_PROGRESS = "In Progress"
    COMPLETED = "Completed"
    FAILED = "Failed"
    CANCELLED = "Cancelled"


### This file contains the base definitions of abstract task classes related to the
### Orchestration and structure of the data pipeline
### UTILITY: These abstract task classes define a standardized interface for pipeline stages, ensuring modular,
### scalable, and consistent orchestration across different stages.


class Task(ABC):
    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def setup(self):
        pass

    @abstractmethod
    def execute(self):
        pass

    @abstractmethod
    def status(self):
        pass

    @abstractmethod
    def cleanup(self):
        pass
