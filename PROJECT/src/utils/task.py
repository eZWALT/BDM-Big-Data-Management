from abc import ABC, abstractmethod
from enum import Enum


# Enumeration for status of a task
class TaskStatus(Enum):
    PENDING = "Pending"
    IN_PROGRESS = "In Progress"
    COMPLETED = "Completed"
    FAILED = "Failed"
    CANCELLED = "Cancelled"


# ===----------------------------------------------------------------------===#
# Task                                                                       #
#                                                                            #
# This abstract class defines the foundational structure for tasks within    #
# an orchestration framework, ensuring a standardized interface for all      #
# pipeline stages. Tasks can be customized by inheriting and implementing    #
# the abstract methods `setup()`, `execute()`, `status()`, and `cleanup()`.  #
# This class enforces modularity, scalability, and consistency in pipeline   #
# orchestration, providing a base for various task types.                    #
#                                                                            #
# Author: Walter J.T.V                                                       #
# ===----------------------------------------------------------------------===#


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
