from abc import ABC, abstractmethod

###
### This file contains the base definitions of abstract task classes related to the 
### Orchestration and structure of the data pipeline
###

class Task(ABC):
    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def setup(self):
        pass

    @abstractmethod
    def execute(self):
        pass


class IngestionTask(Task):
    pass 

class LandingTask(Task):
    pass 

class TrustedTask(Task):
    pass

class ExploitationTask(Task):
    pass    
