from src.utils.config import ConfigManager

from abc import ABC, abstractmethod, staticmethod

### Abstract class for API connectors
### Each API client (like TwitterAPIClient, YoutubeAPIClient, etc.) will inherit from this class to 
### provide concrete implementations of the API-specific logic.
### UTILITY: This abstracts the complexity of each individual API handling to a simple interface

class APIClient(ABC):
    def __init__(self):
        #Define the base attributes
        self.config_manager = ConfigManager(config_path="config/api_config.yaml")
        
    @abstractmethod
    def authenticate(self):
        pass
    
    @abstractmethod
    def connect(self):
        pass 
    
    @abstractmethod
    def fetch(self, base: str, endpoint: str, params: dict = None):
        """
        Abstract method to fetch data from the API.

        :param base: The base URL of the API (e.g., "https://api.twitter.com").
        :param endpoint: The specific endpoint to which the request is being made (e.g., "/tweets").
        :param params: Optional dictionary containing query parameters or request body data (default: None).

        :return: The data retrieved from the API
        """
        pass
