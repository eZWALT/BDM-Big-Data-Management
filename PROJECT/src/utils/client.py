from src.utils.config import ConfigManager

from abc import ABC, abstractmethod

# ===-----------------------------------------------------------------------===#
# APIClient                                                                   #
#                                                                             #
# This abstract class provides a common interface for API connectors. Each    #
# specific API client (e.g., TwitterAPIClient, YoutubeAPIClient) will inherit #
# and implement its authentication, connection setup, and data fetching logic.#
# It includes a generic fetch method with retry logic and exponential backoff.#
# This class abstracts the complexity of interacting with different APIs,     #
# simplifying the process for concrete subclasses. Extract methods are the    #
# main interface for interacting with these classes in a ETL-like fashion     #
# Author: Walter J.T.V                                                        #
# ===-----------------------------------------------------------------------===#


class APIClient(ABC):
    def __init__(self):
        # Define the base attributes
        self.config_manager = ConfigManager(config_path="config/api.yaml")
        self.api_name = "abstract"

    # Each API has its own way of authentification
    # user/pass, api key, OAuth...
    @abstractmethod
    def authenticate(self):
        pass

    # Some api's may need to create a connection
    @abstractmethod
    def connect(self):
        pass

    # Abstract function for generic requests of data in raw format
    # and implementing a basic retry with exponential backoff algorithm.
    @abstractmethod
    def fetch(
        self,
        base: str,
        endpoint: str,
        params: dict = None,
        max_retries: int = 5,
        backoff: int = 2,
    ):
        """
        Abstract method to fetch data from the API.

        :param base: The base URL of the API (e.g., "https://api.twitter.com").
        :param endpoint: The specific endpoint to which the request is being made (e.g., "/tweets").
        :param params: Optional dictionary containing query parameters or request body data (default: None).

        :return: The data retrieved from the API
        """
        pass

    # high level "extract" data functions (ETL)
    @abstractmethod
    def extract(self):
        pass
