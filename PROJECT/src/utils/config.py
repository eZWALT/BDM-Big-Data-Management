import os

import yaml

# ===----------------------------------------------------------------------===#
# ConfigManager                                                              #
#                                                                            #
# This class provides utility functions to manage configuration files,       #
# specifically designed for API configurations. It loads and parses the      #
# YAML configuration file, making the data accessible through various        #
# methods. Key methods include retrieving API base URLs, endpoints, and      #
# authentication credentials. It supports dynamic access to nested config    #
# data and raises errors when required values are missing.                   #
#                                                                            #
# Author: Walter J.T.V                                                       #
# ===----------------------------------------------------------------------===#


class ConfigManager:
    def __init__(self, config_path="config/api.yaml"):
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self):
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file '{self.config_path}' not found.")
        with open(self.config_path, "r") as file:
            return yaml.safe_load(file)

    def get_api_base_url(self, api_name):
        return self.config.get("apis", {}).get(api_name, {}).get("base_url", "")

    def get_api_endpoints(self, api_name):
        endpoints = self.config.get("apis", {}).get(api_name, {}).get("endpoints", {})
        return endpoints

    # Retrieves authentication credentials based on the API's auth type and raises an error if missing."""
    def get_api_credentials(self, api_name):
        auth_config = self.config.get("apis", {}).get(api_name, {}).get("auth", {})
        auth_type = auth_config.get("type", "")

        if auth_type == "api_key":
            api_key_env = auth_config.get("api_key_env", "")
            api_key = os.getenv(api_key_env)

            if not api_key:
                raise ValueError(f"Missing API key for {api_name}. Set the environment variable '{api_key_env}'.")

            return {"api_key": api_key}

        elif auth_type == "username_password":
            username_env = auth_config.get("username_env", "")
            password_env = auth_config.get("password_env", "")
            username = os.getenv(username_env)
            password = os.getenv(password_env)

            if not username or not password:
                raise ValueError(f"Missing credentials for {api_name}. Set '{username_env}' and '{password_env}'.")

            return {"username": username, "password": password}

        elif auth_type == "email_password":
            email_env = auth_config.get("email_env", "")
            password_env = auth_config.get("password_env", "")
            email = os.getenv(email_env)
            password = os.getenv(password_env)

            if not email or not password:
                raise ValueError(f"Missing credentials for {api_name}. Set '{email_env}' and '{password_env}'.")

            return {"email": email, "password": password}

        raise ValueError(f"Unknown authentication type '{auth_type}' for API '{api_name}'.")

    def get_config(self, *keys, default=None):
        data = self.config
        for key in keys:
            data = data.get(key, {})
            if not isinstance(data, dict):
                return data
        return data if data else default


if __name__ == "__main__":
    cfg = ConfigManager()

    # Example usage (this will explode if env vars are missing)
    try:
        print(cfg.get_api_credentials("youtube")["api_key"])
    except ValueError as e:
        print(f"ERROR: {e}")
