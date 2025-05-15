import os
from typing import Any, Dict, List, Optional, TypedDict

from src.utils.config import ConfigManager


class LoaderConfig(TypedDict):
    loader_type: str
    application_args: Dict[str, str]
    packages: Optional[str]


def get_data_loader_configs(social_network: str) -> Dict[str, LoaderConfig]:
    """
    Get the dictionary of id:loader_config of loaders for a given social network.
    """
    config = ConfigManager(config_path="configuration/loaders.yaml")
    if not social_network in config:
        raise ValueError(f"Social network {social_network} not found in configuration.")
    return config[social_network]


loader_scripts = {
    "jsonl": os.path.abspath(os.path.join(os.path.dirname(__file__), "jsonl_loader.py")),
    "blob": os.path.abspath(os.path.join(os.path.dirname(__file__), "blob_loader.py")),
}


def get_data_loader_script(loader_type: str) -> str:
    """
    Get the script path for a given loader type.
    """
    if loader_type not in loader_scripts:
        raise ValueError(f"Loader type {loader_type} not found.")
    return loader_scripts[loader_type]
