import os
from typing import Dict, Optional, TypedDict

from src.utils.config import ConfigManager


class NormalizerConfig(TypedDict):
    topics: Dict[str, str]  # Map from social network to topic name
    conf: Optional[Dict[str, str]]  # Optional configuration for the normalizer
    env_vars: Optional[Dict[str, str]]  # Optional environment variables for the normalizer
    py_files: Optional[str]  # Optional Python files for the normalizer
    application_args: Optional[Dict[str, str]]  # Optional application arguments for the normalizer
    output_dir: str  # Output directory for the normalizer


def get_normalizer_config() -> NormalizerConfig:
    """
    Get the normalizer configuration for a given social network
    """
    config = ConfigManager(config_path="configuration/normalizer.yaml").config
    return config


normalizer_script = os.path.abspath(os.path.join(os.path.dirname(__file__), "data_normalizer.py"))
