import os
from typing import Any, Dict, List, Literal, Optional, TypedDict

from src.utils.config import ConfigManager


class CleanerConfig(TypedDict):
    loader_type: str
    py_files: Optional[str]
    application_args: Dict[str, str]
    env_vars: Optional[Dict[str, str]]
    conf: Optional[Dict[str, str]]


def get_data_cleaner_configs(social_network: str) -> Dict[str, CleanerConfig]:
    """
    Get the dictionary of id:cleaner_config of cleaners for a given social network.
    """
    config = ConfigManager(config_path="configuration/cleaners.yaml").config
    if not social_network in config:
        raise ValueError(f"Social network {social_network} not found in configuration.")
    return config[social_network]


cleaner_scripts = {
    "bluesky-likes": os.path.abspath(os.path.join(os.path.dirname(__file__), "bluesky_likes.py")),
    "bluesky-posts": os.path.abspath(os.path.join(os.path.dirname(__file__), "bluesky_posts.py")),
    "twitter-posts": os.path.abspath(os.path.join(os.path.dirname(__file__), "twitter_posts.py")),
    "youtube-comments": os.path.abspath(os.path.join(os.path.dirname(__file__), "youtube_comments.py")),
    "youtube-video-metadata": os.path.abspath(os.path.join(os.path.dirname(__file__), "youtube_video_metadata.py")),
    "blob": os.path.abspath(os.path.join(os.path.dirname(__file__), "blob_cleaner.py")),
}


def get_data_cleaner_script(cleaner_type: str) -> str:
    """
    Get the script path for a given cleaner type.
    """
    if cleaner_type not in cleaner_scripts:
        raise ValueError(f"Cleaner type {cleaner_type} not found.")
    return cleaner_scripts[cleaner_type]
