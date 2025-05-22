import os 
from typing import Any, Dict, List, Literal, Optional, TypedDict
from src.utils.config import ConfigManager

class ConsumerConfig(TypedDict):
    loader_type: str
    py_files: Optional[str]
    application_args: Dict[str, str]
    env_vars: Optional[Dict[str, str]]
    conf: Optional[Dict[str, str]]

consumer_scripts = {
    "sentiment": os.path.abspath(os.path.join(os.path.dirname(__file__), "sentiment.py")),
}
    
# These do not depend on the social media
def get_consumer_configs(task_name: str) -> Dict[str, ConsumerConfig]:
    """
    Get the all the consumers configurations
    """
    config = ConfigManager(config_path="configuration/consumers.yaml").config 
    if not task_name in config:
        raise ValueError(f"Consumption task {task_name} not found in configuration.")
    return config 

def get_consumer_script(consumer_type: str) -> str:
    """
    Get the script path for a given consumer type.
    """
    if consumer_type not in consumer_scripts:
        raise ValueError(f"Cleaner type {consumer_scripts} not found.")
    return consumer_scripts[consumer_type]
