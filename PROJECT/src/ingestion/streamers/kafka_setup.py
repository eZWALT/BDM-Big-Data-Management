from kafka.admin import KafkaAdminClient, NewTopic 
from loguru import logger
import yaml
from src.utils.config import ConfigManager

# ===----------------------------------------------------------------------===#
# Kafka Admin Setup                                                           #
#                                                                             #
# This script represent a Kafka Admin class which simply manages              #
# the most basic aspects needed to use kafka such as topic creation and the   #
# connection to the local server and other management tasks                   #
#                                                                             #
# Author: Walter J.T.V                                                        #
# ===----------------------------------------------------------------------===#


def create_topics_from_config(admin_client, config):
    """ Create Kafka topics from loaded YAML configuration """
    new_topics = []
    for topic_config in config["topics"]:
        new_topic = NewTopic(
            name=topic_config["name"],
            num_partitions=topic_config["num_partitions"],
            replication_factor=topic_config["replication_factor"]
        )
        new_topics.append(new_topic)
    
    admin_client.create_topics(new_topics=new_topics)
    logger.success("Kafka topics successfully created")

if __name__ == "__main__":
    cfg = ConfigManager(config_path="config/streaming.yaml")
    config = cfg._load_config()
    bootstrap_servers = config["kafka"]["bootstrap_servers"]
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    # Create topics from the loaded configuration
    create_topics_from_config(admin_client, config)    
    admin_client.close()