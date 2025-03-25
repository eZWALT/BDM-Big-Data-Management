from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
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


# Create Kafka topics only if they don't exist
def create_topics_from_config(admin_client, config):
    try:
        # Get the current topics in the streaming configuration file
        existing_topics = set(admin_client.list_topics())

        new_topics = []
        for topic_config in config["kafka"]["topics"]:
            topic_name = topic_config["name"]
            if topic_name in existing_topics:
                logger.info(f"Topic '{topic_name}' already exists. Skipping.")
            else:
                new_topic = NewTopic(
                    name=topic_name,
                    num_partitions=topic_config["num_partitions"],
                    replication_factor=topic_config["replication_factor"],
                )
                new_topics.append(new_topic)

        if new_topics:
            admin_client.create_topics(new_topics=new_topics)
            logger.success(f"Created new topics: {[t.name for t in new_topics]}")
        else:
            logger.info("No new topics needed to be created.")

    except Exception as e:
        logger.error(f"Error creating Kafka topics: {e}")


# Delete Kafka topics safely
def delete_topics(admin_client, topics):
    try:
        existing_topics = set(admin_client.list_topics())
        topics_to_delete = [t for t in topics if t in existing_topics]
        if not topics_to_delete:
            logger.info("No matching topics found for deletion.")
            return

        admin_client.delete_topics(topics_to_delete)
        logger.success(f"Deleted topics: {topics_to_delete}")

    except Exception as e:
        logger.error(f"Error deleting topics: {e}")


if __name__ == "__main__":
    cfg = ConfigManager(config_path="config/streaming.yaml")
    config = cfg._load_config()
    bootstrap_servers = config["kafka"]["bootstrap_servers"]

    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    # Test: delete topics
    # Creation of topics will expectedly raise an error given that the
    # kafka admin hasn't been closed, just retry again
    # delete_topics(admin_client, topics=["youtube_topic", "twitter_topic", "bluesky_topic", "test_topic"])

    # Create topics if they don't exist
    create_topics_from_config(admin_client, config)
    admin_client.close()
