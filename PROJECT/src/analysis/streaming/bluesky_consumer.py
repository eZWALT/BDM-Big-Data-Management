from kafka.admin import KafkaAdminClient
from loguru import logger

if __name__ == "__main__":
    import os
    import sys

    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    sys.path.append(root)
    os.chdir(root)


from src.analysis.streaming import StreamConsumer
from src.utils.config import ConfigManager


class BlueskyConsumer(StreamConsumer):
    def process_message(self, message):
        logger.info(message)


if __name__ == "__main__":
    config = ConfigManager(config_path="configuration/stream.yaml")
    bootstrap_servers = config._load_config()["kafka"]["bootstrap_servers"]
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topics = admin.list_topics()
    topics = list(filter(lambda x: x.startswith("bluesky"), topics))

    logger.info(f"Subscribing to topics: {topics}")
    consumer = BlueskyConsumer(id="bluesky_consumer")
    consumer.subscribe(topics)
    consumer.poll()
