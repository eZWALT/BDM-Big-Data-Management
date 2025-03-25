from kafka import KafkaConsumer
import json
import time
from src.utils.config import ConfigManager
from src.utils.streamer import Consumer

# ===----------------------------------------------------------------------===#
# Youtube Streaming Consumer                                                  #
#                                                                             #
# This class represents a "Consumer" for the Youtube API following the Kafka  #
# Producer-Consumer and publication/subscription patterns. Fetches data from  #
# the broker kafka server managed by zookeeper from the producers
#                                                                             #
# Author: Walter J.T.V                                                        #
# ===----------------------------------------------------------------------===#


class YoutubeConsumer:
    def __init__(self):
        pass

    def run_consumer(self):
        pass


if __name__ == "__main__":
    local_test = True
    cfg = ConfigManager(config_path="config/streaming.yaml")
    if local_test:
        consumer = KafkaConsumer(
            ["test_topic"],
            bootstrap_servers=cfg._load_config()["kafka"]["bootstrap_servers"],
            value_deserializer=(lambda v: json.loads(v.decode("utf-8"))),
            auto_offset_reset="earliest",  # Offset 0 (earliest)
            enable_auto_commit=False,
        )
        # Show captured messages
        for message in consumer:
            print(f"[CONSUMER] recieved: {message.value}")

    else:
        # To do a real example lets create a consumer and also perform
        # subscription to a set of topics and active polling to get them
        consumer = KafkaConsumer(
            bootstrap_servers=cfg._load_config()["kafka"]["bootstrap_servers"],
            value_deserializer=(lambda v: json.loads(v.decode("utf-8"))),
            auto_offset_reset="earliest",  # Offset 0 (earliest)
            enable_auto_commit=False,
        )
        # Subscribe to a specific topic
        consumer.subscribe(topics=["my-topic"])

        # Actively Poll for new messages
        while True:
            msg = consumer.poll(timeout_ms=1000)
            if msg:
                for topic, partition, offset, key, value in msg.items():
                    print(
                        "Topic: {} | Partition: {} | Offset: {} | Key: {} | Value: {}".format(
                            topic, partition, offset, key, value.decode("utf-8")
                        )
                    )
            else:
                print("No new messages")
