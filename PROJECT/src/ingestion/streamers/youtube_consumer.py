from kafka import KafkaConsumer
import json
import time
from loguru import logger
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


class TestConsumer(Consumer):
    def __init__(self):
        super().__init__(id="TEST")

    def process_message(self, message: dict):
        logger.success(f"[{self.id}-CONSUMER] Processed Message {message.value}")
    
    def run(self):
        pass
    
class YoutubeConsumer(Consumer):
    def __init__(self):
        self.id = "YT"
        super().__init__(id="YT")

    def process_message(self, message: dict):
        logger.success(f"[{self.id}-CONSUMER] Processed Message {message}")
    
    def run(self):
        pass


if __name__ == "__main__":
    local_test = True
    if local_test:
        consumer = TestConsumer()
        consumer.subscribe(["test_topic"])
        #consumer.consume()
        consumer.poll(verbose=True)
    else:
        # To do a real example lets create a consumer and also perform
        # subscription to a set of topics and active polling to get them
        consumer = YoutubeConsumer()
        consumer.subscribe(["youtube_caption", "youtube_metadata", "youtube_comment"])
        consumer.consume()
        #consumer.poll()
