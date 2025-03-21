from kafka import KafkaConsumer
import json

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
    if local_test:
        consumer = KafkaConsumer(
            topic="test_topic",
            bootstrap_server="localhost:9092",
            value_deserializer=(lambda v: json.loads(v.decode("utf-8"))),
            group_id="test_group",
            auto_offset_reset="earliest",  # Offset 0 (earliest)
        )
        for message in consumer:
            print(f"[CONSUMER] recieved: {message.value}")
    else:
        pass
