from kafka import KafkaProducer
import json
import time
from loguru import logger
from typing import List, Dict
import os

from src.ingestion.connectors.youtube_client import YoutubeAPIClient
from src.utils.config import ConfigManager

# ===----------------------------------------------------------------------===#
# Youtube Streaming Producer                                                  #
#                                                                             #
# This class represents a "Producer" for the Youtube API following the Kafka  #
# Producer-Consumer and publication/subscription patterns. Fetches data from  #
# fresh published data and publishes it into a zookeeper broker for consumers #
# to subscribe and consume the given streamed/stored data. It publishes       #
# captions/metadata/comments to each of these 3 topics. Note the lack of      #
# try/catch error handling, silent errors will be allowed to avoid verbosity  #
#                                                                             #
# Author: Walter J.T.V                                                        #
# ===----------------------------------------------------------------------===#

"""
Kafka partitioning 
-------------------------------------
        +-----------+  
        | Producer  |  
        +-----------+  
              |  
       +------------+  
       | Kafka API  |  
       +------------+  
              |  
    +-------------------+  
    | Kafka Broker(s)   |  
    | (Manages Topics)  |  
    +-------------------+  
              |  
    +-------------------+  
    | Topic Partitions  | (In this case we would have 3 topics) 
    +-------------------+  
      |      |      |    
  +----+  +----+  +----+  
  | P0 |  | P1 |  | P2 |  (Partitions for load balancing)
  +----+  +----+  +----+  
              |  
    +-------------------+  
    |   Kafka Consumer  |  
    | (Reads from Topic)|  
    +-------------------+  
              |  
    +-------------------+  
    |    Data Storage   |  
    | (DB, FileSystem)  |  
    +-------------------+  
              
    +-------------------+  
    |  ZooKeeper        |  (Manages Kafka metadata)
    +-------------------+  
"""


class YoutubeProducer:
    def __init__(self, kafka_broker: str):
        self.kafka_broker = KafkaManagement().get_server()
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_broker,
            value_serializer=(lambda v: json.dumps(v).encode("utf-8")),
        )
        self.client = YoutubeAPIClient()

    # Also return the metadata for other functions to use
    def produce_video_metadata(self, query: str, max_results: int = 5) -> list:
        videos = self.client.extract_videos(query, max_results, save=False)
        for video in videos:
            # Kafka Send messages <Topic, Message>
            self.producer.send("youtube_metadata", video)
        logger.success(f"[YT PRODUCER] Sent metadata to Zookeeper")
        return videos

    def produce_comments(self, videos: list, max_comments: int = 5):
        comments = self.client.extract_comments_from_videos(
            videos=videos, max_comments=max_comments, save=False
        )
        for comment in comments:
            self.producer.send("youtube_comment", comment)
        logger.success(f"[YT PRODUCER] Sent comments to Zookeeper")

    def produce_captions(self, videos: list, max_comments: int = 5):
        output_folder = "data_lake/temporal/captions"
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        captions = self.client.extract_captions_from_videos(
            videos=videos, output_folder=output_folder
        )

        for video_id, caption_file in captions.items():
            # Read the captions file
            with open(caption_file, "r", encoding="utf-8") as file:
                captions_text = file.read()

            message = {
                "video_id": video_id,
                "captions": captions_text,
                "timestamp": time.time(),
            }
            self.producer.send("youtube_captions", message)

            # Remove temporary file (Cleanup)
            os.remove(caption_file)
        logger.success(f"[YT PRODUCER] Sent captions to Zookeeper")

    # Video metadata is always sent, but comments and captions are optional
    def run_producer(
        self,
        query: str,
        max_results: int = 5,
        max_comments: int = 10,
        prod_comments: bool = False,
        prod_captions: bool = False,
    ):
        videos = self.produce_video_metadata(query, max_results)
        if prod_comments:
            self.produce_comments(videos, max_comments)
        if prod_captions:
            self.produce_captions(videos, max_comments)


if __name__ == "__main__":
    # Example of local and API test
    local_test = True
    cfg = ConfigManager(config_path="config/streaming.yaml")
    if not local_test:
        producer = YoutubeProducer()
        producer.run_producer(
            query="Chill Guy Jordans",
            max_results=5,
            max_comments=10,
            prod_comments=True,
            prod_captions=True,
        )
    else:
        producer = KafkaProducer(
            bootstrap_servers=cfg._load_config()["kafka"]["bootstrap_servers"],
            value_serializer=(lambda v: json.dumps(v).encode("utf-8")),
        )
        for i in range(5):
            message = {"number": i, "message": f"[PRODUCER] sent: {i}"}
            logger.success("Flipa tulipa")
            producer.send("test_topic", value=message)
            time.sleep(2)
        producer.flush()
        producer.close()
