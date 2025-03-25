
import multiprocessing
import time 
from datetime import datetime
from loguru import logger
from typing import List, Dict

from src.utils.task import Task, TaskStatus
from src.utils.config import ConfigManager
from src.ingestion.streamers.youtube_consumer import YoutubeConsumer
from src.ingestion.streamers.youtube_producer import YoutubeProducer
# ===----------------------------------------------------------------------===#
# Streaming Prod/Cons Tasks                                                   #
#                                                                             #
# The following classes represents tasks to be orchestrated (NOT AIRFLOW)     #
# to perform the real-time streaming production of content simultaneously and #
# consumption by a parallel consumption process, these will actively produce  #
# and consume until a given max duration                                      #
#                                                                             #
# Author: Walter J.T.V                                                        #
# ===----------------------------------------------------------------------===#



#TODO: MAKE A COMMON PRODUCE/CONSUME INTERFACE FOR APIS/STREAMERS/...

# One producer per source will interleave to stream their data
class StreamingProduceTask(Task):
    def __init__(self):
        super().__init__()
        self.cfg = ConfigManager("config/streaming.yaml")
        self.max_time = self.cfg._load_config()["orchestration"]["max_streaming_duration"]
        self.prod_interleave_time = self.cfg._load_config()["orchestration"]["producer_interleaving_time"]
        self.producers = [
            YoutubeProducer(),
            YoutubeProducer(),
            #TwitterProducer(),
            #BlueskyProducer(),
        ]
    
    def execute(self, queries: List[str]):
        start_time = time.time()
        while time.time() - start_time < self.max_time:
            for producer in self.producers:
                for query in queries:
                    producer.produce(None)     # TODO: these methods need to do some querying and do something
                                               # TODO: Also some variability/randomness need to be added to the mix 
                                               
                time.sleep(self.prod_interleave_time)
        logger.info(f"[PRODUCER TASK] Max streaming duration of {self.max_time} seconds reached. Stopping...")

# Multiple consumers need to multithread to read from all topics
class StreamingConsumeTask(Task):
    def __init__(self):
        super().__init__()
        self.cfg = ConfigManager("config/streaming.yaml")
        self.max_time = self.cfg._load_config()["orchestration"]["max_streaming_duration"]
        self.topic_list = self.cfg._load_config()["orchestration"]["topics_to_consume"]
        self.consumers = [
            YoutubeConsumer(),
            YoutubeConsumer(),
            #TwitterConsumer(),
            #BlueskyConsumer(),
        ]

    def execute(self):
        start_time = time.time()

        processes = []
        for consumer in self.consumers:
            # Start each consumer in its own process
            process = multiprocessing.Process(target=consumer.poll, args=(self.topic_list,))
            process.start()
            processes.append(process)

        # Keep track of elapsed time and check when to stop
        while True:
            elapsed_time = time.time() - start_time

            if elapsed_time >= self.max_time:
                logger.info(f"[CONSUMER TASK] Max streaming duration of {self.max_time} seconds reached. Stopping consumption.")
                break  # Exit the loop if max_time has been reached

            time.sleep(0.5)  # Sleep to avoid busy-waiting

        # Now terminate all the consumer processes
        for process in processes:
            process.terminate()  # Forcefully terminate the process after max_time

        # Optionally, wait for processes to finish
        for process in processes:
            process.join()

        logger.info("[CONSUMER TASK] All consumer processes have been terminated.")
        