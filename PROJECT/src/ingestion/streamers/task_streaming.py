
import time 
from loguru import logger
from typing import List, Dict
import threading                # Used for API I/O overhead
import multiprocessing          # Used for real process parallelism (producer/consumer)


from src.utils.task import Task, TaskStatus
from src.utils.config import ConfigManager
from src.ingestion.streamers.youtube_consumer import YoutubeConsumer
from src.ingestion.streamers.youtube_producer import YoutubeProducer
from src.utils.streamer import IngestionStrategy

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
        # TODO: Define default additional arguments for each social media
        self.producers = {
            "youtube_1": (YoutubeProducer(), {"max_results": 1, "max_comments": 1}),
            "youtube_2": (YoutubeProducer(), {"max_results": 1, "max_comments": 1}),
            # "twitter": (TwitterProducer(), {"bearer_token": "TWITTER_TOKEN"}),
            # "bluesky": (BlueskyProducer(), {"auth_token": "BLUESKY_TOKEN"}),
        }
        
    def setup(self):
        pass
    
    # Helper function
    def produce_data(self, query, producer, extra_args):
        # Logic to produce data for a given query
        producer.produce(query=query, strategy=IngestionStrategy.NEWEST, **extra_args)

    def execute(self, queries: List[str]):
        self.task_status = TaskStatus.IN_PROGRESS
        it_count = 0
        
        start_time = time.time()
        while time.time() - start_time < self.max_time:
            threads = []
            for producer_name, (producer, extra_args) in self.producers.items():
                for query in queries:
                    strategy = IngestionStrategy.RELEVANCE if it_count == 0 else IngestionStrategy.NEWEST
                    thread = threading.Thread(target=self.produce_data, args=(query, producer, extra_args))
                    thread.start()  # Start the thread
                    threads.append(thread)
            
            # Wait for all threads to finish
            for thread in threads:
                thread.join()

            time.sleep(self.prod_interleave_time)
            it_count += 1
        logger.info(f"[PRODUCER TASK] Max streaming duration of {self.max_time} seconds reached. Stopping...")
        self.task_status = TaskStatus.COMPLETED


# Multiple consumers need to multithread to read from all topics
class StreamingConsumeTask(Task):
    def __init__(self):
        super().__init__()
        self.cfg = ConfigManager("config/streaming.yaml")
        self.max_time = self.cfg._load_config()["orchestration"]["max_streaming_duration"]
        self.consumers = [
            YoutubeConsumer(),
            YoutubeConsumer(),
            #TwitterConsumer(),
            #BlueskyConsumer(),
        ]
        
    def setup(self):
        pass

    def execute(self):
        self.task_status = TaskStatus.IN_PROGRESS
        start_time = time.time()

        processes = []
        for consumer in self.consumers:
            # Start each consumer in its own process
            process = multiprocessing.Process(target=consumer.poll)
            process.start()
            processes.append(process)

        # Keep track of elapsed time and check when to stop
        while time.time() - start_time < self.max_time:
            logger.error("fuck me")
            time.sleep(0.5)  # Sleep to avoid busy-waiting

        logger.info(f"[CONSUMER TASK] Max streaming duration of {self.max_time} seconds reached. Stopping consumption.")

        # Forcefully terminate the processes after max_time
        for process in processes:
            process.terminate()  

        logger.info("[CONSUMER TASK] All consumer processes have been terminated.")
        self.task_status = TaskStatus.COMPLETED


def streaming_orchestration(queries: List[str] = ["Fake jordans", "Water Jordans", "Fire Jordans"]):
    logger.info("Starting streaming production and consumption tasks...")
    producer_task = StreamingProduceTask()
    consumer_task = StreamingConsumeTask()

    # Start producer and consumer in parallel using multiprocessing (2 processes)
    consumer_process = multiprocessing.Process(target=consumer_task.execute)
    producer_process = multiprocessing.Process(target=producer_task.execute, args=(queries,))

    #Start the procesess
    consumer_process.start()
    producer_process.start()

    # Wait for both processes to finish
    consumer_process.join()
    producer_process.join()

    logger.info("Streaming tasks have completed.")

if __name__ == "__main__":
    streaming_orchestration()
