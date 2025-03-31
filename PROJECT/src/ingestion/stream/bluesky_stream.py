import asyncio
import json

import websockets
from aiokafka import AIOKafkaProducer

from src.ingestion.stream import StreamProducer

# ===-----------------------------------------------------------------------===#
# Bluesky Async Streaming Producer                                             #
#                                                                              #
# The BlueskyStreamProducer is a specialized streaming producer that connects  #
# to a Bluesky Jetstream websocket, processes incoming messages, and sends     #
# relevant data to a Kafka topic. It filters messages based on a query and     #
# handles specific event types, such as "app.bsky.feed.post", using predefined #
# handlers. This class is designed to continuously produce data to Kafka while #
# allowing for extensibility to handle additional event types.                 #
#                                                                              #
# Author: Marc Parcerisa                                                       #
# ===-----------------------------------------------------------------------===#


class BlueskyStreamProducer(StreamProducer):
    def __init__(self, bootstrap_servers, topic):
        self._bootstrap_servers = bootstrap_servers
        self._producer: AIOKafkaProducer = None
        self._topic = topic

        self.COMMIT_HANDLERS = {
            "app.bsky.feed.post": self.handle_post,
            # The following are events that can be handled in the future:
            # "app.bsky.feed.like":
            # "app.bsky.graph.follow":
            # "app.bsky.feed.repost":
            # "app.bsky.feed.threadgate":
            # "app.bsky.feed.postgate":
            # "app.bsky.feed.generator":
            # "app.bsky.graph.block":
            # "app.bsky.graph.listitem":
            # "app.bsky.graph.starterpack":
            # "app.bsky.graph.list":
            # "app.bsky.graph.listblock":
            # "app.bsky.actor.profile":
            # "social.pinksky.app.preference":
            # "social.pinksky.app.collection":
            # "chat.bsky.actor.declaration":
            # "blue.flashes.feed.post":
            # "blue.flashes.actor.profile":
        }

    async def send_message(self, message: dict):
        self._producer.send(self._topic, value=message)

    async def handle_message(self, query: str, message: dict):
        """
        Handle incoming messages from the websocket.
        This method processes the message and sends it to the Kafka topic.
        """
        if message["kind"] == "commit":
            handle_commit = self.COMMIT_HANDLERS.get(message["commit"]["collection"])
            if handle_commit:
                await handle_commit(query, message)

    async def main(self, query: str, jetstream_url: str):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            value_serializer=(lambda v: json.dumps(v).encode("utf-8")),
        )
        async with websockets.connect(jetstream_url) as socket:
            while True:
                # Receive messages from the websocket
                message = await socket.recv()
                await self.handle_message(query, json.loads(message))

    def produce(self, query: str, jetstream_url: str):
        """
        Continuously produce data to the Kafka topic.
        This method must be run in a separate process to avoid asyncio event loop issues.
        """
        asyncio.run(self.main(query, jetstream_url))

    async def handle_post(self, query: str, message: dict):
        if message["commit"]["operation"] == "create":
            # Determine if this post is related to the query
            # and send it to the Kafka topic if it is.
            post_text = message["commit"]["record"]["text"]
            if query in post_text:
                # By now, the simplest way to determine if the post is related to the query
                # is to check if the query is in the post text.
                await self.send_message(message)

    async def close(self):
        await self._producer.stop()
