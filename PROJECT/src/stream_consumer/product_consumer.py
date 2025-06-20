import json
import os
import re
import signal
from typing import Callable, Dict

from kafka import KafkaConsumer, KafkaProducer
from loguru import logger
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from consumption.sentiment import create_vader_udf

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_PATTERN = "^trusted-.*-.*-.*$"  # trusted-<source>-<query_hash>-<data_type>
OUTPUT_PREFIX = "product"

def analyze_sentiment(analyzer, text):
    try:
        score = analyzer.polarity_scores(text)["compound"]
        if score >= 0.05:
            return "positive"
        elif score <= -0.05:
            return "negative"
        else:
            return "neutral"
    except:
        return "unknown"

class ProductConsumer:
    def __init__(self):
        # consume _all_ of the landing-* topics
        self.consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        )
        self.consumer.subscribe(pattern=INPUT_PATTERN)

        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def run(self):
        logger.info("ProductConsumer started, listening on trusted-* topics")
        for msg in self.consumer:
            topic = msg.topic  # example: "trusted-youtube-abc123-posts"
            payload = msg.value  # cleaned + landed dict
            meta = payload.get("_meta", {})

            # pick product key (injected earlier into _meta)
            product_id = meta.get("product_id") or meta.get("query_hash")
            dtype = meta.get("type")  # "posts", "likes", …

            if not product_id or not dtype:
                logger.warning("Skipping message without product_id/type in _meta")
                continue

            # Sentiment analysis on message
            analyzer = SentimentIntensityAnalyzer()
            sentiment = analyze_sentiment(analyzer, payload)

            # route into product-<product_id>-<data_type>
            out_topic = f"{OUTPUT_PREFIX}-{product_id}-{dtype}"
            sentiment_topic = f"{sentiment}"
            self.producer.send(out_topic, payload, sentiment_topic)
            logger.debug(f"Routed → {out_topic} - {sentiment}")

        self._shutdown()

    def _shutdown(self, *args):
        logger.warning("Shutting down ProductConsumer…")
        try:
            self.consumer.close()
            self.producer.close()
        except:
            pass
        os._exit(0)


if __name__ == "__main__":
    ProductConsumer().run()
