from datetime import datetime
from typing import Generator, List, Optional, Tuple

from atproto import models
from loguru import logger

from ..connectors.twitter_client import TweetData, TwitterAPIClient
from . import BatchProducer, DBConnection

# ===-----------------------------------------------------------------------===#
# Twitter Batch Producer                                                       #
#                                                                              #
# Author: Marc Parcerisa                                                       #
# ===-----------------------------------------------------------------------===#


class TwitterBatchProducer(BatchProducer):
    def __init__(self):
        super().__init__()
        self.client = TwitterAPIClient()

    def _load_posts_to_db(self, posts: List[TweetData], db_connection: DBConnection):
        """
        Load the given posts into the database.
        """
        db_connection.add_many(
            [
                {
                    "uri": post.get("id"),
                    "text": post.get("text"),
                    "created_at": post.get("created_at"),
                    "like_count": post.get("public_metrics", {}).get("like_count"),
                    "reply_count": post.get("public_metrics", {}).get("reply_count"),
                    "repost_count": post.get("public_metrics", {}).get("retweet_count"),
                }
                for post in posts
            ],
        )

    def _posts_generator(
        self, query: str, since: Optional[str] = None, until: Optional[str] = None, batch_size: int = 100
    ) -> Generator[List[models.AppBskyFeedDefs.PostView], None, None]:
        posts, cursor = self.client.query_posts(q=query, sort="latest", since=since, until=until, limit=batch_size)
        yield posts
        while cursor:
            posts, cursor = self.client.query_posts(
                q=query, sort="latest", since=since, until=until, limit=batch_size, cursor=cursor
            )
            yield posts

    def produce(
        self,
        query: str,
        utc_since: Optional[datetime],
        utc_until: Optional[datetime],
        db_connection: DBConnection,
        posts_batch_size: int = 100,
    ):
        """
        Produce data from the Twitter API using the provided query.
        """
        db_connection.connect("posts")
        db_connection.connect("likes")
        if utc_since is not None:
            since = utc_since.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if utc_until is not None:
            until = utc_until.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        total_posts = 0
        total_likes = 0
        for post_minibatch in self._posts_generator(query, since=since, until=until, batch_size=posts_batch_size):
            self._load_posts_to_db(post_minibatch, db_connection)
            total_posts += len(post_minibatch)

        return total_posts + total_likes
