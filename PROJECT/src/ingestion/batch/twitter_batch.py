from datetime import datetime
from typing import Generator, List, Optional

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
    ) -> Generator[List[TweetData], None, None]:
        posts, cursor = self.client.fetch_tweets(query=query, since=since, until=until, limit=batch_size)
        yield posts
        while cursor:
            posts, cursor = self.client.fetch_tweets(
                query=query, since=since, until=until, limit=batch_size, cursor=cursor
            )
            yield posts

    def produce(
        self,
        query: str,
        utc_since: Optional[datetime],
        utc_until: Optional[datetime],
        posts_batch_size: int = 100,
        posts_db: DBConnection = None,
    ):
        """
        Produce data from the Twitter API using the provided query.
        """
        if utc_since is not None:
            utc_since = utc_since.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if utc_until is not None:
            utc_until = utc_until.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        total_posts = 0
        for post_minibatch in self._posts_generator(
            query, since=utc_since, until=utc_until, batch_size=posts_batch_size
        ):
            self._load_posts_to_db(post_minibatch, posts_db)
            total_posts += len(post_minibatch)

        return total_posts
