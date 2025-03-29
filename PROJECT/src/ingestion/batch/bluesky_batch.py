from datetime import datetime
from typing import Generator, List, Optional, Tuple

from atproto import models
from loguru import logger

from src.ingestion.connectors.bluesky_client import BlueSkyAPIClient

from . import BatchProducer, DBConnection

# ===-----------------------------------------------------------------------===#
# BlueSky Batch Producer                                                       #
#                                                                              #
# Author: Marc Parcerisa                                                       #
# ===-----------------------------------------------------------------------===#


class BlueskyBatchProducer(BatchProducer):
    def __init__(self):
        super().__init__()
        self.client = BlueSkyAPIClient()

    def _load_posts_to_db(self, posts: List[models.AppBskyFeedDefs.PostView], db_connection: DBConnection):
        """
        Load the given posts into the database.
        """
        db_connection.add_many(
            "posts",
            [
                {
                    "uri": post.uri,
                    "text": post.record.text,
                    "created_at": post.record.created_at,
                    "author_did": post.author.did,
                    "author_handle": post.author.handle,
                    "author_display_name": post.author.display_name,
                    "like_count": post.like_count,
                    "repost_count": post.repost_count,
                    "reply_count": post.reply_count,
                    "quote_count": post.quote_count,
                }
                for post in posts
            ],
        )

    def _load_likes_to_db(
        self, likes: List[models.AppBskyFeedGetLikes.Like], post_uri: str, db_connection: DBConnection
    ):
        """
        Load the given likes into the database.
        """
        db_connection.add_many(
            "likes",
            [
                {
                    "actor_did": like.actor.did,
                    "actor_handle": like.actor.handle,
                    "actor_display_name": like.actor.display_name,
                    "created_at": like.created_at,
                    "post_uri": post_uri,
                }
                for like in likes
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

    def _post_likes_generator(
        self, post_uri: str, batch_size: int = 100
    ) -> Generator[List[models.AppBskyFeedGetLikes.Like], None, None]:
        """
        Generator to fetch likes for a given post.
        """
        likes, cursor = self.client.get_post_likes(post_uri, limit=batch_size)
        yield likes
        while cursor:
            likes, cursor = self.client.get_post_likes(post_uri, limit=batch_size, cursor=cursor)
            yield likes

    def _likes_generator(
        self, posts: List[models.AppBskyFeedDefs.PostView], batch_size: int = 100
    ) -> Generator[Tuple[List[models.AppBskyFeedGetLikes.Like], str], None, None]:
        """
        Generator to fetch likes for a list of posts.

        Not all batches are guaranteed to be the same size, nor even to have any like.
        """
        for post in posts:
            yield from map(lambda l: (l, post.uri), self._post_likes_generator(post.uri, batch_size=batch_size))

    def produce(
        self,
        query: str,
        db_connection: DBConnection,
        utc_since: Optional[datetime] = None,
        utc_until: Optional[datetime] = None,
        posts_batch_size: int = 100,
        likes_batch_size: int = 100,
    ):
        """
        Produce data from the BlueSky API using the provided query.
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
            for likes_minibatch, post_uri in self._likes_generator(post_minibatch, batch_size=likes_batch_size):
                self._load_likes_to_db(likes_minibatch, post_uri, db_connection)
                total_likes += len(likes_minibatch)

        return total_posts
