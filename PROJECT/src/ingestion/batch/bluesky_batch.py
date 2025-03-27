from typing import Generator, List

from atproto import models
from loguru import logger

from src.ingestion.connectors.bluesky_client import BlueSkyAPIClient
from src.utils.config import ConfigManager

from . import BatchProducer


class BlueskyBatchProducer(BatchProducer):
    def __init__(self):
        super().__init__()
        self.client = BlueSkyAPIClient()

    def _load_posts_to_db(self, posts: List[models.AppBskyFeedDefs.PostView], db_connection):
        """
        Load the given posts into the database.
        """
        # TODO: Implement the load logic to the database.
        pass

    def _load_likes_to_db(self, likes: List[models.AppBskyFeedGetLikes.Like], db_connection):
        """
        Load the given likes into the database.
        """
        # TODO: Implement the load logic to the database.
        pass

    def _posts_generator(
        self, query: str, batch_size: int = 100
    ) -> Generator[List[models.AppBskyFeedDefs.PostView], None, None]:
        posts, cursor = self.client.query_posts(q=query, sort="latest", limit=batch_size)
        yield posts
        while cursor:
            posts, cursor = self.client.query_posts(q=query, sort="latest", limit=batch_size, cursor=cursor)
            yield posts

    def _post_likes_generator(
        self, post_uri: str, batch_size: int = 100
    ) -> Generator[List[models.AppBskyFeedGetLikes.Like], None, None]:
        """
        Generator to fetch likes for a given post.
        """
        likes, cursor = self.client.get_post_likes(post_uri, limit=100)
        yield likes
        while cursor:
            likes, cursor = self.client.get_post_likes(post_uri, limit=100, cursor=cursor)
            yield likes

    def _likes_generator(
        self, posts: List[models.AppBskyFeedDefs.PostView], batch_size: int = 100
    ) -> Generator[List[models.AppBskyFeedGetLikes.Like], None, None]:
        """
        Generator to fetch likes for a list of posts.

        Not all batches are guaranteed to be the same size, nor even to have any like.
        """
        for post in posts:
            yield from self._post_likes_generator(post.uri, batch_size=batch_size)

    def produce(self, query: str, db_connection, posts_batch_size: int = 100, likes_batch_size: int = 100):
        """
        Produce data from the BlueSky API using the provided query.
        """
        logger.info(f"[BLUESKY PRODUCER] Producing post data for query: {query}")
        total_posts = 0
        total_likes = 0
        for post_minibatch in self._posts_generator(query, batch_size=posts_batch_size):
            self._load_posts_to_db(post_minibatch, db_connection)
            total_posts += len(post_minibatch)
            for likes_minibatch in self._likes_generator(post_minibatch, batch_size=likes_batch_size):
                self._load_likes_to_db(likes_minibatch, db_connection)
                total_likes += len(likes_minibatch)

        logger.info(f"[BLUESKY PRODUCER] Produced {total_posts} posts and {total_likes} likes for query: {query}")
