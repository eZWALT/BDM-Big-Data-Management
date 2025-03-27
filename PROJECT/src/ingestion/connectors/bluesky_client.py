import os
from typing import List, Literal, Optional, Tuple, Union

from atproto import Client, models

from src.utils.config import ConfigManager


# ===-----------------------------------------------------------------------===#
# BlueSky API Client                                                           #
#                                                                              #
# Author: Marc Parcerisa                                                       #
# ===-----------------------------------------------------------------------===#
class BlueSkyAPIClient(object):
    def __init__(self, email: str = None, password: str = None, base_url: str = None):
        self.config_manager = ConfigManager(config_path="config/api.yaml")
        if not email or not password:
            credentials = self.config_manager.get_api_credentials("bluesky")
            self.email = email or credentials.get("email")
            self.password = password or credentials.get("password")
        else:
            self.email = email
            self.password = password

        if not self.email or not self.password:
            raise ValueError("User name and password must be provided for BlueSky authentication.")
        self.base_url = (
            base_url or self.config_manager.get_api_base_url("bluesky") or os.getenv("BLUESKY_BASE_URL") or None
        )

        self.client, self.profile = self._login(self.email, self.password)

    def _login(self, email: str, password: str):
        """
        Log in to the AT Proto client using environment variables.

        Args:
            email (str): The email for login.
            password (str): The password for login.
        Returns:
            client (Client): The AT Proto client instance.
            profile (ProfileViewDetailed): The user's profile information.
        """
        client = Client(base_url=self.base_url)
        profile = client.login(email, password)
        return client, profile

    def query_posts(
        self,
        q: str,
        author: Optional[str] = None,
        cursor: Optional[str] = None,
        domain: Optional[str] = None,
        lang: Optional[str] = None,
        limit: int = 100,
        mentions: Optional[str] = None,
        since: Optional[str] = None,
        sort: Optional[Union[Literal["top", "latest"], str]] = "latest",
        tag: Optional[List[str]] = None,
        until: Optional[str] = None,
        url: Optional[str] = None,
    ) -> Tuple[List[models.AppBskyFeedDefs.PostView], Optional[str]]:
        """
        Query posts from the BlueSky API.

        Args:
            q (str): Search query string; syntax, phrase, boolean, and faceting
                is unspecified, but Lucene query syntax is recommended.
            author (str, optional): Filter to posts by the given account.
                Handles are resolved to DID before query-time.
            cursor (str, optional): Optional pagination mechanism; may not
                necessarily allow scrolling through entire result set.
            domain (str, optional): Filter to posts with URLs (facet links or
                embeds) linking to the given domain (hostname). Server may apply
                hostname normalization.
            lang (str, optional): Filter to posts in the given language.
                Expected to be based on post language field, though server may
                override language detection.
            limit (int): Limit. Default is 100.
            mentions (str, optional): Filter to posts which mention the given
                account. Handles are resolved to DID before query-time. Only
                matches rich-text facet mentions.
            since (str, optional): Filter results for posts after the indicated
                datetime (inclusive). Expected to use `sortAt` timestamp, which
                may not match `createdAt`. Can be a datetime, or just an ISO
                date (YYYY-MM-DD).
            sort (str, optional): Specifies the ranking order of results.
            tag (List[str], optional): Filter to posts with the given tag
                (hashtag), based on rich-text facet or tag field. Do not include
                the hash (#) prefix. Multiple tags can be specified, with `AND`
                matching.
            until (str, optional): Filter results for posts before the indicated
                datetime (not inclusive). Expected to use `sortAt` timestamp,
                which may not match `createdAt`. Can be a datetime, or just an
                ISO date (YYY-MM-DD).
            url (str, optional): Filter to posts with links (facet links or
                embeds) pointing to this URL. Server may apply URL normalization
                or fuzzy matching.

        Returns:
            (posts, cursor) (Tuple[List[PostView], Optional[str]]): A list of
                posts matching the query, and a cursor for pagination.
        Raises:
            ValueError: If the limit is less than 1.
        """
        MAX_SEARCH_LIMIT = 100  # Hard limit imposed by the API

        # Validate the limit parameter
        if limit < 1:
            raise ValueError("limit must be greater than 1")

        params = {
            "q": q,
            "author": author,
            "cursor": cursor,
            "domain": domain,
            "lang": lang,
            "limit": min(limit, MAX_SEARCH_LIMIT),  # Limit to a maximum
            "mentions": mentions,
            "since": since,
            "sort": sort,
            "tag": tag,
            "until": until,
            "url": url,
        }
        response = self.client.app.bsky.feed.search_posts(params)
        posts = response.posts
        total_posts = len(posts)
        while total_posts < limit and response.cursor:
            params["cursor"] = response.cursor
            params["limit"] = min(limit - total_posts, MAX_SEARCH_LIMIT)
            response = self.client.app.bsky.feed.search_posts(params)
            posts.extend(response.posts)
            total_posts += len(response.posts)
        return posts, response.cursor

    def get_post_thread(
        self, uri: str, depth: Optional[int] = None, parent_height: Optional[int] = None
    ) -> models.AppBskyFeedDefs.ThreadViewPost:
        """
        Get a thread of posts from the BlueSky API.

        Args:
            uri (str): The URI of the post to get the thread for.
            depth (int, optional): The maximum depth of the thread to retrieve.
            parent_height (int, optional): The height of the parent post in the thread.
        Returns:
            thread (ThreadViewPost): The thread of posts.
        Raises:
            KeyError: If the post is not found.
            ValueError: If the post is blocked.
        """
        params = {
            "uri": uri,
            "depth": depth,
            "parent_height": parent_height,
        }
        response = self.client.app.bsky.feed.get_post_thread(params)
        if isinstance(response.thread, models.AppBskyFeedDefs.NotFoundPost):
            raise KeyError(f"Post not found: {uri}")
        elif isinstance(response.thread, models.AppBskyFeedDefs.BlockedPost):
            raise ValueError(f"Post blocked: {uri}")
        return response.thread

    def get_post_likes(
        self, uri: str, cid: Optional[str] = None, limit: Optional[int] = None, cursor: Optional[str] = None
    ) -> Tuple[List[models.AppBskyFeedGetLikes.Like], Optional[str]]:
        """
        Get the likes of a post from the BlueSky API.

        Args:
            uri (str): The URI of the post to get the likes for.
            limit (int, optional): The maximum number of likes to retrieve.
            cursor (str, optional): The cursor for pagination.
        Returns:
        """
        params = {
            "uri": uri,
            "cid": cid,
            "limit": limit,
            "cursor": cursor,
        }
        response = self.client.app.bsky.feed.get_likes(params)

        likes = response.likes
        total_likes = len(likes)
        while (limit is None or total_likes < limit) and response.cursor:
            params["cursor"] = response.cursor
            params["limit"] = limit - total_likes if limit else None
            response = self.client.app.bsky.feed.get_likes(params)
            likes.extend(response.likes)
            total_likes += len(response.likes)
        return likes, response.cursor


if __name__ == "__main__":
    import argparse

    from _bluesky_prints import print_like, print_thread_view_post

    parser = argparse.ArgumentParser(description="BlueSky API Client")
    parser.add_argument("query", type=str, help="Search query string")
    parser.add_argument("--limit", type=int, default=10, help="Limit for the number of posts")
    args = parser.parse_args()

    # Example usage
    bluesky_client = BlueSkyAPIClient()
    posts, cursor = bluesky_client.query_posts(args.query, limit=args.limit)

    for post in posts:
        thread = bluesky_client.get_post_thread(post.uri)
        likes, cursor = bluesky_client.get_post_likes(post.uri)
        print_thread_view_post(thread)
        print("Likes:")
        for j, like in enumerate(likes):
            print(f" ├─ {j}:")
            print_like(like, prefix=" │      ")
        print("-" * 80)
