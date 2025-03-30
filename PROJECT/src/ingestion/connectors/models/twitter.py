from typing import List, Optional, TypedDict


class TweetData(TypedDict):
    class PublicMetrics(TypedDict):
        retweet_count: int
        reply_count: int
        like_count: int

    id: str
    text: str
    created_at: str
    public_metrics: PublicMetrics


class MetaData(TypedDict):
    result_count: int
    newest_id: str
    oldest_id: str
    next_token: Optional[str]


class TwitterResponse(TypedDict):
    data: List[TweetData]
    meta: MetaData
