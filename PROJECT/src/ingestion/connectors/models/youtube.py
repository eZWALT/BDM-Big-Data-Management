from typing import Any, List, Optional, TypedDict


class PaginableResponse(TypedDict):
    class PageInfo(TypedDict):
        totalResults: int
        resultsPerPage: int

    etag: str
    items: List[Any]
    kind: str
    nextPageToken: Optional[str]
    prevPageToken: Optional[str]
    pageInfo: PageInfo
    regionCode: str


class VideoSearchResponse(PaginableResponse):
    class Item(TypedDict):
        class Id(TypedDict):
            kind: str
            videoId: str

        class Snippet(TypedDict):
            class Thumbnails(TypedDict):
                class ThumbnailData(TypedDict):
                    height: int
                    url: str
                    width: int

                default: ThumbnailData
                high: ThumbnailData
                medium: ThumbnailData

            channelId: str
            channelTitle: str
            description: str
            liveBroadcastContent: str
            publishTime: str
            publishedAt: str
            thumbnails: Thumbnails
            title: str
            tags: Optional[List[str]]

        etag: str
        id: Id
        kind: str
        snippet: Snippet

    items: List[Item]


class VideoDetailsResponse(PaginableResponse):
    class Item(TypedDict):

        class ContentDetails(TypedDict):
            caption: str
            contentRating: dict
            definition: str
            dimension: str
            duration: str
            licensedContent: bool
            projection: str

        class Statistics(TypedDict):
            commentCount: str
            favoriteCount: str
            likeCount: str
            viewCount: str

        contentDetails: ContentDetails
        etag: str
        id: str
        kind: str
        statistics: Statistics

    items: List[Item]


class CommentThreadResponse(PaginableResponse):
    class Item(TypedDict):
        class Snippet(TypedDict):
            class TopLevelComment(TypedDict):
                class Snippet(TypedDict):
                    class AuthorChannelId(TypedDict):
                        value: str

                    authorChannelId: AuthorChannelId
                    authorChannelUrl: str
                    authorDisplayName: str
                    authorProfileImageUrl: str
                    canRate: bool
                    channelId: str
                    likeCount: int
                    publishedAt: str
                    textDisplay: str
                    textOriginal: str
                    updatedAt: str
                    videoId: str
                    viewerRating: str

                etag: str
                id: str
                kind: str
                snippet: Snippet

            canReply: bool
            channelId: str
            isPublic: bool
            topLevelComment: TopLevelComment
            totalReplyCount: int
            videoId: str

        etag: str
        id: str
        kind: str
        snippet: Snippet

    items: List[Item]


class VideoBasicData(TypedDict):
    title: str
    description: str
    url: str
    videoId: str
    channel: str
    publishedAt: str
    thumbnail: str


class VideoStatistics(TypedDict):
    viewCount: int
    likeCount: int
    commentCount: int
    duration: int
    definition: int


class VideoComment(TypedDict):
    channelName: str
    channelId: str
    comment: str
    likes: int
    replies: int
    publishedAt: str
    videoId: str
