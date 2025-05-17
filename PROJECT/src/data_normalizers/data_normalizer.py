"""
Spark application to process simultaneously data from all of the Trusted Zone
sources into a single Delta table in the Exploitation Zone.

This application loads all of the dataframes from the Trusted Zone, performs
necessary transformations, unifies their schemas, groups them into a single
table, and writes the result into the Exploitation Zone.

Each time this application is run, at the start, it will get the current time
and use it to create a new extraction date, and all data will be appended to the
exploitation zone table. This will ensure that data in there is time-invariant
and non-volatile.

Note for the future: Instead of using the current time, which relies on the
clocks of the drivers to be synchronized, we should use a centralized extraction
metadata store, which would dispatch the extraction identifier.

The input of this application is a list of paths to the Trusted Zone topics,
along with their corresponding social network sources. The application will
process all the tables inside a topic, knowing that they all have the correct
schema, defined in the data_cleaners module.

The schema of the output table has the following columns:
- source (str): The name of the social network it came from (e.g., "bluesky").
- uri (str): The URI of the post within the same social network. (Note: to get
    an absolute URI, we need to join it with the social network domain).
- created_at (datetime): The date and time when the post was created.
- author_uri (Optional[str]): The URI of the author of the post within the same social
    network. (Note: to get an absolute URI, we need to join it with the social
    network domain).
- title (Optional[str]): The title of the post, if available.
- description (Optional[str]): The description of the post, if available.
- text (Optional[str]): The text content of the post, if available.
- video_attachments (Optional[List[str]]): A list of the uris of the video
    attachments, where each uri is a string pointing to the video file in the
    data store, if available.
- image_attachments (Optional[List[str]]): A list of the uris of the image
    attachments, where each uri is a string pointing to the image file in the
    data store, if available.
- audio_attachments (Optional[List[str]]): A list of the uris of the audio
    attachments, where each uri is a string pointing to the audio file in the
    data store, if available.
- views (Optional[int]): The number of views of the post, if available.
- shares (Optional[int]): The number of shares of the post, if available.
- likes (Optional[int]): The number of positive reactions to the post, if available.
- dislikes (Optional[int]): The number of negative reactions to the post, if available.
- reposts (Optional[int]): The number of reposts of the post, if available.
- replies (Optional[int]): The number of replies to the post, if available.
- quotes (Optional[int]): The number of quotes of the post, if available.
"""

from functools import reduce
from typing import List, Literal, Tuple

# from minio import Minio
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit


def process_bluesky(spark: SparkSession, input_topic_path: str) -> DataFrame:
    """
    Bluesky topic contains two tables: Posts and Likes, with the following
    schemas:

    Posts:
        uri: str
        text: str
        created_at: datetime
        author_did: str
        author_handle: str
        author_display_name: str
        like_count: int
        repost_count: int
        reply_count: int
        quote_count: int

    Likes:
        actor_did: str
        actor_handle: str
        actor_display_name: str
        created_at: datetime
        post_uri: str
    """
    df_posts = spark.read.format("delta").load(f"s3a://{input_topic_path}/posts")
    df_likes = spark.read.format("delta").load(f"s3a://{input_topic_path}/likes")
    # Select only the columns we need from the posts table
    df_posts = df_posts.select(
        col("uri"),
        col("text"),
        col("created_at"),
        col("author_did").alias("author_uri"),
        col("repost_count").alias("reposts"),
        col("reply_count").alias("replies"),
        col("quote_count").alias("quotes"),
    )

    # Add the null columns that we don't have in the bluesky data
    df_posts = df_posts.withColumn("title", lit(None).cast("string"))
    df_posts = df_posts.withColumn("description", lit(None).cast("string"))
    df_posts = df_posts.withColumn("video_attachments", lit(None).cast("array<string>"))
    df_posts = df_posts.withColumn("image_attachments", lit(None).cast("array<string>"))
    df_posts = df_posts.withColumn("audio_attachments", lit(None).cast("array<string>"))
    df_posts = df_posts.withColumn("views", lit(None).cast("int"))
    df_posts = df_posts.withColumn("shares", lit(None).cast("int"))
    df_posts = df_posts.withColumn("dislikes", lit(None).cast("int"))

    # Finally, re-count the likes from the likes table
    df_likes = df_likes.groupBy("post_uri").count().withColumnRenamed("count", "likes")
    # Join the likes table with the posts table
    df = df_posts.join(df_likes, df_posts.uri == df_likes.post_uri, "left")
    df = df.drop(df_likes.post_uri)

    # Add the source column
    df = df.withColumn("source", lit("bluesky"))

    return df


def process_twitter(spark: SparkSession, input_topic_path: str) -> DataFrame:
    """
    Twitter topic contains one single table: Posts, with the following schema:

    Posts:
        uri: str
        text: str
        created_at: datetime
        like_count: int
        reply_count: int
        repost_count: int
    """
    df = spark.read.format("delta").load(f"s3a://{input_topic_path}/posts")
    # Select only the columns we need from the posts table
    df = df.select(
        col("uri"),
        col("text"),
        col("created_at"),
        col("like_count").alias("likes"),
        col("reply_count").alias("replies"),
        col("repost_count").alias("reposts"),
    )

    # Add the null columns that we don't have in the twitter data
    df = df.withColumn("author_uri", lit(None).cast("string"))
    df = df.withColumn("title", lit(None).cast("string"))
    df = df.withColumn("description", lit(None).cast("string"))
    df = df.withColumn("video_attachments", lit(None).cast("array<string>"))
    df = df.withColumn("image_attachments", lit(None).cast("array<string>"))
    df = df.withColumn("audio_attachments", lit(None).cast("array<string>"))
    df = df.withColumn("views", lit(None).cast("int"))
    df = df.withColumn("shares", lit(None).cast("int"))
    df = df.withColumn("dislikes", lit(None).cast("int"))
    df = df.withColumn("quotes", lit(None).cast("int"))

    # Add the source column
    df = df.withColumn("source", lit("twitter"))

    return df


def process_youtube(spark: SparkSession, input_topic_path: str) -> DataFrame:
    """
    Youtube topic contains two tables: Video Metadata, and Comments.
    And three BLOB stores: Videos, Thumbnails, and Audios.

    The schemas of the tables are as follows:

    Video Metadata:
        title: str
        description: str
        url: str
        videoId: str
        channel: str
        publishedAt: str
        tags: str
        thumbnail: str (The url)
        viewCount: Optional[int]
        likeCount: Optional[int]
        commentCount: Optional[int]
        duration: Optional[str]
        definition: Optional[str]
        captions: Optional[str]

    Comments: (We will ignore this table for now, as YT API does only return a few top level comments)
        channelName: str
        channelId: Optional[str]
        comment: str
        likes: int
        replies: int
        publishedAt: datetime
        videoId: str
        threadId: str
    """
    video_metadata_df = spark.read.format("delta").load(f"s3a://{input_topic_path}/video_metadata")
    # Select only the columns we need from the video metadata table
    video_metadata_df = video_metadata_df.select(
        col("videoId").alias("uri"),
        col("title"),
        col("description"),
        col("channel").alias("author_uri"),
        col("publishedAt").alias("created_at"),
        col("viewCount").alias("views"),
        col("likeCount").alias("likes"),
        col("commentCount").alias("replies"),
        col("captions").alias("text"),
    )

    # For each video, we will do the following:
    # 1. Look in the input_path/videos folder for a file with the same name as the videoId
    #    If one is found, copy it to the output_path/videos/youtube folder and add the path to the video_attachments column
    # 2. Look in the input_path/thumbnails folder for a file with the same name as the videoId
    #    If one is found, copy it to the output_path/images/youtube folder and add the path to the image_attachments column
    # 3. Look in the input_path/audios folder for a file with the same name as the videoId
    #    If one is found, copy it to the output_path/audios/youtube folder and add the path to the audio_attachments column

    def handle_attachments(row):
        video_id = row["uri"]
        video_path = f"s3a://{input_topic_path}/videos/{video_id}.mp4"
        thumbnail_path = f"s3a://{input_topic_path}/thumbnails/{video_id}.jpg"
        audio_path = f"s3a://{input_topic_path}/audios/{video_id}.mp3"

        video_attachments = (
            [video_path]
            if spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()).exists(
                spark._jvm.org.apache.hadoop.fs.Path(video_path)
            )
            else []
        )
        image_attachments = (
            [thumbnail_path]
            if spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()).exists(
                spark._jvm.org.apache.hadoop.fs.Path(thumbnail_path)
            )
            else []
        )
        audio_attachments = (
            [audio_path]
            if spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()).exists(
                spark._jvm.org.apache.hadoop.fs.Path(audio_path)
            )
            else []
        )

        return row + (video_attachments, image_attachments, audio_attachments)

    df = video_metadata_df.rdd.map(handle_attachments).toDF(
        [
            "uri",
            "title",
            "description",
            "author_uri",
            "created_at",
            "views",
            "likes",
            "replies",
            "text",
            "video_attachments",
            "image_attachments",
            "audio_attachments",
        ]
    )

    # Add the null columns that we don't have in the youtube data
    df = df.withColumn("shares", lit(None).cast("int"))
    df = df.withColumn("dislikes", lit(None).cast("int"))
    df = df.withColumn("reposts", lit(None).cast("int"))
    df = df.withColumn("quotes", lit(None).cast("int"))

    # Add the source column
    df = df.withColumn("source", lit("youtube"))
    return df


def main(input_sources: List[Tuple[Literal["bluesky", "youtube", "twitter"], str]], output_path: str):
    spark = SparkSession.builder.appName("DataNormalizer").getOrCreate()

    source_dataframes: List[DataFrame] = []
    for source, input_topic_path in input_sources:
        if source == "bluesky":
            res = process_bluesky(spark, input_topic_path)
        elif source == "youtube":
            res = process_youtube(spark, input_topic_path)
        elif source == "twitter":
            res = process_twitter(spark, input_topic_path)
        else:
            raise ValueError(f"Unknown source: {source}")

        source_dataframes.append(res)
    # Combine all dataframes into one
    combined_df = reduce(lambda df1, df2: df1.unionByName(df2), source_dataframes)

    # De-duplicate any posts that were captured twice by different queries, based
    # on a combination of the uri and the source
    combined_df = combined_df.dropDuplicates(["uri", "source"])

    # Add the extraction id column
    combined_df = combined_df.withColumn("extraction_id", lit(current_timestamp()))

    # Write the DataFrame to the output path in Delta format
    combined_df.write.format("delta").mode("append").save(f"s3a://{output_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=False, help="Output delta table path")
    parser.add_argument(
        "--input",
        required=False,
        nargs="+",
        type=str,
        help="Input topic path, with the format <source>:<path>. Example: bluesky:/path/to/bluesky",
    )

    args = parser.parse_args()
    inputs = []
    for input_source in args.input:
        source, path = input_source.split(":", 1)
        inputs.append((source, path))

    main(inputs, args.output)
