"""
Spark application to process the Youtube comment data from the Landing Layer into
the Trusted Layer.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, when
from pyspark.sql.window import Window


def main(input_path: str, output_path: str):
    spark = SparkSession.builder.appName("YoutubeCommentsCleaner").getOrCreate()
    df = spark.read.format("delta").load(f"s3a://{input_path}")
    # channelName: str
    # channelId: str ("Unknown" if not present)
    # comment: str
    # likes: int
    # replies: int
    # publishedAt: str ("Unknown" if not present)
    # videoId: str
    # threadId: str

    # Convert the "Unknown" values to None in the columns channelId and publishedAt
    df = df.withColumn("channelId", when(df["channelId"] == "Unknown", None).otherwise(df["channelId"]))
    df = df.withColumn("publishedAt", when(df["publishedAt"] == "Unknown", None).otherwise(df["publishedAt"]))

    # If threadId, videoId, comment or publishedAt are not present, drop the row
    df = df.dropna(subset=["threadId", "videoId", "comment", "publishedAt"])
    # If likes or replies are not present, set them to 0
    df = df.fillna({"likes": 0, "replies": 0})

    # Cast publishedAt to timestamp
    df = df.withColumn("publishedAt", df["publishedAt"].cast("timestamp"))

    # Cast likes and replies to int
    df = df.withColumn("likes", df["likes"].cast("int"))
    df = df.withColumn("replies", df["replies"].cast("int"))

    # If two rows have the same threadId, keep the one with the latest publishedAt
    window_spec = Window.partitionBy("threadId").orderBy(df["publishedAt"].desc())
    df = df.withColumn("row_number", row_number().over(window_spec))
    df = df.filter(df["row_number"] == 1).drop("row_number")

    # Write the DataFrame to the output path in Delta format
    df.write.format("delta").mode("overwrite").save(f"s3a://{output_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input delta table path")
    parser.add_argument("--output", required=False, help="Output delta table path")

    args = parser.parse_args()
    main(args.input, args.output)
