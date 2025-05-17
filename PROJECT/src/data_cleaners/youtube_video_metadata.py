"""
Spark application to process the Youtube Video Metadata from the Landing Layer into
the Trusted Layer.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, when
from pyspark.sql.window import Window


def main(input_path: str, output_path: str):
    spark = SparkSession.builder.appName("YoutubeVideoMetadataCleaner").getOrCreate()
    df = spark.read.format("delta").load(f"s3a://{input_path}")
    # title: str
    # description: str
    # url: str
    # videoId: str
    # channel: str
    # publishedAt: str
    # tags: str
    # thumbnail: str (The url)
    # viewCount: Optional[int]
    # likeCount: Optional[int]
    # commentCount: Optional[int]
    # duration: Optional[str]
    # definition: Optional[str]
    # captions: Optional[str]

    # If videoId or publishedAt are not present, drop the row
    df = df.dropna(subset=["videoId", "publishedAt"])
    # If likes or replies are not present, set them to 0
    df = df.fillna({"viewCount": 0, "likeCount": 0, "commentCount": 0})

    # Cast publishedAt to timestamp
    df = df.withColumn("publishedAt", df["publishedAt"].cast("timestamp"))

    # Cast viewCount and likeCount and commentCount to int
    df = df.withColumn("viewCount", df["viewCount"].cast("int"))
    df = df.withColumn("likeCount", df["likeCount"].cast("int"))
    df = df.withColumn("commentCount", df["commentCount"].cast("int"))

    # If two rows have the same videoId, keep the one with the latest publishedAt
    window_spec = Window.partitionBy("videoId").orderBy(df["publishedAt"].desc())
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
