"""
Spark application to process the Twitter post data from the Landing Layer into
the Trusted Layer.

WARNING: This should not be used. It is not tested, because the twitter API
does not work properly.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window


def main(input_path: str, output_path: str):
    spark = SparkSession.builder.appName("TwitterPostsCleaner").getOrCreate()
    df = spark.read.format("delta").load(f"s3a://{input_path}")
    # "uri": str
    # "text": str
    # "created_at": datetime (str)
    # "like_count": int
    # "reply_count": int
    # "repost_count": int

    # If uri, text or created_at are not present, drop the row
    df = df.dropna(subset=["uri", "text", "created_at"])
    # If like_count, repost_count, reply_count are not present, set them to 0
    df = df.fillna({"like_count": 0, "repost_count": 0, "reply_count": 0})

    # Cast created_at to timestamp
    df = df.withColumn("created_at", df["created_at"].cast("timestamp"))

    # Cast like_count, repost_count, reply_count to int
    df = df.withColumn("like_count", df["like_count"].cast("int"))
    df = df.withColumn("repost_count", df["repost_count"].cast("int"))
    df = df.withColumn("reply_count", df["reply_count"].cast("int"))

    # If two rows have the same uri, keep the one with the latest created_at
    window_spec = Window.partitionBy("uri").orderBy(df["created_at"].desc())
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
