"""
Spark application to process the Bluesky data from the Landing Layer into the
Trusted Layer.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window


def main(input_path: str, output_path: str):
    spark = (
        SparkSession.builder.appName("BlueskyLikesLandingToTrusted")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    df = spark.read.format("delta").load(input_path)
    # actor_did: str
    # actor_handle: str
    # actor_display_name: str
    # created_at: datetime (str)
    # post_uri: str

    # If post_uri, author_did or created_at are not present, drop the row
    df = df.dropna(subset=["post_uri", "actor_did", "created_at"])

    # Cast created_at to timestamp
    df = df.withColumn("created_at", df["created_at"].cast("timestamp"))

    # If two rows have the same post_uri and actor_did, keep the one with the latest created_at
    window_spec = Window.partitionBy("post_uri", "actor_did").orderBy(df["created_at"].desc())
    df = df.withColumn("row_number", row_number().over(window_spec))
    df = df.filter(df["row_number"] == 1).drop("row_number")

    # Write the DataFrame to the output path in Delta format
    df.write.format("delta").mode("overwrite").save(output_path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input delta table path")
    parser.add_argument("--output", required=False, help="Output delta table path")

    args = parser.parse_args()
    main(args.input, args.output)
