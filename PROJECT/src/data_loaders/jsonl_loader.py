"""
Spark application to load data from a series of JSONL files into a Delta table.

Args:
    input_path (str): Path to the folder containing the JSONL files.
    output_path (str): Path to the output Delta table.
"""

import os

from pyspark.sql import SparkSession


def main(input_path: str, output_path: str):
    spark = SparkSession.builder.appName("JSONLDataLoader").getOrCreate()
    input_glob = f"s3a://{input_path}/*.jsonl"
    df = spark.read.json(input_glob)

    # We won't perform data cleaning whatsoever, we'll just load it into the
    # Landing Zone.

    # Write the DataFrame to the output path in Delta format
    df.write.format("delta").mode("append").save(f"s3a://{output_path}")
    # We use append here because we are accumulating data in the landing zone.

    # Empty the input path
    for file in os.listdir(input_path):
        os.remove(os.path.join(input_path, file))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input folder path")
    parser.add_argument("--output", required=False, help="Output delta table path")

    args = parser.parse_args()
    main(args.input, args.output)
