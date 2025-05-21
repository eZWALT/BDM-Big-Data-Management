"""
Spark application to load data from a series of JSONL files into a Delta table.

Args:
    input_path (str): Path to the folder containing the JSONL files.
    output_path (str): Path to the output Delta table.
"""

from minio import Minio
from pyspark.sql import SparkSession


def main(
    input_path: str, output_path: str, minio_host: str, minio_port: str, minio_access_key: str, minio_secret_key: str
):
    spark = SparkSession.builder.appName("JSONLDataLoader").getOrCreate()
    input_glob = f"s3a://{input_path}/*.jsonl"

    # Before reading the data with Spark, we'll list the files with the MinIO
    # That way, we can delete them after loading the data. We don't care if we delete
    # less than we read, because we are going to clean the data in the trusted
    # zone anyway. What we don't want is to delete files that we haven't read yet.
    minio = Minio(f"{minio_host}:{minio_port}", access_key=minio_access_key, secret_key=minio_secret_key, secure=False)
    src_bucket, src_folder = input_path.split("/", 1)
    src_files = list(minio.list_objects(src_bucket, recursive=True, prefix=src_folder))
    if len(src_files) == 0:
        print(f"No files found in {input_path}")
        return

    df = spark.read.json(input_glob)

    # We won't perform data cleaning whatsoever, we'll just load it into the
    # Landing Zone.

    # Write the DataFrame to the output path in Delta format
    df.write.format("delta").mode("append").save(f"s3a://{output_path}")
    # We use append here because we are accumulating data in the landing zone.

    # Empty the input folder using minio
    for obj in src_files:
        minio.remove_object(src_bucket, obj.object_name)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input folder path")
    parser.add_argument("--output", required=False, help="Output delta table path")
    parser.add_argument("--minio_host", required=True, help="MinIO host")
    parser.add_argument("--minio_port", required=True, help="MinIO port")
    parser.add_argument("--minio_access_key", required=True, help="MinIO access key")
    parser.add_argument("--minio_secret_key", required=True, help="MinIO secret key")

    args = parser.parse_args()
    main(args.input, args.output, args.minio_host, args.minio_port, args.minio_access_key, args.minio_secret_key)
