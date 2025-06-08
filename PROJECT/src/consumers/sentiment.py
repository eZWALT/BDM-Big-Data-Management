import argparse
import pandas as pd
import yaml
from loguru import logger
from datetime import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from minio import Minio
from minio.error import S3Error
import duckdb
import time  # for retries

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, coalesce
from pyspark.sql.types import StringType


def get_minio_client(minio_endpoint, access_key, secret_key, secure=False):
    """
    Returns a MinIO client instance.
    """
    return Minio(
        minio_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
    )


def download_duckdb_from_minio(minio_client, bucket_name, object_name, local_path):
    """
    Attempts to download an existing DuckDB file from MinIO.
    Returns True if successful, False if file not found or error.
    """
    try:
        minio_client.fget_object(bucket_name, object_name, local_path)
        logger.info(f"Downloaded existing DuckDB file from s3://{bucket_name}/{object_name}")
        return True
    except S3Error as e:
        logger.warning(f"DuckDB file not found on MinIO: s3://{bucket_name}/{object_name} - Will create a new DB locally")
        return False
    except Exception as e:
        logger.error(f"Error accessing MinIO: {e} - Will proceed with local DB only")
        return False


def upload_to_minio(client, local_path, bucket_name, object_name, max_retries=3, retry_delay=2):
    """
    Upload a local file to MinIO with retries and bucket existence check.
    Expects an initialized MinIO client object.
    """
    try:
        # Make sure bucket exists
        found = client.bucket_exists(bucket_name)
        if not found:
            client.make_bucket(bucket_name)
            logger.info(f"Bucket '{bucket_name}' created in MinIO")

        # Upload file with retry
        for attempt in range(1, max_retries + 1):
            try:
                client.fput_object(bucket_name, object_name, local_path)
                logger.info(f"Uploaded {local_path} to s3://{bucket_name}/{object_name}")
                break
            except S3Error as e:
                logger.warning(f"Upload attempt {attempt} failed: {e}")
                if attempt < max_retries:
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Failed to upload after {max_retries} attempts")
                    raise
    except Exception as e:
        logger.error(f"Error during upload to MinIO: {e}")
        raise


# -------------------------------
# VADER UDF for individual fields
# -------------------------------
def create_vader_udf():
    analyzer = SentimentIntensityAnalyzer()

    def analyze(text):
        try:
            if text and isinstance(text, str) and text.strip():
                score = analyzer.polarity_scores(text)["compound"]
                if score >= 0.05:
                    return "positive"
                elif score <= -0.05:
                    return "negative"
                else:
                    return "neutral"
            else:
                return "unknown"
        except:
            return "unknown"

    return udf(analyze, StringType())


# -------------------------------
# Cast Pandas DF using YAML Schema
# -------------------------------
def cast_df(df: pd.DataFrame, schema_path: str):
    with open(schema_path, "r") as f:
        meta = yaml.safe_load(f)
    columns = meta["columns"]

    type_map = {
        "string": "string",
        "int": "int64",
        "float": "float64",
        "bool": "bool"
    }

    result = pd.DataFrame()
    for col in columns:
        name = col["name"]
        dtype = type_map.get(col["type"].lower(), "string")
        if name in df.columns:
            result[name] = df[name].astype(dtype)
        else:
            result[name] = pd.Series([None] * len(df), dtype=dtype)

    return meta["table"], result

def unknown_to_none(sentiment):
    return None if sentiment == "unknown" else sentiment

# -------------------------------
# Main Function
# -------------------------------
def main(input_path: str,
         output_path: str,
         #schema_path: str,
         minio_host: str,
         minio_port: str,
         minio_access_key: str,
         minio_secret_key: str
        ):
    spark = SparkSession.builder.appName("VaderSentimentAnalyzer").getOrCreate()
    df = spark.read.format("delta").load(f"s3a://{input_path}")

    vader_udf = create_vader_udf()
    df = df.withColumn("text_sentiment", vader_udf(df["text"]))
    df = df.withColumn("title_sentiment", vader_udf(df["title"]))
    df = df.withColumn("description_sentiment", vader_udf(df["description"]))

    unknown_to_none_udf = udf(unknown_to_none, StringType())

    df = df.withColumn("title_sentiment", unknown_to_none_udf(df["title_sentiment"]))
    df = df.withColumn("text_sentiment", unknown_to_none_udf(df["text_sentiment"]))
    df = df.withColumn("description_sentiment", unknown_to_none_udf(df["description_sentiment"]))

    df = df.withColumn(
        "sentiment",
        coalesce(
            df["title_sentiment"],
            df["text_sentiment"],
            df["description_sentiment"],
        )
    ).fillna("unknown")


    table_name = "sentiment"
    df = df.toPandas()
    df["transaction_timestamp"] = datetime.utcnow().isoformat()

    parts = output_path.split("/", 1)
    bucket_name = parts[0]
    object_name = parts[1] if len(parts) > 1 else "sentiment.duckdb"
    local_db_path = "/tmp/sentiment.duckdb"

    # Initialize MinIO client
    minio_endpoint = f"{minio_host}:{minio_port}"
    minio_client = get_minio_client(minio_endpoint, minio_access_key, minio_secret_key)

    # Try to download existing DuckDB from MinIO
    exists = download_duckdb_from_minio(minio_client, bucket_name, object_name, local_db_path)

    con = duckdb.connect(local_db_path)

    # Create table if not exists
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS 
        SELECT * FROM df WHERE FALSE
    """)

    # Append new data
    con.register("temp_df", df)
    con.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")

    con.close()

    # Upload back to MinIO only if MinIO is reachable (file existed or not)
    try:
        upload_to_minio(
            client=minio_client,
            local_path=local_db_path,
            bucket_name=bucket_name,
            object_name=object_name
        )
    except Exception as e:
        logger.error(f"Failed to upload to MinIO: {e}. The DB file remains local.")

    spark.stop()
    logger.info(f"Appended results to {output_path} in table {table_name}")


# -------------------------------
# Entrypoint
# -------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # Default values should never be used they only serve as a visual placeholder
    parser.add_argument(
        "--input", default="exploitation/{company_id}/{product_id}/data", help="Delta table input path (MinIO)"
    )
    parser.add_argument(
        "--output", default="exploitation/{company_id}/{product_id}/data", help="DuckDB output path (MinIO)"
    )
    # parser.add_argument(
    #     "--schema", default="governance/sentiment_warehouse.yaml", help="YAML warehouse schema file path"
    # )
    parser.add_argument("--minio_host", required=True, help="MinIO host")
    parser.add_argument("--minio_port", required=True, help="MinIO port")
    parser.add_argument("--minio_access_key", required=True, help="MinIO access key")
    parser.add_argument("--minio_secret_key", required=True, help="MinIO secret key")
    args = parser.parse_args()
    main(
        args.input,
        args.output,
        args.minio_host,
        args.minio_port,
        args.minio_access_key,
        args.minio_secret_key,
        #args.schema
    )
