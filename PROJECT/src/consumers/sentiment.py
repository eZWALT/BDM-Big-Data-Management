import argparse
import pandas as pd
import duckdb
import yaml
from loguru import logger

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


# -------------------------------
# VADER UDF
# -------------------------------
def create_vader_udf():
    analyzer = SentimentIntensityAnalyzer()

    def analyze(text):
        try:
            score = analyzer.polarity_scores(text)["compound"]
            if score >= 0.05:
                return "positive"
            elif score <= -0.05:
                return "negative"
            else:
                return "neutral"
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


# -------------------------------
# Main Function
# -------------------------------
def main(input_path: str, output_path: str, schema_path: str):
    # Load
    spark = SparkSession.builder.appName("VaderSentimentAnalyzer").getOrCreate()
    df = spark.read.format("delta").load(f"s3a://{input_path}")

    vader_udf = create_vader_udf()
    # Lets simplify the problem and just get the labels
    df = df.withColumn("sentiment", vader_udf(df["text"]))

    table_name, casted = cast_df(df.toPandas(), schema_path)

    duckdb.connect(output_path).execute(
        f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM casted"
    )

    spark.stop()
    logger.info(f"Written results to {output_path} in table {table_name}")


# -------------------------------
# Entrypoint
# -------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input", default="exploitation", help="Delta table input path (MinIO)"
    )
    parser.add_argument(
        "--output", default="consumption/sentiment.duckdb", help="DuckDB output path"
    )
    parser.add_argument(
        "--schema", default="governance/sentiment_warehouse.yaml", help="YAML schema file path"
    )

    args = parser.parse_args()
    main(args.input, args.output, args.schema)
