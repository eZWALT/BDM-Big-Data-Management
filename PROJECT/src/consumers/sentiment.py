import argparse
import pandas as pd
import duckdb
import yaml
from loguru import logger

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, coalesce
from pyspark.sql.types import StringType

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime

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

# -------------------------------
# Main Function
# -------------------------------
def main(input_path: str, output_path: str, schema_path: str):
    spark = SparkSession.builder.appName("VaderSentimentAnalyzer").getOrCreate()
    df = spark.read.format("delta").load(f"s3a://{input_path}")

    # --- Create UserDefinedFunction to do sentiment analysis per field
    vader_udf = create_vader_udf()
    df = df.withColumn("text_sentiment", vader_udf(df["text"]))
    df = df.withColumn("title_sentiment", vader_udf(df["title"]))
    df = df.withColumn("description_sentiment", vader_udf(df["description"]))

    # --- Derive overall sentiment to just 1 variable
    df = df.withColumn(
        "sentiment",
        coalesce(
            df["text_sentiment"],
            df["title_sentiment"],
            df["description_sentiment"]
        )
    )

    # --- Load schema metadata to extract the table name ---
    with open(schema_path, "r") as f:
        meta = yaml.safe_load(f)
    table_name = meta["table"]
    df = df.toPandas()
    
    # --- Optionally enforce schema casting ---
    # table_name, df = cast_df(df, schema_path)

    # --- Add transaction timestamp column ---
    df["transaction_timestamp"] = datetime.utcnow().isoformat()
    # --- Append data to DuckDB table ---
    con = duckdb.connect(output_path)

    # Create table with schema (if not exists) (schema trick :)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS 
        SELECT * FROM df WHERE FALSE
    """)

    # Register and append data (Remember guys its a OLAP Warehouse :)
    con.register("temp_df", df)
    con.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")

    spark.stop()
    logger.info(f"Appended results to {output_path} in table {table_name}")


# -------------------------------
# Entrypoint
# -------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input", default="exploitation", help="Delta table input path (MinIO)"
    )
    parser.add_argument(
        "--output", default="sconsumers/sentiment.duckdb", help="DuckDB output path"
    )
    parser.add_argument(
        "--schema", default="governance/sentiment_warehouse.yaml", help="YAML warehouse schema file path"
    )

    args = parser.parse_args()
    main(args.input, args.output, args.schema)
