import os
import yaml
import duckdb
import pyarrow as pa
import pandas as pd

from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import torch.nn.functional as F
from loguru import logger

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import StringType


# -------------------------------
# Model Loader and UDF Factory
# -------------------------------
def load_model(model_name: str):
    logger.info(f"Loading model and tokenizer: {model_name}")
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(model_name)
    labels = model.config.id2label
    logger.info(f"Model loaded with labels: {list(labels.values())}")
    return tokenizer, model, labels


def create_sentiment_udf(tokenizer, model, labels):
    def analyze_sentiment(text: str) -> str:
        try:
            inputs = tokenizer(text, return_tensors="pt", truncation=True)
            with torch.no_grad():
                outputs = model(**inputs)
            probs = F.softmax(outputs.logits, dim=-1)
            label_idx = int(torch.argmax(probs))
            return labels[label_idx]
        except Exception as e:
            logger.error(f"Error analyzing text: {text}. Error: {e}")
            return "unknown"
    return udf(analyze_sentiment, StringType())


# -------------------------------
# Schema Handling for DuckDB Output
# -------------------------------
def load_output_schema(schema_path: str):
    with open(schema_path, "r") as f:
        meta = yaml.safe_load(f)

    table_name = meta["table"]
    column_defs = meta["columns"]

    logger.info(f"Loaded output schema for table '{table_name}'")
    return table_name, column_defs


def cast_pandas_df(df: pd.DataFrame, column_defs):
    dtype_map = {
        "string": "string",
        "int": "int64",
        "float": "float64",
        "bool": "bool"
    }

    casted_df = pd.DataFrame()

    for col_def in column_defs:
        name = col_def["name"]
        dtype = dtype_map.get(col_def["type"].lower(), "string")
        if name in df.columns:
            casted_df[name] = df[name].astype(dtype)
        else:
            logger.warning(f"Missing column '{name}' in DataFrame. Filling with None.")
            casted_df[name] = pd.Series([None] * len(df), dtype=dtype)

    return casted_df


def write_to_duckdb(df, duckdb_path: str, schema_path: str):
    table_name, column_defs = load_output_schema(schema_path)
    pandas_df = df.toPandas()
    casted_df = cast_pandas_df(pandas_df, column_defs)

    con = duckdb.connect(duckdb_path)
    logger.info(f"Writing to DuckDB table '{table_name}' at {duckdb_path}")
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM casted_df")
    con.close()
    logger.info("Write complete")


# -------------------------------
# Spark I/O Helpers
# -------------------------------
def start_spark(app_name: str = "SentimentAnalysisApp") -> SparkSession:
    logger.info("Starting Spark session")
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def read_from_delta(spark: SparkSession, path: str):
    logger.info(f"Reading data from Delta Lake: {path}")
    return spark.read.format("delta").load(path)


# -------------------------------
# Pipeline
# -------------------------------
def run_sentiment_pipeline(
    model_name: str,
    delta_input_path: str,
    duckdb_output_path: str,
    schema_path: str
):
    spark = start_spark()

    tokenizer, model, labels = load_model(model_name)
    sentiment_udf = create_sentiment_udf(tokenizer, model, labels)

    df = read_from_delta(spark, delta_input_path)
    logger.info(f"Loaded {df.count()} records from Delta")

    df_preds = df.withColumn("predicted_label", sentiment_udf(df["text"]))

    write_to_duckdb(df_preds, duckdb_output_path, schema_path)

    spark.stop()
    logger.info("Pipeline finished")


# -------------------------------
# Entrypoint
# -------------------------------
if __name__ == "__main__":
    run_sentiment_pipeline(
        model_name="cardiffnlp/twitter-roberta-base-emotion",
        delta_input_path="delta/exploitation",
        duckdb_output_path="consumption/sentiment.duckdb",
        schema_path="governance/sentiment_warehouse.yaml"
    )
