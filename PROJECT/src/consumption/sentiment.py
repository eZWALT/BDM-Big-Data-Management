import duckdb
import pyarrow as pa

from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import torch.nn.functional as F
from loguru import logger 

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


# -------------------------------
# Load Sentiment Model and Labels
# -------------------------------
def load_model(model_name: str = "cardiffnlp/twitter-roberta-base-emotion"):
    logger.info(f"Loading model and tokenizer: {model_name}")
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(model_name)
    labels = model.config.id2label
    logger.info(f"Model loaded with labels: {list(labels.values())}")
    return tokenizer, model, labels


def analyze_sentiment_factory(tokenizer, model, labels):
    def analyze_sentiment(text: str) -> str:
        try:
            inputs = tokenizer(text, return_tensors="pt", truncation=True)
            with torch.no_grad():
                outputs = model(**inputs)
            probs = F.softmax(outputs.logits, dim=-1)
            label_idx = int(torch.argmax(probs))
            return labels[label_idx]
        except Exception as e:
            logger.error(f"Error during sentiment analysis for text: {text}. Error: {e}")
            return "unknown"
    return analyze_sentiment


# -------------------------------
# Read data from DuckDB
# -------------------------------
def read_data_from_duckdb(duckdb_path: str, query: str):
    logger.info(f"Connecting to DuckDB at {duckdb_path}")
    con = duckdb.connect(duckdb_path)
    arrow_table = con.execute(query).fetch_arrow_table()
    con.close()
    logger.info("Data loaded from DuckDB")
    return arrow_table


# -------------------------------
# Main processing function
# -------------------------------
def main():
    model_name = "cardiffnlp/twitter-roberta-base-emotion"
    query = "SELECT text FROM exploitation" # CHANGE THIS
    input_path = "data/exploitation.duckdb"
    output_path = "data/sentiment.duckdb"

    # Initialize Spark
    spark = SparkSession.builder.appName("DistributedSentimentAnalysis").getOrCreate()
    logger.info("Spark session started")

    # Load model/tokenizer
    tokenizer, model, labels = load_model(model_name)

    # Prepare UDF with closure on model objects
    sentiment_func = analyze_sentiment_factory(tokenizer, model, labels)
    sentiment_udf = udf(sentiment_func, StringType())

    # Load data from DuckDB
    arrow_table = read_data_from_duckdb(input_path, query)
    df = spark.createDataFrame(arrow_table)

    logger.info(f"Dataframe created with {df.count()} rows")

    # Apply sentiment analysis UDF
    logger.info("Applying sentiment analysis UDF to dataframe")
    df_with_sentiment = df.withColumn("predicted_label", sentiment_udf(df["text"]))

    # Save results
    logger.info(f"Saving results to {output_path}")
    df_with_sentiment.write.mode("overwrite").parquet(output_path)

    logger.info("Job complete")
    spark.stop()


if __name__ == "__main__":
    main()
