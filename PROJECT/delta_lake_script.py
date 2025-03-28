from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import col
from src.ingestion.twitter_client import tweets_df

def delete_from_delta_table(spark, delta_table_path):
    """
    Deletes data from a Delta Lake table.

    Parameters:
    - spark: SparkSession object.
    - delta_table_path: The path to the Delta Lake table.
    """
    # Ensure the table exists
    if DeltaTable.isDeltaTable(spark, delta_table_path):
        deltaTable = DeltaTable.forPath(spark, delta_table_path)
        deltaTable.delete()
    else:
        print("The specified path does not contain a Delta table.")
        

def main():
    # Create Spark session with Delta Lake support
    spark = SparkSession.builder \
        .appName("DeltaLake") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Define the path for the Delta table
    delta_table_path = "src/landing/delta-table"

    #delete_from_delta_table(spark, delta_table_path)

    # Load the Delta table
    #df = spark.read.format("delta").load(delta_table_path)
    #df.show()

    # Read the JSON into a DataFrame
    tweets_df = spark.read.csv('/data/tweets.json', header=True, inferSchema=True)

    # Write data to Delta Lake
    tweets_df.write.format("delta").mode("append").save(delta_table_path)

    # Load the Delta table
    df = spark.read.format("delta").load(delta_table_path)
    df.show()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":  
    main()