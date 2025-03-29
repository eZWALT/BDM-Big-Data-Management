from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import col

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
    delta_table_path = "data_lake/landing/persistent/delta-table"

    #delete_from_delta_table(spark, delta_table_path)

    # Path where we get the data from
    data_path = "data_lake/landing/temporal/twitter_data.csv"

    # Read the CSV into a DataFrame
    tweets_df = spark.read.format("csv").load(data_path)

    # Write data to Delta Lake
    tweets_df.write.format("delta").mode("overwrite").save(delta_table_path)

    # Load the Delta table
    df = spark.read.format("delta").load(delta_table_path)
    df.show()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":  
    main()