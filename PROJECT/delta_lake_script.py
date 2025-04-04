from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import col
import os

# ===-----------------------------------------------------------------------===#
# Script to create and insert data into delta lake                             #
#                                                                              #
# Author: Mateja Zatezalo                                                      #
# ===-----------------------------------------------------------------------===#

def create_spark_session():
    return SparkSession.builder \
        .appName("DeltaLakeApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

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



def load_jsonl_to_delta(spark_session, source_directory, delta_table_path):
    # Traverse the directory structure to find all JSONL files
    for subdir, dirs, files in os.walk(source_directory):
        for file in files:
            if file.endswith('.jsonl'):
                file_path = os.path.join(subdir, file)
                # Read the JSONL file into a DataFrame
                df = spark_session.read.json(file_path)
                
                # Write the DataFrame to Delta Lake
                df.write.format("delta").mode("append").save(delta_table_path)
        

def main():
    # Create Spark session with Delta Lake support
    spark_session = create_spark_session()

    # Define the path for the Delta table
    delta_table_path = "data_lake/landing/persistent/delta-table"

    # Path where we get the data from
    data_path = "data_lake/landing/temporal"

    # Get all textual .jsonl files into the delta lake
    load_jsonl_to_delta(spark_session, data_path, delta_table_path)

    # Load the Delta table
    df = spark_session.read.format("delta").load(delta_table_path)
    df.show()

    # Stop the Spark session
    spark_session.stop()

if __name__ == "__main__":  
    main()

""" How to run the delta lake
    spark-submit \
  --packages io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  delta_lake_script.py
"""
