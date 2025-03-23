from pyspark.sql import SparkSession

def main():
    # Create Spark session with Delta Lake support
    spark = SparkSession.builder \
        .appName("DeltaLake") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Define the path for the Delta table
    delta_table_path = "/src/landing/delta-table"

    # Create a simple DataFrame
    data = spark.range(0, 5)
    data.write.format("delta").mode("overwrite").save(delta_table_path)

    # Load the Delta table
    df = spark.read.format("delta").load(delta_table_path)
    df.show()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()