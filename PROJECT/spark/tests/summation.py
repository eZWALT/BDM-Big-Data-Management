from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName("Summation") \
            .master("spark://localhost:7077") \
            .getOrCreate()

    # Create a resilient distributed dataset 1-1000
    numbers_rdd = spark.sparkContext.parallelize(range(1,100000000))
    # COunt the elements in the RDD
    sum = numbers_rdd.sum()
    print(f"sum of numbers: {sum}")
    spark.stop()
        
            
    