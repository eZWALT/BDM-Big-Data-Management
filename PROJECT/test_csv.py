import random

from spark.sql import SparkSession

data = [
    (random.randint(-100, 100), random.randint(-100, 100), random.randint(-100, 100), random.randint(-100, 100))
    for _ in range(1000)
]

spark = SparkSession.builder.appName("CSV Test").getOrCreate()

df = spark.createDataFrame(data, ["a", "b", "c", "d"])
df.write.format("csv").mode("overwrite").option("header", "true").save("s3a://landing/example_csv_2")
