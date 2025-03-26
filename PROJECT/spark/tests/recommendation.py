from pyspark.conf import SparkConf
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder
from pyspark import SparkFiles
import shutil
import os

# Step 1: Initialize Spark session
spark = SparkSession.builder \
    .master("spark://localhost:7077") \
    .appName("RecommendationSystem") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

#IF WE WANT TO EXECUTE THIS CODE REMOTELY WE NEED TO SEND THE CSV TO THE HDFS
#THIS EXAMPLE WON'T WORK DISTRIBUTEDLY :(
local = False 
local_file_path = "spark/tests/ml-ratings.csv"
if local:
    df = spark.read.csv("spark/tests/ml-ratings.csv", inferSchema=True, header=True).cache()
else:
    #Distribute the dataset 
    df = spark.read.csv("//ml-ratings.csv", inferSchema=True, header=True).cache()
    broadcast_data = sc.broadcast(df)


# Step 2: Load and explore the dataset
df.show(10)

# Show data summary
df.describe().show()

# Step 3: Split the data into training and test sets
(train, test) = df.randomSplit([0.8, 0.2])

# Step 4: Set up ALS (Alternating Least Squares) model
als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop", nonnegative=True)

# Step 5: Define hyperparameters for tuning
paramGrid = ParamGridBuilder() \
    .addGrid(als.rank, [12, 14]) \
    .addGrid(als.maxIter, [18, 20]) \
    .addGrid(als.regParam, [.17, .19]) \
    .build()

# Step 6: Set up RMSE evaluator
evaluator = RegressionEvaluator(
    metricName="rmse", labelCol="rating", predictionCol="prediction")

# Step 7: Set up Cross-validation
tvs = TrainValidationSplit(
    estimator=als,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator)

# Step 8: Train the ALS model with training data
model = tvs.fit(train)
bestModel = model.bestModel

# Step 9: Make predictions on the test set and evaluate the model
predictions = bestModel.transform(test)
rmse = evaluator.evaluate(predictions)

print(f"RMSE : {rmse}")
print(f"Best rank : {bestModel.rank}")
print(f"Best maxIter : {bestModel._java_obj.parent().getMaxIter()}")
print(f"Best regParam : {bestModel._java_obj.parent().getRegParam()}")

# Step 10: Show the top 5 predictions
predictions.sort("userId", "rating").show(5)

# Step 11: Get recommendations for all users
recs = bestModel.recommendForAllUsers(10)

# Step 12: Define UDF for mapping recommendations
zip_ = udf(
    lambda x, y: list(zip(x, y)),
    ArrayType(StructType([
        StructField("movieId", IntegerType()),
        StructField("rating", DoubleType())
    ]))
)

# Step 13: Function to get recommendations for a specific user
def get_recs_for_user(recs, userId):
    recs = recs.filter(recs["userId"] == userId)
    recs = recs.select("userId", "recommendations.movieId", "recommendations.rating") \
        .withColumn("tmp", explode(zip_("movieId", "rating"))) \
        .select("userId", "tmp.movieId", "tmp.rating")
    return recs

# Step 14: Show recommendations for user with ID 11
recs = get_recs_for_user(recs, "11")
recs.show()

# Step 15: Stop the Spark session
spark.stop()
