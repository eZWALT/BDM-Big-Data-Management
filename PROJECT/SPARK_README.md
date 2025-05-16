## Setting up MinIO

To set up MinIO, navigate to `localhost:9001` in your browser. This is the MinIO web interface, where you can manage
your buckets and objects. There, create the following buckets:
- `buffer`
- `landing`
- `trusted`
- `exploitation`
Then, create a new access and secret key pair by navigating th the access keys tab on the left side of the screen.
Make sure to copy the access and secret keys, as you will need them later.

## Interacting with MinIO + Delta Lake from you local machine

For use with delta:
```bash
pyspark \
--packages io.delta:delta-spark_2.12:3.2.0 \
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

For use with MinIO:

Set the following environment variables in your terminal:
```bash
export MINIO_HOST=localhost
export MINIO_PORT=9000
export AWS_SECRET_KEY=<your_minio_secret_key>
export AWS_ACCESS_KEY=<your_minio_access_key>
```

Then, run the following command to start `pyspark` with the necessary configurations:

```bash
pyspark \
--packages "org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-spark_2.12:3.2.0" \
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
--conf spark.executorEnv.AWS_ACCESS_KEY=$AWS_ACCESS_KEY \
--conf spark.executorEnv.AWS_SECRET_KEY=$AWS_SECRET_KEY \
--conf spark.hadoop.fs.s3a.endpoint=http://$MINIO_HOST:$MINIO_PORT \
--conf spark.hadoop.fs.s3a.path.style.access=true
```

Note: This cannot be executed in a spark cluster directly (by setting the `--master` key), because if you do so, your
computer (the host) becomes the driver, and connects to the workers running inside the docker compose network. Thus,
for you, the minio server is `localhost:9000`, but for the executors, it is `minio:9000`. This means that the executors
will not be able to connect to the minio server, because they are running in a different network. To solve this, you
need to run the spark job in the cluster, and not on your local machine.

This, however, cannot be done with a simple `spark-submit` command, because the `pyspark` doesn't seem to support that.
Instead, we've set up a jupyter notebook server with `pyspark` installed, from which you can run all the code from 
inside the docker compose network.

## Interacting with MinIO + Delta Lake from a Jupyter Notebook

First, make sure to copy the token of the notebook from the docker logs (one of the lines at the end of the startup 
logs). Then, open the notebook in your browser, and paste the token.

Open a new notebook with the Python kernel, and run the following code:
```python
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.executorEnv.AWS_ACCESS_KEY", os.environ["AWS_ACCESS_KEY"]) \
    .config("spark.executorEnv.AWS_SECRET_KEY", os.environ["AWS_SECRET_KEY"]) \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .master("spark://spark-master:7077") \
    .getOrCreate()
```

> [!NOTE]
> Make sure to set the environment variables `MINIO_HOST`, `MINIO_PORT`, `AWS_ACCESS_KEY`, and `AWS_SECRET_KEY` in the notebook
> before running the code above. You can do this by running the following code in a cell:
> ```python
> %env MINIO_HOST=minio
> %env MINIO_PORT=9000
> %env MINIO_ACCESS_KEY=<your_minio_access_key>
> %env MINIOSECRET_KEY=<your_minio_secret_key>
> ```


Test that everything is working by running the following code in a cell:
```python
import random

data = [(random.randint(1, 100), random.randint(1, 100)) for _ in range(100)]
df = spark.createDataFrame(data, ["col1", "col2"])
df.write.format("delta").mode("overwrite").save("s3a://my-bucket/my-delta-table")
```