from loguru import logger
from pyspark.sql import SparkSession

if __name__ == "__main__":
    import os
    import sys

    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    sys.path.append(root)
    os.chdir(root)

from src.landing.readers import EmptyError, get_reader
from src.utils.environ import modified_environ
from src.utils.task import Task

# ===-----------------------------------------------------------------------===#
# Landing Zone Task                                                            #
#                                                                              #
# This task reads structured files (e.g. JSON, CSV, Parquet) from a temporal   #
# landing zone and writes them into Delta Lake format in a persistent zone.    #
#                                                                              #
# Delta Lake is an open-source storage layer that brings ACID transactions,    #
# schema enforcement, and time-travel (versioning) to big data workloads. It   #
# allows querying data efficiently while maintaining consistency.              #
#                                                                              #
# Additionally, this task detects and stores unstructured files (e.g. images,  #
# audio, video) in the same persistent zone alongside the Delta table.         #
#                                                                              #
# Author: Walter J.T.V, Mateja Z, Marc Parcerisa                               #
# ===-----------------------------------------------------------------------===#


class LoadToLandingTask(Task):
    """
    Read the contents of the LOCAL source_file, and writes them into a Delta Lake
    table located at target_table in the SPARK CLUSTER.
    """

    def __init__(self, source_folder: str, target_table: str, spark_master_url: str = "local[*]") -> None:
        super().__init__()
        self.source_folder = source_folder
        self.target_table = target_table
        self.spark_master_url = spark_master_url

    def setup(self):
        self.spark = self._create_spark_session("CreateDataLakeTask")

    def _create_spark_session(self, app_name: str) -> SparkSession:
        """
        Create a Spark session with Delta Lake support
        """
        with modified_environ(
            PYSPARK_SUBMIT_ARGS=(
                "--packages io.delta:delta-spark_2.12:3.1.0 "
                "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
                "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog pyspark-shell"
            ),
        ):
            spark = SparkSession.builder.appName(app_name).master(self.spark_master_url).getOrCreate()
            spark_version = spark.version
            if spark_version != "3.5.1":
                logger.warning(f"Spark version {spark_version} might not compatible with Delta Lake.")
            return spark

    def _spark_folder_exists(self, path: str) -> bool:
        hdfs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(self.spark._jsc.hadoopConfiguration())
        return hdfs.exists(self.spark._jvm.org.apache.hadoop.fs.Path(path))

    def _spark_folder_create(self, path: str) -> bool:
        """
        Create a folder in the Spark cluster
        """
        hdfs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(self.spark._jsc.hadoopConfiguration())
        return hdfs.mkdirs(self.spark._jvm.org.apache.hadoop.fs.Path(path))

    def _create_table_if_not_exists(self) -> bool:
        """
        Creates a new Delta table if it doesn't exist.
        Does nothing if the Delta table already exists.
        """
        if not self._spark_folder_exists(self.target_table):
            logger.info(f"Creating Delta table at {self.target_table}...")
            if self._spark_folder_create(self.target_table):
                logger.success(f"Delta table created at {self.target_table}")
            else:
                logger.error(f"Failed to create Delta table at {self.target_table}")

    def execute(self):
        self._create_table_if_not_exists()
        reader = get_reader(self.source_folder)
        try:
            data = list(reader)
        except EmptyError as e:
            logger.warning(f"Empty folder: {self.source_folder}. Skipping...")
            return
        df = self.spark.createDataFrame(data)
        df.write.format("delta").mode("append").save(self.target_table)
        logger.success(f"Data written to Delta table at {self.target_table}")


if __name__ == "__main__":
    print("[*] Running landing module test...")

    topics = [
        ("bluesky-803b3eb2", ("likes", "posts")),
        ("bluesky-dcaa8e5e", ("likes", "posts")),
        ("twitter-803b3eb2", ("posts",)),
        ("twitter-dcaa8e5e", ("posts",)),
        ("youtube-803b3eb2", ("video_metadata", "comments", "videos", "thumbnails", "audios")),
        ("youtube-dcaa8e5e", ("video_metadata", "comments", "videos", "thumbnails", "audios")),
    ]

    for topic, tables in topics:
        for table in tables:
            task = LoadToLandingTask(
                source_folder=f"buffer/{topic}/{table}",
                target_table=f"data_lake/landing/{topic}/{table}",
            )
            task.setup()
            task.execute()
            print(f"[✓] Task executed successfully for buffer/{topic}/{table} -> data_lake/landing/{topic}/{table}.")
