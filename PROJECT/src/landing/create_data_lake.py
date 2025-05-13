import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional

from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.utils.task import Task, TaskStatus
from utils.environ import modified_environ

from .readers import get_row_reader

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
# Author: Walter J.T.V, Mateja Z                                               #
# ===-----------------------------------------------------------------------===#


class CreateDataLakeTask(Task):
    """
    Read the contents of the LOCAL source_file, and writes them into a Delta Lake
    table located at target_table in the SPARK CLUSTER.
    """

    def __init__(self, source_file: str, target_table: str, spark_master_url: str = "spark://localhost:7077") -> None:
        super().__init__()
        self.source_file = source_file
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
            PYSPARK_DRIVER_PYTHON="/usr/bin/python3.11",
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

        try:
            reader = get_row_reader(self.source_file)
        except ValueError as e:
            logger.debug("File format is not tabular, considering it binary data...")
            df = self.spark.read.format("binaryFile").load(self.source_file)
            df.write.format("delta").mode("append").save(self.target_table)
        else:
            df = self.spark.createDataFrame(list(reader))
            df.write.format("delta").mode("append").save(self.target_table)
            logger.success(f"Data written to Delta table at {self.target_table}")
            return


# Main to test the task locally
if __name__ == "__main__":
    print("[*] Running CreateDataLakeTask test...")

    # Reference the paths inside the spark executors to produce any result
    task = CreateDataLakeTask(
        source_file="/tmp/data_lake/temporal",
        target_table="/tmp/data_lake/persistent",
        # spark_master_url="local[*]"
    )
    task.execute()

    print("[✓] Task executed successfully.")
