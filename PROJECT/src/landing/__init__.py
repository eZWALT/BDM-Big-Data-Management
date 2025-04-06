from pyspark.sql import SparkSession
from delta import *
import os
from datetime import datetime
from typing import List, Optional

from src.utils.task import Task, TaskStatus
# ===-----------------------------------------------------------------------===#
# Landing Zone Scripts Tasks                                                   #
#                                                                              #
#                                                                              #
#                                                                              #
#                                                                              #
#                                                                              #
#                                                                              #
# Author: Walter J.T.V                                                         #
# ===-----------------------------------------------------------------------===#


class LandingZoneTask(Task):
    def __init__(self, temporal_path: str = "data_lake/landing/temporal", persistent_path: str = "data_lake/landing/persistent"):
        self.temporal_path = temporal_path
        self.persistent_path = persistent_path 
        self.persistent_delta = persistent_path + "/delta-table"
        self.spark_session = self._create_spark_session()
        
        
   #Create a Spark session with Delta Lake support.
    def _create_spark_session(self) -> SparkSession:
    
        return SparkSession.builder \
            .appName("DeltaLakeApp") \
            .master("spark://localhost:7077") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()