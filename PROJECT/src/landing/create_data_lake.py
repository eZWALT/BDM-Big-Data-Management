from pyspark.sql import SparkSession
from delta import DeltaTable
from delta import *
import os
from datetime import datetime
from typing import List, Optional
from loguru import logger 

from src.utils.task import Task, TaskStatus
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
    def __init__(self,
                 temporal_path: str = "data_lake/landing/temporal",
                 persistent_path: str = "data_lake/landing/persistent",
                 spark_master_url: str = "spark://localhost:7077",
                 ) -> None:
        super().__init__()
        # Variable to track supported delta comptaible formats and allowed BLOB types to be stored rawly 
        self.SUPPORTED_FORMATS = ["json", "jsonl", "csv", "parquet"]
        self.SUPPORTED_BLOB_EXTENSIONS = [".jpg", ".jpeg", ".png", ".gif" ".mp4", ".webm", ".mp3", ".wav", ".pdf"]
        # Common Paths
        self.temporal_path = temporal_path
        self.persistent_path = persistent_path 
        self.persistent_delta = os.path.join(persistent_path,"delta-table")     
        self.blob_output_path = os.path.join(persistent_path, "blobs")
   
        # Spark 
        self.spark = self._create_spark_session(spark_master_url=spark_master_url)
        
    def setup(self):
        pass
        
        
   # Create a Spark session with Delta Lake support.
    def _create_spark_session(self, spark_master_url: str = "spark://localhost:7077") -> SparkSession:
        return SparkSession.builder \
            .appName("CreateLandingZoneDataLake") \
            .master(spark_master_url) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
            .getOrCreate()        
                
    def delete_from_delta_table(self):
        """
        Deletes data from a Delta Lake table.

        Parameters:
        - spark: SparkSession object.
        - delta_table_path: The path to the Delta Lake table.
        """
        # Ensure the table exists
        if DeltaTable.isDeltaTable(self.spark, self.persistent_delta):
            deltaTable = DeltaTable.forPath(self.spark, self.persistent_delta)
            deltaTable.delete()
        else:
            logger.error("The specified path does not contain a Delta table.")

    
    # def _load_supported_files_to_delta_format(self):
    #         for subdir, _, files in os.walk(self.temporal_path):
    #             for file in files:
    #                 ext = os.path.splitext(file)[1].lower()
    #                 file_path = os.path.join(subdir, file)

    #                 # Handle structured formats (tabular)
    #                 if ext in [f".{fmt}" for fmt in self.SUPPORTED_FORMATS]:
    #                     format = ext.lstrip(".")
    #                     try:
    #                         df = self.spark.read.format(format).load(file_path)
    #                         df.write.format("delta").mode("append").save(self.persistent_delta)
    #                         logger.success(f"[✓] Ingested {file} as {format}")
    #                     except Exception as e:
    #                         logger.error(f"[!] Failed to process {file}: {e}")

    #                 # Handle blob files (non-tabular)
    #                 elif ext in self.SUPPORTED_BLOB_EXTENSIONS:
    #                     try:
    #                         os.makedirs(self.blob_output_path, exist_ok=True)
    #                         dst = os.path.join(self.blob_output_path, file)
    #                         if not os.path.exists(dst):  # Avoid overwriting
    #                             with open(file_path, "rb") as src_file, open(dst, "wb") as dest_file:
    #                                 dest_file.write(src_file.read())
    #                             logger.success(f"[✓] Copied blob file: {file}")
    #                         else:
    #                             logger.warning(f"[!] Blob file already exists, skipped: {file}")
    #                     except Exception as e:
    #                         logger.error(f"[!] Failed to copy blob file {file}: {e}")
    
    def _load_supported_files_to_delta_format(self):
        # Print the temporal path to check it's being passed correctly
        print(f"Temporal Path: {self.temporal_path}")

        # Walk through all files in the temporal path
        for subdir, _, files in os.walk(self.temporal_path):
            print(f"Checking directory: {subdir}")
            
            for file in files:
                print(f"Found file: {file}")
                ext = os.path.splitext(file)[1].lower()  # Extract file extension
                file_path = os.path.join(subdir, file)  # Full file path
                print(f"File path: {file_path}")
                print(f"File extension: {ext}")

                # Handle structured formats (tabular)
                if ext in [f".{fmt}" for fmt in self.SUPPORTED_FORMATS]:
                    format = ext.lstrip(".")
                    print(f"Handling structured file as format: {format}")
                    try:
                        print(f"Attempting to load file: {file_path}")
                        df = self.spark.read.format(format).load(file_path)
                        print(f"File loaded successfully: {file_path}")
                        df.write.format("delta").mode("append").save(self.persistent_delta)
                        print(f"File {file} ingested as {format} into Delta Lake")
                        logger.success(f"[✓] Ingested {file} as {format}")
                    except Exception as e:
                        logger.error(f"[!] Failed to process {file}: {e}")
                        print(f"[!] Failed to process {file}: {e}")

                # Handle blob files (non-tabular)
                elif ext in self.SUPPORTED_BLOB_EXTENSIONS:
                    print(f"Handling blob file: {file}")
                    try:
                        os.makedirs(self.blob_output_path, exist_ok=True)
                        dst = os.path.join(self.blob_output_path, file)
                        print(f"Destination path for blob: {dst}")
                        if not os.path.exists(dst):  # Avoid overwriting
                            with open(file_path, "rb") as src_file, open(dst, "wb") as dest_file:
                                dest_file.write(src_file.read())
                            logger.success(f"[✓] Copied blob file: {file}")
                            print(f"Blob file copied successfully: {file}")
                        else:
                            logger.warning(f"[!] Blob file already exists, skipped: {file}")
                            print(f"Blob file already exists, skipped: {file}")
                    except Exception as e:
                        logger.error(f"[!] Failed to copy blob file {file}: {e}")
                        print(f"[!] Failed to copy blob file {file}: {e}")
                                

    # Entrypoint for the landing zone task.
    # Loads compatible structured files into Delta format,
    # and copies blob files into the persistent zone.
    def execute(self):
        self.status = TaskStatus.IN_PROGRESS
        self._load_supported_files_to_delta_format()
        self.status = TaskStatus.COMPLETED
        try:
            df = self.spark.read.format("delta").load(self.persistent_delta)
            logger.info(f"Preview of the Delta Lake table:\n{df.show(truncate=False)}")
        except Exception as e:
            logger.error(f"[!] Could not preview Delta table: {e}")
            
# Main to test the task locally 
if __name__ == "__main__":
    print("[*] Running CreateDataLakeTask test...")
    
    task = CreateDataLakeTask(
        temporal_path="data_lake/landing/temporal",
        persistent_path="data_lake/landing/persistent",
        #spark_master_url="local[*]"
    )
    task.execute()

    print("[✓] Task executed successfully.")