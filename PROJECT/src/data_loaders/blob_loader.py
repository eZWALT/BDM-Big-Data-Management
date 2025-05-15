"""
Spark application to load Binary Large Objects (BLOBs) from a folder into the
Landing Zone. They won't be cleaned or transformed, just moved from the input
folder to the output folder.

Args:
    input_path (str): Path to the folder containing the BLOBs.
    output_path (str): Path to the output folder where the BLOBs will be stored.
"""

from py4j.java_gateway import java_import
from pyspark.sql import SparkSession


def main(input_path: str, output_path: str):
    spark = SparkSession.builder.appName("BLOBDataLoader").getOrCreate()
    # We don't need any special configuration for BLOBs, so we just create a
    # regular Spark session.
    sc = spark.sparkContext

    # Import Hadoop FileSystem classes
    java_import(sc._jvm, "org.apache.hadoop.fs.Path")
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())

    src_path = sc._jvm.Path(input_path)
    dst_path = sc._jvm.Path(output_path)

    # List files in the source directory
    files = fs.listStatus(src_path)

    for file_status in files:
        file_path = file_status.getPath()
        file_name = file_path.getName()
        dst_file_path = sc._jvm.Path(dst_path, file_name)

        # Move file (rename in HDFS parlance)
        success = fs.rename(file_path, dst_file_path)
        if success:
            print(f"Moved: {file_path} -> {dst_file_path}")
        else:
            print(f"Failed to move: {file_path}")

    spark.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input folder path")
    parser.add_argument("--output", required=False, help="Output folder path")

    args = parser.parse_args()
    main(args.input, args.output)
