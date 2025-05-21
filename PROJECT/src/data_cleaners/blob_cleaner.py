"""
Application to load Binary Large Objects (BLOBs) from a folder into the
Landing Zone. They won't be cleaned or transformed, just moved from the input
folder to the output folder.

Args:
    input_path (str): Path to the folder containing the BLOBs.
    output_path (str): Path to the output folder where the BLOBs will be stored.
"""

import os

from minio import Minio
from minio.commonconfig import CopySource


def main(
    input_path: str, output_path: str, minio_host: str, minio_port: str, minio_access_key: str, minio_secret_key: str
):
    """
    Because we are only moving files from one folder to another, we don't need to
    use Spark. We can just use the MinIO client to copy the files from one
    location to another.

    These movements all happen in the server side, which will be much faster
    than downloading the files to the client and uploading them again. For that
    reason, we don't need to use Spark for this operation.
    """
    minio = Minio(
        f"{minio_host}:{minio_port}",
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False,
    )
    src_bucket, src_folder = input_path.split("/", 1)
    dst_bucket, dst_folder = output_path.split("/", 1)

    print(f"Copying data from bucket={src_bucket},folder={src_folder} to bucket={dst_bucket},folder={dst_folder}")
    src_files = minio.list_objects(src_bucket, recursive=True, prefix=src_folder)
    delete_files = []
    for file in src_files:
        src_basename = os.path.basename(file.object_name)
        dst_file = os.path.join(dst_folder, src_basename)
        print(f"Server-copying file {src_bucket}/{file.object_name} to {dst_bucket}/{dst_file}")
        minio.copy_object(dst_bucket, dst_file, CopySource(src_bucket, file.object_name))
        delete_files.append(file.object_name)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input folder path")
    parser.add_argument("--output", required=False, help="Output folder path")
    parser.add_argument("--minio_host", required=True, help="MinIO host")
    parser.add_argument("--minio_port", required=True, help="MinIO port")
    parser.add_argument("--minio_access_key", required=True, help="MinIO access key")
    parser.add_argument("--minio_secret_key", required=True, help="MinIO secret key")

    args = parser.parse_args()
    main(args.input, args.output, args.minio_host, args.minio_port, args.minio_access_key, args.minio_secret_key)
