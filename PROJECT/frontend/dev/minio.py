import streamlit as st 
import os 
from minio import Minio
import pandas as pd 
import duckdb
import io 

@st.cache_resource
def get_minio_client():
    return Minio(
        endpoint=f"{os.getenv('MINIO_HOST', 'localhost')}:{os.getenv('MINIO_PORT', '9000')}",
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False
    )
    
def list_companies(minio_client, bucket: str):
    objects = list(minio_client.list_objects(bucket, prefix="", recursive=False))

    company_candidates = [
        obj.object_name.strip("/").split("/")[0]
        for obj in objects
        if obj.object_name.endswith("/") and obj.object_name.strip("/")
    ]
    return sorted(set(company_candidates))

def list_products(minio_client, bucket: str, company: str):
    prefix = f"{company}/"
    objects = list(minio_client.list_objects(bucket, prefix=prefix, recursive=False))

    product_candidates = [
        obj.object_name.strip("/").split("/")[1]
        for obj in objects
        if obj.object_name.endswith("/") and obj.object_name.count("/") >= 1
    ]
    return sorted(set(product_candidates))



def load_duckdb_from_minio(minio_client, bucket: str, company: str, product: str, table_name="sentiment"):
    object_path = f"{company}/{product}/sentiment.duckdb"  # Keep same path or adjust as needed
    try:
        response = minio_client.get_object(bucket, object_path)
        duckdb_bytes = io.BytesIO(response.read())

        tmp_path = "/tmp/sentiment.duckdb"
        with open(tmp_path, "wb") as f:
            f.write(duckdb_bytes.getbuffer())

        con = duckdb.connect(tmp_path)
        df = con.execute(f"SELECT * FROM {table_name}").df()
        con.close()
        return df
    except Exception as e:
        st.error(f"❌ Could not load {table_name} from {object_path}: {e}")
        return pd.DataFrame()
