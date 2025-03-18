from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2


default_args = {
    "owner": "Messi",
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    
) as abstract_batch_dag:
    
    ingestion_task = PythonOperator(
        task_id="api_batch_ingestion",
        python_callable=None, # Todo change this
    )
    
    store_task = PythonOperator(
        task_id="store_batch_ingestion",
        python_callable=None,
    )
    
    ingestion_task >> store_task