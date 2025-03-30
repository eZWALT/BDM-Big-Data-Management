from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import DagModel

def say_hello():
    print("We are thrilled to announce that we are using Airflow! 🚀")

def say_goodbye():
    print("I'm eager to be promoted to customer (fired), goodbye Airflow! 👋")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id='silly_dag',
    default_args=default_args,
    schedule='@hourly',
    catchup=True,
) as dag:

    hello_task = PythonOperator(
        task_id='thrilled_to_announce',
        python_callable=say_hello,
    )

    goodbye_task = PythonOperator(
        task_id='promoted_to_customer',
        python_callable=say_goodbye,
    )
    hello_task >> goodbye_task  



