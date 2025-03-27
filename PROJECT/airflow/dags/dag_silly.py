from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import DagModel

def say_hello():
    print("Hello, Airflow! 🚀")

def say_goodbye():
    print("Goodbye, Airflow! 👋")

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
    schedule='@daily',
    catchup=True,
) as dag:

    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=say_hello,
    )

    goodbye_task = PythonOperator(
        task_id='say_goodbye',
        python_callable=say_goodbye,
    )
    hello_task >> goodbye_task  # Hello must happen before Goodbye!



