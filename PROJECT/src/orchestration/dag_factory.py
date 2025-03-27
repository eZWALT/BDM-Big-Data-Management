from typing import List
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task
from datetime import datetime, timedelta, date, timezone
import psycopg2

from airflow.models import DagModel

from src.utils.company import Company

# ===----------------------------------------------------------------------===#
# Factory DAG                                                                 #
#                                                                             #
# Factory dags enable the ease creation of new use cases (Product - Company)  #
# re-using the parametrized factory infrastructure. For custom DAG's they must#
# be generated from the ground up                                             #
#                                                                             #
# Author: Walter J.T.V                                                        #
# ===----------------------------------------------------------------------===#


def initialize_dag_from_company(company: Company, prod_idx: int) -> DAG:
    product = company.get_product(prod_idx)
    airflow_args = product.usecase_airflow_args

    # Fetch airflow arguments for the DAG (Provide default values for the most relevant)
    schedule_interval = airflow_args.get("schedule_interval", "@daily")
    start_date_str = airflow_args.get("start_date", None)
    if start_date_str:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d",).replace(tzinfo=timezone.utc)  
    else:
        start_date = datetime.now(timezone.utc)  
    catchup_str = airflow_args.get("catchup", False)
    catchup = catchup_str.lower() == "true"
    
    dynamic_args = {
        key: value
        for key, value in airflow_args.items()
        if key not in ["schedule_interval", "start_date", "catchup"]
    }

    # Create the DAG
    dag = DAG(
        dag_id=company.generate_usecase_dag_id(prod_idx),
        description=f"Tracking Vibe for {product.name} of {company.company_id}",
        schedule_interval=schedule_interval,
        start_date=start_date,
        catchup=catchup,
        #**dynamic_args, # TODO: use dynamic_args (Airflow docs are trash)
    )
    return dag


# TODO: End this dag
# Factory method to create a parameterized processing DAG for a product.
def create_batch_product_tracking_dag(
    company: Company,
    prod_idx: int,
) -> DAG:

    dag = initialize_dag_from_company(company, prod_idx)

    with dag:
        # Common abstract tasks for batch based workflows

        ingestion_task = PythonOperator(
            task_id=f"batch_ingestion",
            python_callable=None,  # TODO: Define this entrypoint function
            provide_context=True,
            dag=dag,
        )

        landingzone_store_task = PythonOperator(
            task_id=f"landingzone_store",
            python_callable=None,  # TODO: Define this entrypoint function
            provide_context=True,
            dag=dag,
        )

        # Define task dependencies
        ingestion_task >> landingzone_store_task

    return dag

# TODO: End this dag
# Factory method to create a parameterized processing DAG for a product.
def create_dummy_test_dag(
    company: Company,
    prod_idx: int,
) -> DAG:

    product = company.get_product(prod_idx)
    dag = initialize_dag_from_company(company, prod_idx)

    with dag:

        dumb_task1 = EmptyOperator(task_id=f"ingestion_{company.company_id}", dag=dag)
        dumb_task2 = EmptyOperator(task_id=f"landingzone_{company.company_id}", dag=dag)
        dumb_task3 = EmptyOperator(task_id=f"trustedzone_{company.company_id}", dag=dag)
        dumb_task4 = EmptyOperator(
            task_id=f"exploitationzone_{company.company_id}", dag=dag
        )
    dumb_task1 >> dumb_task2 >> dumb_task3 >> dumb_task4

    return dag
