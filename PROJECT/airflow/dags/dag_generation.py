import json
from typing import List
from datetime import datetime, timedelta, date, timezone
import logging
from airflow import DAG
from dataclasses import dataclass
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task
from src.utils.company import deserialize_companies_from_json, Company, Product

# ===----------------------------------------------------------------------===#
# DAG Configuration Dataclass                                                 #
# ===----------------------------------------------------------------------===#

@dataclass
class DAGConfig:
    dag_id: str
    schedule_interval: str
    start_date: datetime
    catchup: bool
    extra_args: dict
    company: Company
    product: Product 

# ===----------------------------------------------------------------------===#
# Automatic USE-CASE DAG Generation                                           #
#                                                                             #
# This script automatically parses the list of companies from a JSON file     #
# and navigates through all these companies' products (use-cases) to          #
# automatically generate all batch jobs with their specified configuration.   #            
# ===----------------------------------------------------------------------===#

def get_airflow_args(company, product):
    """Extracts all Airflow DAG arguments from a product configuration."""
    airflow_args = product.usecase_airflow_args.copy()
    schedule_interval = airflow_args.pop("schedule_interval", "@daily")
    start_date_str = airflow_args.pop("start_date", None)
    
    if start_date_str:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        start_date = datetime.now(timezone.utc)
    
    catchup = airflow_args.pop("catchup", "False").lower() == "true"
    
    return schedule_interval, start_date, catchup, airflow_args

# ===----------------------------------------------------------------------===#
# DAG Factory Methods                                                         #
# ===----------------------------------------------------------------------===#

def create_dummy_test_dag(config: DAGConfig):
    """Generates a simple dummy DAG for testing purposes."""
    dag = DAG(
        dag_id=f"dummy_{config.dag_id}",
        schedule=config.schedule_interval,
        start_date=config.start_date,
        catchup=config.catchup,
        #**config.extra_args
    )

    with dag:
        dumb_task1 = EmptyOperator(task_id=f"ingestion_{config.product.name}")
        dumb_task2 = EmptyOperator(task_id=f"landingzone_{config.product.name}")
        dumb_task3 = EmptyOperator(task_id=f"trustedzone_{config.product.name}")
        dumb_task4 = EmptyOperator(task_id=f"exploitationzone_{config.product.name}")
        
        dumb_task1 >> dumb_task2 >> dumb_task3 >> dumb_task4
    
    return dag

# BIG TODO: Fix extra arguments and end this dag
def create_batch_product_tracking_dag(config: DAGConfig):
    """Creates a batch tracking DAG dynamically."""
    dag = DAG(
        dag_id=config.dag_id,
        schedule=config.schedule_interval,
        start_date=config.start_date,
        catchup=config.catchup,
        #**config.extra_args
    )
    
    with dag:
        ingestion_task = PythonOperator(
            task_id="batch_ingestion",
            python_callable=lambda: logging.info(f"Ingesting data for {config.company.company_id} - {config.product.name}"),
        )

        landingzone_store_task = PythonOperator(
            task_id="landingzone_store",
            python_callable=lambda: logging.info(f"Storing data in landing zone for {config.company.company_id} - {config.product.name}"),
        )
        
        ingestion_task >> landingzone_store_task
    
    return dag
# ===----------------------------------------------------------------------===#
# DAG Generation from JSON                                                    #
# ===----------------------------------------------------------------------===#

def generate_dynamic_dags_from_serialized_companies(data_path: str, is_test: bool = True):
    companies = deserialize_companies_from_json(data_path)
    for company in companies:
        for i, product in enumerate(company.products):
            dag_id = f"dag_{company.generate_usecase_dag_id(i)}"
            schedule_interval, start_date, catchup, extra_args = get_airflow_args(company, product)
            
            config = DAGConfig(
                dag_id=dag_id,
                schedule_interval=schedule_interval,
                start_date=start_date,
                catchup=catchup,
                extra_args=extra_args,
                company=company,
                product=product
            )
            # Create dag and store it in global symbol table 
            dag = create_dummy_test_dag(config) if is_test else create_batch_product_tracking_dag(config)
            globals()[dag_id] = dag
            logging.info(f"DAG created for {company.company_id} - {product.name} with dag_id: {dag_id}")

# Trigger the DAG generation (Relative path inside the docker container)
generate_dynamic_dags_from_serialized_companies("dags/companies.json", is_test=True)