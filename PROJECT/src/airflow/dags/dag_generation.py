import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Type

from airflow.models import BaseOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow import DAG

if __name__ == "__main__":
    import os
    import sys

    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    sys.path.append(root)
    os.chdir(root)


from src.data_loaders import LoaderConfig, get_data_loader_configs, get_data_loader_script
from src.ingestion.batch import BatchProduceTask, hash_query, load_producer_config
from src.utils.company import Company, Product, deserialize_companies_from_json
from src.utils.placeholders import replace_placeholders
from src.utils.tiers import get_tier_definition

# ===-----------------------------------------------------------------------===#
# Automatic USE-CASE DAG Generation                                            #
#                                                                              #
# This script automatically parses the list of companies from a JSON file      #
# and navigates through all these companies' products (use-cases) to           #
# automatically generate all batch jobs with their specified configuration.    #
#                                                                              #
# Author: Walter J.T.V, Marc Parcerisa                                         #
# ===-----------------------------------------------------------------------===#

# ===-----------------------------------------------------------------------===#
# DAG Configuration Dataclass                                                  #
# ===-----------------------------------------------------------------------===#


@dataclass
class DAGConfig:
    dag_id: str
    args: Dict[str, Any]
    company: Company
    product: Product


# ===-----------------------------------------------------------------------===#
# DAG Factory Methods                                                          #
# ===-----------------------------------------------------------------------===#


def batch_produce_task(query: List[str], social_network: str, hours_since_last_execution: int) -> None:
    producer_config = load_producer_config(social_network)
    task = BatchProduceTask(
        producer_config=producer_config,
        query=query,
        utc_since=datetime.now(tz=timezone.utc) - timedelta(hours=hours_since_last_execution),
        utc_until=datetime.now(tz=timezone.utc),
    )
    task.setup()
    task.execute()


def create_batch_product_tracking_dag(dag_id: str, product: Product):
    """Creates a batch tracking DAG dynamically."""
    tier_config = get_tier_definition(product.tracking_tier)

    dag = DAG(dag_id=dag_id, **tier_config.airflow_dag_args, start_date=datetime.now(tz=timezone.utc))
    # Compute the hours since the last execution based on the dag_args["schedule_interval"]
    schedule_interval = tier_config.airflow_dag_args["schedule"]
    if schedule_interval == "@hourly":
        hours_since_last_execution = 1
    elif schedule_interval == "@daily":
        hours_since_last_execution = 24
    elif schedule_interval == "@weekly":
        hours_since_last_execution = 24 * 7
    elif schedule_interval == "@monthly":
        hours_since_last_execution = 24 * 30
    else:
        raise ValueError(f"Unsupported schedule interval: {schedule_interval}")

    with dag:
        for query in product.keywords:
            hashed_query = hash_query(query)
            for social_network in tier_config.social_networks:
                ingestion_task = PythonOperator(
                    task_id=f"ingest-{social_network}-{hashed_query}",
                    python_callable=batch_produce_task,
                    op_kwargs={
                        "query": query,
                        "social_network": social_network,
                        "hours_since_last_execution": hours_since_last_execution,
                    },
                )
                for loader_id, data_loader_config in get_data_loader_configs(social_network).items():

                    app_args: List[str] = []
                    for arg_key, arg_value in data_loader_config["application_args"].items():
                        # Replace placeholders in the argument value
                        app_args.append(f"--{arg_key}")
                        app_args.append(replace_placeholders(arg_value, query_hash=hashed_query))
                    load_task = SparkSubmitOperator(
                        task_id=f"load-{social_network}-{hashed_query}-{loader_id}",
                        application=get_data_loader_script(data_loader_config["loader_type"]),
                        conn_id="spark_default",  # Define this connection in Airflow UI
                        verbose=True,
                        application_args=app_args,
                        conf=replace_placeholders(data_loader_config.get("conf", {}), query_hash=hashed_query),
                        py_files=replace_placeholders(data_loader_config.get("py_files", []), query_hash=hashed_query),
                        env_vars=replace_placeholders(data_loader_config.get("env_vars", {}), query_hash=hashed_query),
                    )
                    ingestion_task >> load_task

    return dag


# ===-----------------------------------------------------------------------===#
# DAG Generation from JSON                                                     #
# ===-----------------------------------------------------------------------===#


def generate_dynamic_dags_from_serialized_companies(data_path: str):
    companies = deserialize_companies_from_json(data_path)
    for company in companies:
        for i, product in enumerate(company.products):
            dag_id = f"dag_{company.generate_usecase_dag_id(i)}"
            # Create dag and store it in global symbol table
            dag = create_batch_product_tracking_dag(dag_id, product)
            # Enable the dag
            globals()[dag_id] = dag
            logging.info(f"DAG created for {company.company_id} - {product.name} with dag_id: {dag_id}")


# Trigger the DAG generation (Relative path inside the docker container)
generate_dynamic_dags_from_serialized_companies("configuration/companies.json")
