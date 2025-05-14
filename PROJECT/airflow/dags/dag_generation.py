import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

if __name__ == "__main__":
    import os
    import sys

    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    sys.path.append(root)
    os.chdir(root)

from airflow.operators.python import PythonOperator

from airflow import DAG
from src.ingestion.batch import (
    BatchProduceTask,
    ProducerConfig,
    discover_db_connections,
    hash_query,
    load_producer_config,
)
from src.landing import LoadToLandingTask
from src.utils.company import Company, Product, deserialize_companies_from_json
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


def batch_produce_task(query: List[str], producer_config: ProducerConfig, hours_since_last_execution: int) -> None:
    task = BatchProduceTask(
        producer_config=producer_config,
        query=query,
        utc_since=datetime.now(tz=timezone.utc) - timedelta(hours=hours_since_last_execution),
        utc_until=datetime.now(tz=timezone.utc),
    )
    task.setup()
    task.execute()


def load_to_landing_zone_task(
    source_folder: str,
    target_table: str,
) -> None:
    task = LoadToLandingTask(source_folder=source_folder, target_table=target_table)
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
            for producer in tier_config.batch_producers:
                producer_config = load_producer_config(producer)
                ingestion_task = PythonOperator(
                    task_id=f"ingest-{producer}-{hash_query(query)}",
                    python_callable=batch_produce_task,
                    op_kwargs={
                        "query": query,
                        "producer_config": producer_config,
                        "hours_since_last_execution": hours_since_last_execution,
                    },
                )
                _, dbs = discover_db_connections(producer_config.get("kwargs", {}), query_hash=hash_query(query))
                for db in dbs:
                    load_task = PythonOperator(
                        task_id=f"load-{db.topic}-{db.table}",
                        python_callable=load_to_landing_zone_task,
                        op_kwargs={
                            "source_folder": db.folder_path,
                            "target_table": os.path.join("dags/data_lake/landing", db.topic, db.table),
                        },
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
