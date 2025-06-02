import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Type

from airflow.models import BaseOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow import DAG
from src.data_normalizer import NormalizerConfig, get_normalizer_config, normalizer_script
from src.consumers import ConsumerConfig, get_consumer_configs, get_consumer_script

if __name__ == "__main__":
    import os
    import sys

    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    sys.path.append(root)
    os.chdir(root)


from src.data_cleaners import get_data_cleaner_configs, get_data_cleaner_script
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
    try:
        task.execute()
    except Exception as e:
        # We don't care if this fails, we want the rest of the DAG to run
        print(f"Batch produce task failed for query {query} on {social_network}: {e}")
        pass


def create_batch_product_tracking_dag(dag_id: str, company: Company, product: Product):
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

    normalizer_config = get_normalizer_config()
    sentiment_config = get_consumer_configs(task_name="sentiment")

    global_context = {
        "product_id": product.name,
        "company_id": company.company_id,
    }

    with dag:
        normalizer_sources: List[Tuple[str, str, List[BaseOperator]]] = []
        # (social_network, source_topic, tasks)
        for query in product.keywords:
            hashed_query = hash_query(query)
            query_context = global_context.copy()
            query_context["query"] = query
            query_context["query_hash"] = hashed_query

            for social_network in tier_config.social_networks:
                social_network_context = query_context.copy()
                social_network_context["social_network"] = social_network

                ingestion_task = PythonOperator(
                    task_id=f"ingest-{social_network}-{hashed_query}",
                    python_callable=batch_produce_task,
                    op_kwargs={
                        "query": query,
                        "social_network": social_network,
                        "hours_since_last_execution": hours_since_last_execution,
                    },
                )
                data_loaders = get_data_loader_configs(social_network)
                data_cleaners = get_data_cleaner_configs(social_network)

                if not set(data_loaders.keys()) == set(data_cleaners.keys()):
                    raise ValueError(
                        f"Data loaders and data cleaners for {social_network} do not match. "
                        f"Data loaders: {data_loaders.keys()}, Data cleaners: {data_cleaners.keys()}"
                    )

                cleaner_tasks = []  # To later set the dependencies with the normalizer
                for loader_cleaner_id in data_loaders.keys():
                    # Create a context for the data loader and cleaner
                    loader_cleaner_context = social_network_context.copy()
                    loader_cleaner_context["loader_cleaner_id"] = loader_cleaner_id

                    data_loader_config = data_loaders[loader_cleaner_id]
                    data_cleaner_config = data_cleaners[loader_cleaner_id]

                    app_args: List[str] = []
                    for arg_key, arg_value in data_loader_config["application_args"].items():
                        # Replace placeholders in the argument value
                        app_args.append(f"--{arg_key}")
                        app_args.append(replace_placeholders(arg_value, **loader_cleaner_context))
                    loader_task = SparkSubmitOperator(
                        task_id=f"load-{social_network}-{hashed_query}-{loader_cleaner_id}",
                        application=get_data_loader_script(data_loader_config["loader_type"]),
                        conn_id="spark_default",  
                        name=f"load-{social_network}-{hashed_query}-{loader_cleaner_id}",
                        verbose=True,
                        application_args=app_args,
                        conf=replace_placeholders(data_loader_config.get("conf", {}), **loader_cleaner_context),
                        py_files=replace_placeholders(data_loader_config.get("py_files", []), **loader_cleaner_context),
                        env_vars=replace_placeholders(data_loader_config.get("env_vars", {}), **loader_cleaner_context),
                    )
                    ingestion_task >> loader_task

                    # Add data cleaner task
                    app_args: List[str] = []
                    for arg_key, arg_value in data_cleaner_config["application_args"].items():
                        # Replace placeholders in the argument value
                        app_args.append(f"--{arg_key}")
                        app_args.append(replace_placeholders(arg_value, **loader_cleaner_context))
                    cleaner_task = SparkSubmitOperator(
                        task_id=f"clean-{social_network}-{hashed_query}-{loader_cleaner_id}",
                        application=get_data_cleaner_script(data_cleaner_config["cleaner_type"]),
                        conn_id="spark_default",  
                        name=f"clean-{social_network}-{hashed_query}-{loader_cleaner_id}",
                        verbose=True,
                        application_args=app_args,
                        conf=replace_placeholders(data_cleaner_config.get("conf", {}), **loader_cleaner_context),
                        py_files=replace_placeholders(
                            data_cleaner_config.get("py_files", []), **loader_cleaner_context
                        ),
                        env_vars=replace_placeholders(
                            data_cleaner_config.get("env_vars", {}), **loader_cleaner_context
                        ),
                    )
                    loader_task >> cleaner_task

                    cleaner_tasks.append(cleaner_task)

                # Add the normalizer source to the list
                normalizer_sources.append(
                    (
                        social_network,
                        replace_placeholders(normalizer_config["topics"][social_network], **social_network_context),
                        cleaner_tasks,
                    )
                )

        app_args = [
            "--output",
            replace_placeholders(normalizer_config["output_dir"], **global_context),
            "--input",
            *(f"{social_network}:{source_topic}" for social_network, source_topic, _ in normalizer_sources),
        ]

        for arg_key, arg_value in normalizer_config.get("application_args", {}).items():
            # Replace placeholders in the argument value
            app_args.append(f"--{arg_key}")
            app_args.append(replace_placeholders(arg_value, **global_context))

        # Create the normalizer task
        normalizer_task = SparkSubmitOperator(
            task_id=f"normalize-{product.name}",
            application=normalizer_script,
            conn_id="spark_default",  
            name=f"normalize-{product.name}",
            verbose=True,
            application_args=app_args,
            conf=replace_placeholders(normalizer_config.get("conf", {}), **global_context),
            py_files=replace_placeholders(normalizer_config.get("py_files", []), **global_context),
            env_vars=replace_placeholders(normalizer_config.get("env_vars", {}), **global_context),
        )
        # Set dependencies for normalizer task
        for _, _, tasks in normalizer_sources:
            for task in tasks:
                task >> normalizer_task
                
        
        # TODO: Update this fixed rigid logic for various consumption tasks 
        # rather than only sentiment analysis as it is right now
        
        for arg_key, arg_value in sentiment_config.get("application_args", {}).items():
            # Replace placeholders in the argument value
            
            # CHECK THIS ARGUMENTS KEYS AND VALUES! (application_args)

            app_args.append(f"--{arg_key}")
            app_args.append(replace_placeholders(arg_value, **global_context))
            
        # Create the sentiment consumer task
        sentiment_task = SparkSubmitOperator(
            task_id=f"consumer-sentiment-{product.name}",
            application=get_consumer_script(consumer_type="sentiment"),
            conn_id="spark_default",  
            name=f"consumer-sentiment-{product.name}",
            verbose=True,
            application_args=app_args,
            conf=replace_placeholders(normalizer_config.get("conf", {}), **global_context),
            py_files=replace_placeholders(normalizer_config.get("py_files", []), **global_context),
            env_vars=replace_placeholders(normalizer_config.get("env_vars", {}), **global_context),
        )
        normalizer_task >> sentiment_task



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
            dag = create_batch_product_tracking_dag(dag_id, company, product)
            # Enable the dag
            globals()[dag_id] = dag
            logging.info(f"DAG created for {company.company_id} - {product.name} with dag_id: {dag_id}")


# Trigger the DAG generation (Relative path inside the docker container)
generate_dynamic_dags_from_serialized_companies("configuration/companies.json")


