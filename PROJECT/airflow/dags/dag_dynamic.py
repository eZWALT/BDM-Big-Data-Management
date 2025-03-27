from datetime import datetime
from airflow.decorators import dag, task
import os

# Sample configuration
configs = {
    "config1": {"message": "first DAG will receive this message"},
    "config2": {"message": "second DAG will receive this message"},
}

# Dynamically create a DAG for each configuration
for config_name, config in configs.items():
    dag_id = f"dynamic_generated_dag_{config_name}"

    # Define the DAG using @dag decorator
    @dag(dag_id=dag_id, start_date=datetime(2022, 2, 1), catchup=True)
    def dynamic_generated_dag():
        
        # Define task with @task decorator
        @task
        def print_message(message):
            print(message)

        # Call the task with the message from the config
        print_message(config["message"])

    # Create the DAG dynamically (Airflow will register it automatically)
    dynamic_generated_dag()