# Airflow configuration

## Description

For each company / use case that VibeRadar wants to track, we will potentially create a parametritzed version from the abstract ones (each dag is a workflow/controlflow)

## Usage

For a fast basic local test, the following commands can be used:

```sh 
pip install apache-airflow
# Initialize the Airflow database
airflow db init
airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
# Start the Airflow web server
airflow webserver -p 8080
# Start the Airflow scheduler
airflow scheduler
```