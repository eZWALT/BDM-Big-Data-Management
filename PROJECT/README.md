# VibeRadar 

## Description 

VibeRadar provides real-time and historical long-term analysis of product impressions across social media, helping companies understand public sentiment, track engagement, and optimize marketing strategies.

## Contents

- **docker-compose.yaml**: Contains the whole software set-up and configuration of the different needed processes.
- **.github/workflows/**: CI/CD configuration files
- **config/**: configuration files 
- **data/**: persistent data 
- **resources/**: images, audios or other miscellaneous resources
- **src/**: main source code of the project divided into 4 high level zones (Ingestion, Landing, Trusted, Exploitation)


## Architectural desing & tech stack

The following tech stack has been used solely using open source big data solutions:

## Tech Stack
This topic is still a work-in-progress section:

- **Python**
- **Docker** 
- **Apache Kafka**
- **Apache ZooKeeper**
- **Jupyter**
- **DeltaLake**
- **Apache Spark**
- **Apache Airflow**
- **Streamlit**

## Design 

WIP


## Usage

In order to fully utilize this project, there is several software and access to private apis you will need, as described in the following sections. To guarantee the ease of execution/portability of this project you will need:

- Docker Compose                   (Mandatory) 
- Java (To execute spark locally)  (Optional) --> aditionally create a HOST_IP environment variable with your IPv4
- Python 3.11 (To execute locally) (Optional)
- Set up a .env file inside both the airflow directory and main project directory (API KEYS)

Note: To execute pyspark on the driver node (your pc/laptop) you will need to install java and set the java_home environment variable and have python 3.11

### Dockerized execution

You can run this project either by setting up the environment locally or using Docker. For simplicity docker compose setup is showcased:

```sh
docker-compose up -d
```

The basic structure of this docker compose server is the following, bearing in mind that they share a storage device (through different volumes) and a bridge network:
1. Apache Airflow: Orchestrator that depends on PostgreSQL
2. PostgreSQL
3. Kafka: Streaming ingestor that depends on ZooKeeper
4. ZooKeeper
5. Spark Master: 1 master
6. Spark Workers: up to N workers
7. Streamlit frontend

### Social Media API's

> Note that you will need several API keys to fully utilize the infrastructure of this project

1. Twitter/X 
2. Bluesky
3. Youtube 

## Testing suite

### Project 
To contribute to our test-driven development, we continiously produce unit tests, which can be executed inside the docker environment using the following command from the base project path:

```sh
PYTHONPATH=. pytest tests/
```

### Airflow

Notice that also DAG tests can be found in `airflow/tests` in order to verify the correct working of airflow.

```sh
PYTHONPATH=. pytest airflow/tests/
```

## TODO 
For the 1st part of the project, the following tasks must be completed:

- [x] Everybody do their BatchProducerTask (ETL)             (Marc)
- [x] Finalize the streaming (just landing)                  (Marc, Walter)
- [x] Add functionality to the streamlit (landing zone page) (Walter)
- [x] Debug & test dynamic dags (Airflow)                    (Walter)
- [x] Test Docker compose (Everything tighted up)            (Marc, Mateja)
- [ ] Get delta table folder working                         (Mateja)
- [ ] Automatize delta lake (dag) and put it in docker?      (Mateja, Walter)
- [ ] Writing the final report                               (Mateja, Marc, Walter)

For the 2nd part of the project, the following tasks must be completed:

- [ ] TRUSTED ZONE 
- [ ] Create Streaming Explotitation zone
- [ ] Ideate 2 Small Easy Exploitation tasks
- [ ] Update the frontend accordingly
- [ ] Update the airflow dags accordingly 
 
## Credits

- Walter J. Troiani 
- Marc Parcerisa
- Mateja Zatezalo

## License 

This project is licensed under the GPLv3 License. See the [LICENSE](../LICENSE) file for details.

