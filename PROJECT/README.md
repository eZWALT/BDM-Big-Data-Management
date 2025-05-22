# VibeRadar 

VibeRadar provides real-time and historical long-term analysis of product 
impressions across social media, helping companies understand public sentiment, 
track engagement, and optimize marketing strategies.

## Contents

- **\*-compose.yaml**: Files that contain the setup for the software and its 
  configuration. This is split into several files for each family of components
  (e.g. airflow, kafka, spark, etc.) The main one, and the one tha should be
  used to run the project is `docker-compose.yaml`
- **requirements.txt**: Python main base requirements 
- **configuration/**: Configuration files for all the project 
- **frontend/**: Streamlit front-end for the project data management and 
  analytics/dashboarding
- **src/**: Main source code of the project divided into 5 high level zones 
  (Ingestion, Landing, Trusted, Exploitation, Consumption...)
  
  - **airflow/**: DAG definitions and Airflow-related logic
  - **analysis/**: Streaming-related processing and analytical tools
  - **consumers/**: Data consumption modules like dashboards and sentiment analysis
  - **data_cleaners/**: Spark jobs to clean and validate ingested data
  - **data_loaders/**: Spark jobs to load data into the lake
  - **data_normalizer/**: Batch jobs to normalize and harmonize cleaned data
  - **governance/**: Code for data governance, schemas, and validation rules
  - **ingestion/**: Batch and streaming ingestion logic from various data sources
  - **landing/**: Batch jobs related to the creation of the landing zone
  - **utils/**: Utility functions shared across different modules

- **tests/**: Some unit tests for the project, as well as other scripts for
  testing the different components of the project

## Architectural design & tech stack

The following tech stack has been designed solely using open source big data
solutions:

- **Python** as the general scripting language.
- **Docker** as the containerization tool.
- **Apache Kafka** as the streaming data pipeline.
- **MinIO** as the object storage.
- **DeltaLake** as the storage layer between spark and the object storage.
- **Apache Spark** as the distributed data processing engine for batch data.
- **Apache Airflow** as the orchestrator of the data pipeline.
- **Streamlit** as the front-end framework for the project.
- **PostgreSQL** as the metadata storage for Airflow.
- **DuckDB** as the analytical database.

## Usage

In order to fully utilize this project, there is several software and access to
private APIs you will need, as described in the following sections. To guarantee
the ease of execution/portability of this project you will need to have Docker
installed.

Previous to running anything, you will need to get the following API keys:

1. **Twitter/X**: You will need to create a developer account and create an app
   in order to get the API keys.
2. **YouTube**: You will need to create a developer account and create an app
   in order to get the API keys.
3. **Bluesky**: You will need to create an account.

We recommend storing the following API keys in a `.env` file in the root of the
project, although this file will not be used in the containers, it will be
useful to keep track of everything.

The format of the .env file used in order to execute the whole project:

```bash 
TWITTER_API_KEY=...
YOUTUBE_API_KEY=...
BLUESKY_PASSWORD=...
BLUESKY_EMAIL=...
```

### Running the project the first time

The first time you run the project, be sure to NOT run the `docker-compose up -d`
command, as this will create ALL the images and containers, but we first want to
create the MinIO storage container, to get the credentials from there.

```sh
docker compose -f minio-compose.yaml up -d
```

This will spin up the MinIO container, which will be available at
`http://localhost:9000`. You can access the MinIO console using
`http://localhost:9001`.

The default credentials for the MinIO console are:
- **Username:** `minioadmin`
- **Password:** `minioadmin`

Once it is up and running, you should access the MinIO console
(`http://localhost:9001`) and navigate to the `Access Keys` tab on the left side
of the screen. There, you should create a new access and secret key pair. Name
it something informative, such as "Data Pipeline Key". Make sure to copy
these keys to the `.env` file you created earlier:

```bash
MINIO_ACCESS_KEY=...
MINIO_SECRET_KEY=...
```

While you are at it, add the following environment variables to the `.env` file:

```bash
MINIO_HOST=minio
MINIO_PORT=9000
```

Then, back in the MinIO console, go to the `Buckets` tab and create the following
buckets:
- `buffer` - This will hold the data in arbitrary format before being ingested
    into the landing zone.
- `landing` - This will hold the data in either BLOB or Parquet (Delta) format,
    before being cleaned and processed by further tasks.
- `trusted` - This will hold data that was taken from the landing zone, de-duplicated
    and cleaned. This data is ready to be used for further processing.
- `exploitation` - This will hold data grouped by the different products that 
    are being analyzed. This data is ready to be analyzed by NLP and other
    techniques.

Then, and only once you have done all the previous steps, you can start the rest
of the containers. To do so, be sure to first export all the environment variables
from the `.env` file to your shell. You can do this by running the following
command in the root of the project:

```sh
export $(cat .env | xargs)
```

> [!NOTE]
> Make sure that the variables are set by running the following command:
> ```sh
> env | grep MINIO
> ```

Then, you can run the following command to start the rest of the containers:

```sh
docker compose up -d --build
```

> [!NOTE]
> This command takes a long time to run (around 20 minutes) the first time
> you run it. Go grab a coffee or something while you wait.

After some minutes, you should have all the containers up and running. The 
following web pages should be available:

- **Apache Airflow**: `http://localhost:8080`
- **Apache Kafka**: `http://localhost:8082`
- **Apache Spark**: `http://localhost:8090`
- **Spark Notebook**: `http://localhost:8888`
- **MinIO**: `http://localhost:9001`
- **Streamlit**: `http://localhost:9999`

> [!NOTE]
> The Spark Notebook is a Jupyter notebook that is configured inside the network
> to be able to access the MinIO storage properly from inside the spark cluster.
> Otherwise, if you try to submit jobs from your local machine, given that the 
> url for the MinIO storage is different from the one inside the cluster 
> (`localhost:9000` vs `minio:9000`), it will not work.

> [!IMPORTANT]
> If you want to use the Notebook, you will need to take a look at the container
> logs to find the token to access the notebook.

Finally, you will need to create a connection in Airflow to connect to the Spark
cluster. To do this, go to the [Airflow web interface](http://localhost:8080),
enter the following credentials:

- **Username:** `airflow`
- **Password:** `airflow`

And navigate to the `Admin` -> `Connections` tab. There, create a new connection
with the following parameters:

- **Connection Id:** `spark_default`
- **Connection Type:** `Spark`
- **Host:** `spark://spark-master`
- **Port:** `7077`

The rest should be left as is, check the following:

- **Deploy mode:** `client`
- **Spark binary:** `spark-submit`

And, after that, you should be able to run the DAGs in Airflow. You can do this
by navigating to the `DAGs` tab and clicking on the `Trigger DAG` button. This
will start the DAG.
 
## Credits

- Walter J. Troiani 
- Marc Parcerisa
- Mateja Zatezalo

## License 

This project is licensed under the GPLv3 License. See the [LICENSE](../LICENSE) file for details.

