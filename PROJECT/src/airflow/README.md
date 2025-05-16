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

## About the `libs/` folder

When executing spark code on the cluster, the code might need some libraries that
are not available out of the box in the cluster. In this case, we need to upload
those along with the `spark-submit` command. This is done by using the `--py-files` 
option. In the `libs/` folder, we have placed the libraries that we might need
to upload.

### How to create a new library

1. Create a new folder somewhere in your machine (This will be deleted later)
    ```bash
    mkdir <your_folder_name>
    cd <your_folder_name>
    ```
2. Download the library (or libraries) that you want to package.
    ```bash
    pip download <library_name>
    ```
    This will download a bunch of `.whl` or `.tar.gz` files. It will also
    download the dependencies of the libraries that you are downloading, so
    you might end up with a lot of files.
3. Install them in an isolated `python` folder. This is the standard for spark.
    ```bash
    mkdir python
    find . -type f \( -name "*.whl" -o -name "*.tar.gz" \) -exec python3 -m pip install --target=python {} +
    ```
4. Zip the folder
    ```bash
    zip -r <your_package_name>.zip python
    ```
5. Move the zip file to the `libs/` folder
    ```bash
    mv <your_package_name>.zip /path/to/libs/
    ```
6. Clean up
    ```bash
    cd ..
    rm -rf <your_folder_name>
    ```

Then, you'll be able to pass this file to the `--py-files` option in the `spark-submit` command.
```bash
spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode cluster \
    --py-files libs/<your_package_name>.zip \
    --conf spark.executorEnv.AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    --conf spark.executorEnv.AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    <your_script.py>
```
