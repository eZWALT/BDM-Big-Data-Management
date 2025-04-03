import streamlit as st
import requests
import json
import os
from streamlit_file_browser import st_file_browser

# Airflow API Configuration (Extracted from Docker Compose) to get URL
LOCAL_TEST = True
AIRFLOW_URL = "http://localhost:8080/api/v1" if LOCAL_TEST else "http://airflow-webserver:8080/api/v1"
AIRFLOW_AUTH = (
    os.getenv("AIRFLOW_USERNAME", "airflow"),  
    os.getenv("AIRFLOW_PASSWORD", "airflow") 
)

# Function to get available DAGs from Airflow
def get_airflow_dags():
    url = f"{AIRFLOW_URL}/dags"
    try:
        response = requests.get(url, auth=AIRFLOW_AUTH, timeout=5)
        if response.status_code == 200:
            dags = response.json()["dags"]
            return [dag["dag_id"] for dag in dags]
        else:
            st.error(f"Failed to fetch DAGs: {response.status_code} - {response.text}")
            return []
    except requests.exceptions.RequestException as e:
        st.error(f"Error connecting to Airflow: {e}")
        return []

def get_dag_status(dag_id):
    url = f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns"
    try:
        response = requests.get(url, auth=AIRFLOW_AUTH, timeout=5)
        if response.status_code == 200:
            dag_runs = response.json().get("dag_runs", [])
            if dag_runs:
                latest_run = dag_runs[0]
                return latest_run['state'], latest_run['start_date']
            else:
                return "No Runs", "N/A"
        else:
            st.error(f"Failed to fetch DAG status: {response.status_code} - {response.text}")
            return "Error", "N/A"
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching DAG status: {e}")
        return "Error", "N/A"

def pause_dag(dag_id):
    url = f"{AIRFLOW_URL}/dags/{dag_id}"
    payload = {"is_paused": True}
    try:
        response = requests.patch(url, json=payload, auth=AIRFLOW_AUTH, timeout=5)
        if response.status_code == 200:
            st.success(f"✅ DAG {dag_id} paused successfully!")
        else:
            st.error(f"❌ Failed to pause DAG {dag_id}: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        st.error(f"Error pausing DAG: {e}")

def unpause_dag(dag_id):
    url = f"{AIRFLOW_URL}/dags/{dag_id}"
    payload = {"is_paused": False}
    try:
        response = requests.patch(url, json=payload, auth=AIRFLOW_AUTH, timeout=5)
        if response.status_code == 200:
            st.success(f"✅ DAG {dag_id} unpaused successfully!")
        else:
            st.error(f"❌ Failed to unpause DAG {dag_id}: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        st.error(f"Error unpausing DAG: {e}")

def trigger_dag(dag_id):
    url = f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns"
    payload = {"conf": {}}
    try:
        response = requests.post(url, json=payload, auth=AIRFLOW_AUTH, timeout=5)
        if response.status_code == 200:
            st.success(f"✅ DAG {dag_id} triggered successfully!")
        else:
            st.error(f"❌ Failed to trigger {dag_id}: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        st.error(f"Error triggering DAG: {e}")

def trigger_all_dags(dags):
    for dag in dags:
        trigger_dag(dag)

def show_airflow_component():
    st.subheader("▶️ Airflow Management")

    available_dags = get_airflow_dags()

    if available_dags:
        # Layout: Using columns to display DAG status and last run info side by side
        col1, col2 = st.columns(2)

        with col1:
            selected_dag = st.selectbox("📌 Select a DAG to Manage:", available_dags)

        with col2:
            # Trigger All DAGs Button
            if st.button("🚀 Trigger All DAGs"):
                trigger_all_dags(available_dags)

        # Fetch DAG status and last run details
        dag_status, last_run = get_dag_status(selected_dag)

        # Display DAG status and last run info
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**Status:** {dag_status}")
        with col2:
            st.write(f"**Last Run:** {last_run}")

        # Show warnings or errors based on the DAG's status
        if dag_status == "running":
            st.warning("⚠️ This DAG is currently running.")
        elif dag_status == "failed":
            st.error("❌ Last DAG run failed. Please check the logs.")
        elif dag_status == "paused":
            st.info("🔒 This DAG is paused. You can resume it below.")

        with st.expander("DAG Controls", expanded=True):
            # Pause/Unpause Controls in columns
            col1, col2, col3 = st.columns(3)

            with col1:
                if st.button(f"⏸️ Pause DAG\n {selected_dag}"):
                    pause_dag(selected_dag)

            with col2:
                if st.button(f"⏺️ Unpause DAG\n {selected_dag}"):
                    unpause_dag(selected_dag)
                    
            with col3:
                if st.button(f"▶️ Trigger DAG\n {selected_dag}"):
                    trigger_dag(selected_dag)

    else:
        st.warning("⚠️ No DAGs found or unable to connect to Airflow.")


# Function to display the file browser
def show_file_browser(datalake_path: str):
    event = st_file_browser(
        datalake_path,
        key="file_browser",
        show_choose_file=True,
        show_choose_folder=True,
        show_download_file=True,
        show_upload_file=True,
    )
    return event

def show_layout(datalake_path: str):
    st.set_page_config(page_title="Landing Zone (Bronze) 🥉", layout="wide")
    st.title("Landing Zone (Bronze) 🥉")

    # Introduction Component
    st.markdown("""
    ### 📥 What is the Landing Zone?
    The **Landing Zone (Bronze)** is the **raw data layer** of the pipeline.  
    This is where raw, unprocessed data is first ingested from different sources before undergoing transformation.  
    It serves as a historical record of all incoming data. **Here users can ingest data by executing batch Airflow DAGs 
    or inspect the contents of the data lake.**
    """)

    with st.expander("View Landing Zone Description"):
        st.markdown("### 💾 Landing Zone:")
        st.write(
            """
            - **Raw Data Ingestion**: Collecting raw data from multiple sources
            - **Format Variability**: Data arrives in different formats (CSV, JSON, Parquet, etc.).
            - **Initial Storage**: Storing unprocessed data before transformations.
            - **Metadata Capture**: Capturing timestamps, file origins, and other metadata for traceability.
            """
        )

    # Airflow Component
    show_airflow_component()

    # File Browser Component
    st.subheader("🔍 Data Lake FileSystem")
    show_file_browser(datalake_path)

if __name__ == "__main__":
    show_layout(datalake_path="./test_data_lake")
