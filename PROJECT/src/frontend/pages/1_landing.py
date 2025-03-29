import streamlit as st
import requests
import json
from streamlit_file_browser import st_file_browser

# Airflow API Configuration (Extracted from Docker Compose)
AIRFLOW_URL = "http://localhost:8080/api/v1"
# Inter-container dummy auth
AIRFLOW_AUTH = ("airflow", "airflow")  

# Function to get available DAGs from Airflow
def get_airflow_dags():
    url = f"{AIRFLOW_URL}/dags"
    try:
        response = requests.get(url, auth=AIRFLOW_AUTH, timeout=5)
        if response.status_code == 200:
            dags = response.json()["dags"]
            return [dag["dag_id"] for dag in dags]
        else:
            st.error(f"Failed to fetch DAGs: {response.status_code}")
            return []
    except requests.exceptions.RequestException as e:
        st.error(f"Error connecting to Airflow: {e}")
        return []

def trigger_dag(dag_id):
    url = f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns"
    payload = {"conf": {}}
    try:
        response = requests.post(url, json=payload, auth=AIRFLOW_AUTH, timeout=5)
        if response.status_code == 200:
            st.success(f"✅ DAG {dag_id} triggered successfully!")
        else:
            st.error(f"❌ Failed to trigger {dag_id}: {response.status_code}")
    except requests.exceptions.RequestException as e:
        st.error(f"Error triggering DAG: {e}")

def trigger_all_dags(dags):
    for dag in dags:
        trigger_dag(dag)

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

    # Introduction COmponent
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

    # Airflow DAG Component
    st.subheader("▶️ Airflow Batch Ingestion")
    available_dags = get_airflow_dags()

    if available_dags:
        if st.button("🚀 Run All DAGs"):
            trigger_all_dags(available_dags)

        selected_dag = st.selectbox("📌 Select a DAG to Run:", available_dags)
        if st.button(f"▶️ Run {selected_dag}"):
            trigger_dag(selected_dag)
    else:
        st.warning("⚠️ No DAGs found or unable to connect to Airflow.")

    # File Browser Component
    st.subheader("🔍 Data Lake FileSystem")
    show_file_browser(datalake_path)

if __name__ == "__main__":
    show_layout(datalake_path="./test_data_lake")
