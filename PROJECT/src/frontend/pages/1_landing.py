import streamlit as st
import pandas as pd
import os
import json
from streamlit_file_browser import st_file_browser

# Function to load and display data
def load_file(filepath):
    file_ext = os.path.splitext(filepath)[1]

    if file_ext == ".csv":
        df = pd.read_csv(filepath)
        st.write(f"### 📊 CSV Data Preview: {os.path.basename(filepath)}")
        st.dataframe(df)

    elif file_ext == ".json":
        with open(filepath, "r", encoding="utf-8") as f:
            json_data = json.load(f)
        st.write(f"### 📜 JSON File Preview: {os.path.basename(filepath)}")
        st.json(json_data)

    elif file_ext == ".parquet":
        df = pd.read_parquet(filepath)
        st.write(f"### 🗂️ Parquet Data Preview: {os.path.basename(filepath)}")
        st.dataframe(df)

    else:
        st.error("❌ Unsupported file type!")

# Streamlit UI Layout
def show_layout(datalake_path: str):
    st.set_page_config(page_title="Landing Zone (Bronze) 🥉", layout="wide")
    st.title("Landing Zone (Bronze) 🥉")

    st.warning("⚠️ Work in Progress. Functionality may be limited ⚠️")

    st.markdown("""
    ### 📥 What is the Landing Zone?
    The **Landing Zone (Bronze)** is the **raw data layer** of the pipeline.  
    This is where **raw, unprocessed** data is first ingested from different sources before undergoing transformation.  
    It serves as a **historical record** of all incoming data.
    """)

    with st.expander("View Landing Zone Description"):
        st.markdown("### 💾 Landing Zone:")
        st.write(
            """
            - **Raw Data Ingestion**: Collecting raw data from multiple sources.
            - **Format Variability**: Data arrives in different formats (CSV, JSON, Parquet, etc.).
            - **Initial Storage**: Storing unprocessed data before transformations.
            - **Metadata Capture**: Capturing timestamps, file origins, and other metadata for traceability.
            """
        )

    st.markdown("🔍 **Browse and preview the raw data below!**")

    # File Browser Component
    st.header("📂 File Browser")

    event = st_file_browser(
        datalake_path,  
        key="file_browser",
        show_choose_file=True,
        show_choose_folder=True,
        #show_delete_file=True,
        show_download_file=True,
        show_new_folder=True,
        show_upload_file=True,
        #show_rename_file=True,
        #show_rename_folder=True,
    )
    # This show the picked file
    # st.write(event)

if __name__ == "__main__":
    show_layout(datalake_path="./data_lake")
