import streamlit as st
import pandas as pd
import os
import json
import pyarrow.parquet as pq

# Function to list files in the directory
def list_files(directory, extensions=(".csv", ".json", ".parquet")):
    if not os.path.exists(directory):
        return []
    return [f for f in os.listdir(directory) if f.endswith(extensions)]

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
        st.error("Unsupported file type!")

# Streamlit UI Layout
def show_layout():
    st.set_page_config(page_title="Landing Zone (Bronze) 🥉", layout="wide")
    st.title("Landing Zone (Bronze) 🥉")
    
    # Select directory
    data_dir = st.text_input("📂 Enter Data Lake Path:", value="/path/to/data/lake")
    
    if os.path.exists(data_dir):
        files = list_files(data_dir)
        
        if files:
            selected_files = st.multiselect("📄 Select files:", files)
            
            if st.button("🔍 Load Files") and selected_files:
                for file in selected_files:
                    filepath = os.path.join(data_dir, file)
                    load_file(filepath)
        else:
            st.warning("No CSV, JSON, or Parquet files found in the selected directory.")

    else:
        st.error("Invalid directory! Please enter a valid path.")

if __name__ == "__main__":
    show_layout()
