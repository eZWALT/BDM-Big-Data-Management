import streamlit as st
import pandas as pd
import os
import json

# Function to list files in the selected directory
def list_files(directory):
    if not os.path.exists(directory):
        return []
    return [f for f in os.listdir(directory) if f.endswith(('.csv', '.json'))]

# Function to load and display data
def load_file(filepath):
    file_ext = os.path.splitext(filepath)[1]
    
    if file_ext == ".csv":
        df = pd.read_csv(filepath)
        st.write("### 📊 CSV Data Preview")
        st.dataframe(df)
    
    elif file_ext == ".json":
        with open(filepath, "r", encoding="utf-8") as f:
            json_data = json.load(f)
        st.write("### 📜 JSON File Preview")
        st.json(json_data)

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
            selected_file = st.selectbox("📄 Select a file:", files)
            filepath = os.path.join(data_dir, selected_file)
            
            if st.button("🔍 Load File"):
                load_file(filepath)
        else:
            st.warning("No CSV or JSON files found in the selected directory.")

    else:
        st.error("Invalid directory! Please enter a valid path.")

if __name__ == "__main__":
    show_layout()
