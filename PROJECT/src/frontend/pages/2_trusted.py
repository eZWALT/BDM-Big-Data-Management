# pages/trusted.py

import streamlit as st

def show_layout():
    st.set_page_config(page_title="Trusted Zone (Silver) 🥈", layout="wide")
    st.title("Trusted Zone (Silver) 🥈")

    st.warning("⚠️ Work in Progress. Functionality may be limited⚠️")

    st.markdown("""
    ### 🔐 What is the Trusted Zone?
    The **Trusted Zone (Silver)** is where raw ingested data from the **Landing Zone (Bronze)** gets **cleaned, validated, and structured**.
    Data transformations, deduplication, and schema enforcement occur here before moving to the **Exploitation Zone (Gold)**.
    """)
    
    with st.expander("View Trusted Zone Description"):
        st.markdown("### 🧹 Trusted Zone Key Processes:")
        st.write(
            """
            - **Data Cleansing**: Removing duplicates, handling missing values, and standardizing formats.
            - **Schema Validation**: Ensuring data adheres to defined schemas for consistency.
            - **Deduplication & Enrichment**: Merging records, adding metadata, and preparing data for analysis.
            - **Homogenization of data**: Data from different social medias have widely different formats that need some structuring.
            """
        )

    st.markdown("🔍 **Explore more about our trusted platform and data security features!**")

if __name__ == "__main__":
    show_layout()
