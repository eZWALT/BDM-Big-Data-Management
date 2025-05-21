import os
import streamlit as st
import pandas as pd
import duckdb
from datetime import datetime, timedelta
import random
from typing import List

# --------------------------
# DATA LOADING
# --------------------------

def load_sentiment_from_duckdb(path='data/sentiment.duckdb') -> pd.DataFrame:
    con = duckdb.connect(path)
    df = con.execute("SELECT timestamp, company, product, platform, text, score, label FROM sentiment").df()
    con.close()
    return df

def load_sentiment_data(environment: str) -> pd.DataFrame:
    if environment == "production":
        return load_sentiment_from_duckdb()
    else:
        raise ValueError("development environment has been removed")


# --------------------------
# VISUALIZATION
# --------------------------

def plot_sentiment_over_time(df: pd.DataFrame):
    import altair as alt
    st.markdown("Shows how average sentiment changes over time.")
    chart = alt.Chart(df).mark_line().encode(
        x='timestamp:T',
        y='score:Q',
        color='platform:N'
    ).properties(height=300)
    st.altair_chart(chart, use_container_width=True)

def plot_label_distribution(df: pd.DataFrame, label_col="label"):
    import altair as alt
    label_df = df[label_col].value_counts().reset_index()
    label_df.columns = ["label", "count"]
    chart = alt.Chart(label_df).mark_bar().encode(
        x='label:N',
        y='count:Q',
        color='label:N'
    ).properties(height=300)
    st.altair_chart(chart, use_container_width=True)


# --------------------------
# MAIN LAYOUT
# --------------------------

def show_layout():
    st.set_page_config(page_title="Sentiment", layout="wide")
    st.title("💬 Sentiment Analysis Dashboard")

    environment = os.getenv("ENVIRONMENT_TYPE", "production")
    df = load_sentiment_data(environment)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Sidebar filters
    st.sidebar.header("🔍 Filter Data")
    selected_company = st.sidebar.selectbox("Select Company", sorted(df["company"].unique()))
    df = df[df["company"] == selected_company]

    selected_product = st.sidebar.selectbox("Select Product", sorted(df["product"].unique()))
    df = df[df["product"] == selected_product]

    selected_platforms = st.sidebar.multiselect(
        "Select Platform(s)", sorted(df["platform"].unique()), default=sorted(df["platform"].unique())
    )
    df = df[df["platform"].isin(selected_platforms)]

    # About section
    with st.expander(f"About this Dashboard"):
        st.markdown("""
        This dashboard helps you analyze **sentiment and emotion trends** in user posts related to a specific product/company.

        **Features:**
        - 📈 Sentiment over time (average score)
        - 📊 Emotion distribution (labels)
        - 📝 Recent post sentiment samples
        - 🧪 Live model prediction (in dev mode only)
        """)

    # Plots
    st.subheader("📈 Sentiment Score Over Time")
    plot_sentiment_over_time(df)

    st.subheader("📊 Emotion Label Distribution")
    plot_label_distribution(df, label_col="label")

    st.subheader("🔍 Recent Comments")
    st.dataframe(df.sort_values(by="timestamp", ascending=False)[
        ["timestamp", "label", "text"]
    ].reset_index(drop=True).head(10))

    st.markdown("---")
    st.caption("Built with 🐍 Streamlit + 🤗 Transformers + 🦆 DuckDB")


# --------------------------
# ENTRYPOINT
# --------------------------

if __name__ == "__main__":
    show_layout()
