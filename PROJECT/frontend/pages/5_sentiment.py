import os
import io
import pandas as pd
import streamlit as st
from datetime import datetime
import pyarrow.parquet as pq
import duckdb
from urllib.parse import unquote
import altair as alt


from dev.minio import get_minio_client, list_companies, list_products, load_duckdb_from_minio
from dev.transforms import subset_sentiment, add_sentiment_score


# --------------------------
# VISUALIZATION
# --------------------------

def plot_sentiment_trend(df: pd.DataFrame):
    df["date"] = pd.to_datetime(df["transaction_timestamp"]).dt.date
    avg_sentiment = df.groupby("date")["sentiment_score"].mean().reset_index()

    chart = alt.Chart(avg_sentiment).mark_line(point=True).encode(
        x=alt.X("date:T", title="Date"),
        y=alt.Y("sentiment_score:Q", title="Average Sentiment Score", scale=alt.Scale(domain=[-1, 1])),
        tooltip=["date", "sentiment_score"]
    ).properties(title="📈 Average Sentiment Score Over Time", height=300)

    st.altair_chart(chart, use_container_width=True)
    
def plot_sentiment_distribution(df: pd.DataFrame):
    counts = df["sentiment"].value_counts().reset_index()
    counts.columns = ["sentiment", "count"]

    chart = alt.Chart(counts).mark_arc(innerRadius=50).encode(
        theta=alt.Theta(field="count", type="quantitative"),
        color=alt.Color(field="sentiment", type="nominal", legend=alt.Legend(title="Sentiment")),
        tooltip=["sentiment", "count"]
    ).properties(title="📊 Sentiment Distribution")

    st.altair_chart(chart, use_container_width=True)


def plot_sentiment_vs_engagement(df: pd.DataFrame):
    chart = alt.Chart(df).mark_circle(size=60, opacity=0.5).encode(
        x=alt.X("likes:Q", title="Likes"),
        y=alt.Y("sentiment_score:Q", title="Sentiment Score", scale=alt.Scale(domain=[-1,1])),
        color=alt.Color("sentiment:N", legend=alt.Legend(title="Sentiment")),
        tooltip=["title", "sentiment", "likes", "views", "shares"]
    ).properties(title="⚡ Sentiment vs Likes", height=300)

    st.altair_chart(chart, use_container_width=True)

# --------------------------
# MAIN DASHBOARD LAYOUT
# --------------------------
def show_layout():
    st.set_page_config(page_title="Sentiment Analysis", layout="wide")
    st.title("💬 Sentiment Analysis Dashboard")

    st.warning("⚠️ Work in Progress. Functionality may be limited ⚠️")

    # About section
    with st.expander(f"About this Dashboard"):
        st.markdown("""
        This dashboard helps you analyze **sentiment and emotion trends** in user posts related to a specific product/company.
        Each product's data is stored separately for modular and safe access for each company.

        **Features:**
        - 📈 Sentiment over time (average score)
        - 📊 Emotion distribution (labels)
        - 📝 Recent post sentiment samples
        """)


    minio = get_minio_client()
    bucket = "exploitation"

    # First: Select Company
    companies = list_companies(minio, bucket)
    if not companies:
        st.warning("No companies found in exploitation bucket.")
        st.stop()
    selected_company = st.sidebar.selectbox("Select Company", companies)
    # Then: Select Product for the company
    products = list_products(minio, bucket, selected_company)
    if not products:
        st.warning(f"No products found for {selected_company}.")
        st.stop()

    selected_product = st.sidebar.selectbox("Select Product", products)
    # Load data
    df = load_duckdb_from_minio(minio, bucket, selected_company, selected_product)
    if df.empty:
        st.stop()
        
    # Subse the original dataframe 
    df = subset_sentiment(df)
    
    # TODO: Revise this, probably transaction time does not make as much sense as creation_timestamp
    df["transaction_timestamp"] = pd.to_datetime(df["transaction_timestamp"], errors="coerce")
    df = df.dropna(subset=["transaction_timestamp"])
    # Fill unknown fields :)

    # Filter by platform
    selected_platforms = st.sidebar.multiselect(
        "Select Platform(s)", sorted(df["source"].dropna().unique()), default=sorted(df["source"].dropna().unique())
    )
    df = df[df["source"].isin(selected_platforms)]
    df = add_sentiment_score(df)

    # Visualizations
    st.subheader("📈 Average Sentiment Score Over Time")
    plot_sentiment_trend(df)

    st.subheader("📊 Sentiment Distribution")
    plot_sentiment_distribution(df)

    st.subheader("⚡ Sentiment vs Likes Engagement")
    plot_sentiment_vs_engagement(df)


    st.caption("Built with 🐍 Streamlit + 🪣 MinIO + 🔎 Altair + 🦆 DuckDB")


# --------------------------
# ENTRYPOINT
# --------------------------

if __name__ == "__main__":
    show_layout()
