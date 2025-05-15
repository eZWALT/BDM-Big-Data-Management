import streamlit as st
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
from typing import Dict, List, Any
import os

# ----------------------------
# Platforms Supported
# ----------------------------

PLATFORMS = ["YouTube", "Twitter", "Bluesky"]

# ----------------------------
# Synthetic Data Fetchers
# ----------------------------

def fetch_synthetic_data_youtube() -> List[Dict[str, Any]]:
    return [
        {"title": "Video A", "views": 10000, "likes": 500, "comments": 50},
        {"title": "Video B", "views": 20000, "likes": 700, "comments": 100},
    ]

def fetch_synthetic_data_twitter() -> List[Dict[str, Any]]:
    return [
        {"text": "Tweet A", "likes": 100, "retweets": 40, "created_at": "2025-05-10"},
        {"text": "Tweet B", "likes": 250, "retweets": 60, "created_at": "2025-05-11"},
    ]

def fetch_synthetic_data_bluesky() -> List[Dict[str, Any]]:
    return [
        {"post": "Post A", "likes": 80, "replies": 20, "date": "2025-05-10"},
        {"post": "Post B", "likes": 150, "replies": 45, "date": "2025-05-12"},
    ]

# ----------------------------
# DuckDB Data Fetchers (Production)
# ----------------------------

def fetch_real_data_youtube() -> List[Dict[str, Any]]:
    connection = duckdb.connect("your_database.db")
    query = "SELECT title, views, likes, comments FROM youtube_videos LIMIT 100"
    result = connection.execute(query).fetchall()
    connection.close()
    return [{"title": r[0], "views": r[1], "likes": r[2], "comments": r[3]} for r in result]

def fetch_real_data_twitter() -> List[Dict[str, Any]]:
    connection = duckdb.connect("your_database.db")
    query = "SELECT text, likes, retweets, created_at FROM tweets LIMIT 100"
    result = connection.execute(query).fetchall()
    connection.close()
    return [{"text": r[0], "likes": r[1], "retweets": r[2], "created_at": r[3]} for r in result]

def fetch_real_data_bluesky() -> List[Dict[str, Any]]:
    connection = duckdb.connect("your_database.db")
    query = "SELECT post, likes, replies, date FROM bluesky_posts LIMIT 100"
    result = connection.execute(query).fetchall()
    connection.close()
    return [{"post": r[0], "likes": r[1], "replies": r[2], "date": r[3]} for r in result]

# ----------------------------
# Abstract Fetcher Dispatcher
# ----------------------------

def fetch_data(platform: str, environment: str) -> List[Dict[str, Any]]:
    key = f"{environment.lower()}_{platform.lower()}"
    fetchers = {
        "development_youtube": fetch_synthetic_data_youtube,
        "development_twitter": fetch_synthetic_data_twitter,
        "development_bluesky": fetch_synthetic_data_bluesky,
        "production_youtube": fetch_real_data_youtube,
        "production_twitter": fetch_real_data_twitter,
        "production_bluesky": fetch_real_data_bluesky,
    }
    return fetchers.get(key, lambda: [])()

# ----------------------------
# Visualization Dispatcher
# ----------------------------

def visualize_data(platform: str, data: List[Dict[str, Any]]) -> None:
    df = pd.DataFrame(data)
    st.dataframe(df)

    if platform == "YouTube":
        st.subheader("📊 YouTube Views vs Likes")
        fig, ax = plt.subplots()
        ax.bar(df["title"], df["views"], label="Views", alpha=0.7)
        ax.bar(df["title"], df["likes"], label="Likes", alpha=0.7)
        ax.legend()
        st.pyplot(fig)

    elif platform == "Twitter":
        st.subheader("📊 Twitter Likes vs Retweets")
        fig, ax = plt.subplots()
        ax.bar(df["text"], df["likes"], label="Likes", alpha=0.7)
        ax.bar(df["text"], df["retweets"], label="Retweets", alpha=0.7)
        ax.legend()
        st.pyplot(fig)

    elif platform == "Bluesky":
        st.subheader("📊 Bluesky Likes vs Replies")
        fig, ax = plt.subplots()
        ax.bar(df["post"], df["likes"], label="Likes", alpha=0.7)
        ax.bar(df["post"], df["replies"], label="Replies", alpha=0.7)
        ax.legend()
        st.pyplot(fig)

# ----------------------------
# UI Layout
# ----------------------------

def render_intro(environment: str):
    st.title("📱 Social Media Analytics Dashboard")
    
    with st.expander("View Analytical Dashboard Description"):
    # TODO MAKE THE MARKDOWN DYNAMIC TO THE NUMBER OF SOCIAL MEDIAS
        st.markdown(
            f"""
            Welcome to the unified dashboard for monitoring engagement across multiple platforms:
            **YouTube**, **Twitter**, and **Bluesky**.
            
            This application is currently running in **{environment.upper()}** mode, using 
            {"gold data from DuckDB" if environment == "production" else "synthetic development data"}.

            Use the sidebar to choose a platform and view corresponding statistics.
            """
        )
        st.markdown("---")

# ----------------------------
# Main App
# ----------------------------

def main():
    environment = os.getenv("ENVIRONMENT_TYPE", "development").lower()

    st.sidebar.title("🔧 Configuration")
    st.sidebar.markdown(f"**Environment:** `{environment}`")
    platform = st.sidebar.selectbox("Select Platform", PLATFORMS)

    render_intro(environment)

    data = fetch_data(platform, environment)
    if data:
        visualize_data(platform, data)
    else:
        st.warning("No data available for the selected platform and environment.")

if __name__ == "__main__":
    main()
