# pages/dashboard.py

import streamlit as st
import pandas as pd
import altair as alt
import os
import random
from datetime import datetime, timedelta
import duckdb

# ----------------------------
# Data Loader (DuckDB)
# ----------------------------

def load_data() -> pd.DataFrame:
    environment = os.getenv("ENVIRONMENT_TYPE", "development")
    if environment == "production":
        # Example placeholder, replace with your real DB loading code
        con = duckdb.connect("data/sentiment.duckdb")
        df = con.execute("SELECT timestamp, company, product, platform, text, num_likes, num_comments FROM sentiment").df()
        con.close()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    else:
        raise ValueError("development environment has been removed")

# ----------------------------
# KPI Cards
# ----------------------------

def render_kpis(df: pd.DataFrame):
    total_posts = len(df)
    avg_likes = df["num_likes"].mean()
    avg_comments = df["num_comments"].mean()
    top_platform = df["platform"].mode()[0] if not df.empty else "N/A"

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Posts", f"{total_posts}")
    col2.metric("Avg Likes per Post", f"{avg_likes:.1f}")
    col3.metric("Avg Comments per Post", f"{avg_comments:.1f}")
    col4.metric("Top Platform", top_platform)

# ----------------------------
# Plots
# ----------------------------

def plot_volume_over_time(df: pd.DataFrame):
    df_count = (
        df.groupby([pd.Grouper(key="timestamp", freq="D"), "platform"])
          .size()
          .reset_index(name="post_count")
    )
    chart = (
        alt.Chart(df_count)
        .mark_bar()
        .encode(
            x=alt.X("timestamp:T", title="Date"),
            y=alt.Y("post_count:Q", title="Number of Posts"),
            color="platform:N",
            tooltip=["timestamp:T", "platform:N", "post_count:Q"],
        )
        .properties(height=300)
    )
    st.altair_chart(chart, use_container_width=True)

def plot_platform_share(df: pd.DataFrame):
    df_platform = df["platform"].value_counts().reset_index()
    df_platform.columns = ["platform", "count"]
    chart = (
        alt.Chart(df_platform)
        .mark_arc()
        .encode(
            theta=alt.Theta(field="count", type="quantitative"),
            color=alt.Color(field="platform", type="nominal"),
            tooltip=["platform", "count"]
        )
        .properties(height=250)
    )
    st.altair_chart(chart, use_container_width=True)

def plot_engagement_momentum(df: pd.DataFrame):
    # Calculate 7-day rolling averages for likes and comments
    df_sorted = df.sort_values("timestamp")
    df_engagement = (
        df_sorted.groupby("timestamp")[["num_likes", "num_comments"]].sum().reset_index()
    )
    df_engagement["likes_7d_avg"] = df_engagement["num_likes"].rolling(7).mean()
    df_engagement["comments_7d_avg"] = df_engagement["num_comments"].rolling(7).mean()

    base = alt.Chart(df_engagement).encode(x="timestamp:T")

    likes_line = base.mark_line(color="blue").encode(
        y=alt.Y("likes_7d_avg", title="7-day Avg Likes"),
        tooltip=[alt.Tooltip("timestamp:T", title="Date"), alt.Tooltip("likes_7d_avg", title="Likes")]
    )
    comments_line = base.mark_line(color="orange").encode(
        y=alt.Y("comments_7d_avg", title="7-day Avg Comments"),
        tooltip=[alt.Tooltip("timestamp:T", title="Date"), alt.Tooltip("comments_7d_avg", title="Comments")]
    )

    chart = alt.layer(likes_line, comments_line).resolve_scale(
        y="independent"
    ).properties(height=300, title="Engagement Momentum (7-day rolling avg)")

    st.altair_chart(chart, use_container_width=True)

# ----------------------------
# Top Liked Posts Table
# ----------------------------

def render_top_liked_posts(df: pd.DataFrame, n=10):
    top_likes = df.nlargest(n, "num_likes")[
        ["timestamp", "platform", "company", "product", "text", "num_likes", "num_comments"]
    ]
    st.subheader(f"🔥 Top {n} Most Liked Posts")
    st.dataframe(top_likes.reset_index(drop=True))

# ----------------------------
# Main App
# ----------------------------

def show_layout():
    st.set_page_config(page_title="Dashboard", layout="wide")
    st.title("📊 Media Analysis Dashboard")
    
    # About this dashboard expander
    with st.expander(f"About this Dashboard:"):
        st.markdown(
            """
            This dashboard helps you analyze social media engagement and activity for the selected **product/company pair**.

            **Features:**

            - 📈 **Post Volume Over Time:** Tracks daily number of posts by platform.
            - 📊 **Platform Share:** Shows distribution of posts across platforms.
            - 📈 **Engagement Momentum:** Visualizes 7-day rolling averages of likes and comments.
            - 🔥 **Top Liked Posts:** Highlights the posts with highest likes.
            - 📝 **Recent Posts:** Displays latest posts with engagement metrics.
            """
        )
    
    # Load data
    df = load_data()

    # Sidebar filters
    st.sidebar.header("🔍 Filter Data")
    selected_company = st.sidebar.selectbox("Select Company", sorted(df["company"].unique()))
    filtered_df = df[df["company"] == selected_company]

    selected_product = st.sidebar.selectbox("Select Product", sorted(filtered_df["product"].unique()))
    filtered_df = filtered_df[filtered_df["product"] == selected_product]

    selected_platform = st.sidebar.multiselect(
        "Select Platform(s)", sorted(filtered_df["platform"].unique()), default=sorted(filtered_df["platform"].unique())
    )
    filtered_df = filtered_df[filtered_df["platform"].isin(selected_platform)]

    # Date filter
    min_date = filtered_df["timestamp"].min()
    max_date = filtered_df["timestamp"].max()
    date_range = st.sidebar.date_input("Select Date Range", [min_date, max_date], min_value=min_date, max_value=max_date)
    if len(date_range) == 2:
        filtered_df = filtered_df[(filtered_df["timestamp"].dt.date >= date_range[0]) & (filtered_df["timestamp"].dt.date <= date_range[1])]

    # PLOT 1: KPI's display
    render_kpis(filtered_df)
    
    
    # PLOT 2: Volume over time by platform (Num posts)
    st.subheader("📈 Post Volume Over Time by Platform")
    plot_volume_over_time(filtered_df)

    # Platform share pie chart
    st.subheader("📊 Platform Share")
    plot_platform_share(filtered_df)

    # Engagement momentum chart
    st.subheader("📈 Engagement Momentum (7-day Rolling Average)")
    plot_engagement_momentum(filtered_df)

    # Top liked posts
    render_top_liked_posts(filtered_df)

    # Recent posts table
    st.subheader("📝 Recent Posts")
    recent_posts = filtered_df.sort_values("timestamp", ascending=False)[
        ["timestamp", "platform", "company", "product", "text", "num_likes", "num_comments"]
    ]
    st.dataframe(recent_posts.reset_index(drop=True))


if __name__ == "__main__":
    show_layout()
