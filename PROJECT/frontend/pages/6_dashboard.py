import os
import io
import pandas as pd
import streamlit as st
import altair as alt
import duckdb
from datetime import datetime
from dev.minio import get_minio_client, list_companies, list_products, load_duckdb_from_minio

# --------------------------
# KPI Cards
# --------------------------

def render_kpis(df: pd.DataFrame):
    total_posts = len(df)
    avg_likes = df["likes"].mean() if not df.empty else 0
    avg_replies = df["replies"].mean() if not df.empty else 0
    top_platform = df["source"].mode()[0] if not df.empty else "N/A"

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Posts", f"{total_posts}")
    col2.metric("Avg Likes per Post", f"{avg_likes:.1f}")
    col3.metric("Avg Replies per Post", f"{avg_replies:.1f}")
    col4.metric("Top Platform", top_platform)

# --------------------------
# Plots
# --------------------------

def plot_volume_over_time(df: pd.DataFrame):
    df_count = (
        df.groupby([pd.Grouper(key="transaction_timestamp", freq="D"), "source"])
          .size()
          .reset_index(name="post_count")
    )
    chart = (
        alt.Chart(df_count)
        .mark_bar()
        .encode(
            x=alt.X("transaction_timestamp:T", title="Date"),
            y=alt.Y("post_count:Q", title="Number of Posts"),
            color="source:N",
            tooltip=["transaction_timestamp:T", "source:N", "post_count:Q"],
        )
        .properties(height=300)
    )
    st.altair_chart(chart, use_container_width=True)

def plot_platform_share(df: pd.DataFrame):
    df_platform = df["source"].value_counts().reset_index()
    df_platform.columns = ["platform", "count"]
    chart = (
        alt.Chart(df_platform)
        .mark_arc(innerRadius=50)
        .encode(
            theta=alt.Theta(field="count", type="quantitative"),
            color=alt.Color(field="platform", type="nominal"),
            tooltip=["platform", "count"]
        )
        .properties(height=250)
    )
    st.altair_chart(chart, use_container_width=True)

def plot_engagement_momentum(df: pd.DataFrame):
    df_sorted = df.sort_values("transaction_timestamp")
    df_engagement = (
        df_sorted.groupby("transaction_timestamp")[["likes", "replies"]].sum().reset_index()
    )
    df_engagement["likes_7d_avg"] = df_engagement["likes"].rolling(7).mean()
    df_engagement["replies_7d_avg"] = df_engagement["replies"].rolling(7).mean()

    base = alt.Chart(df_engagement).encode(x="transaction_timestamp:T")

    likes_line = base.mark_line(color="blue").encode(
        y=alt.Y("likes_7d_avg", title="7-day Avg Likes"),
        tooltip=[alt.Tooltip("transaction_timestamp:T", title="Date"), alt.Tooltip("likes_7d_avg", title="Likes")]
    )
    replies_line = base.mark_line(color="orange").encode(
        y=alt.Y("replies_7d_avg", title="7-day Avg Replies"),
        tooltip=[alt.Tooltip("transaction_timestamp:T", title="Date"), alt.Tooltip("replies_7d_avg", title="Replies")]
    )

    chart = alt.layer(likes_line, replies_line).resolve_scale(
        y="independent"
    ).properties(height=300, title="📈 Engagement Momentum (7-day rolling avg)")

    st.altair_chart(chart, use_container_width=True)

# --------------------------
# Top Liked Posts Table
# --------------------------

def render_top_liked_posts(df: pd.DataFrame, n=10):
    top_likes = df.nlargest(n, "likes")[
        ["transaction_timestamp", "source", "title", "description", "likes", "replies"]
    ]
    st.subheader(f"🔥 Top {n} Most Liked Posts")
    st.dataframe(top_likes.reset_index(drop=True))

# --------------------------
# Main dashboard layout
# --------------------------

def show_layout():
    st.set_page_config(page_title="Engagement Dashboard", layout="wide")
    st.title("📊 Engagement & Platform Analysis Dashboard")

    st.warning("⚠️ Work in Progress. Functionality may be limited ⚠️")

    # About section expander
    with st.expander("About this Dashboard"):
        st.markdown("""
        This dashboard helps analyze **engagement metrics and platform distribution** for a selected product/company.

        **Features:**
        - 📈 Post Volume Over Time by Platform
        - 📊 Platform Share Distribution
        - 📈 Engagement Momentum (7-day rolling averages)
        - 🔥 Top Liked Posts
        - 📝 Recent Posts with engagement metrics
        """)

    # Setup MinIO connection and bucket
    minio = get_minio_client()
    bucket = "exploitation"

    # Company and Product selection
    companies = list_companies(minio, bucket)
    if not companies:
        st.warning("No companies found in bucket.")
        st.stop()
    selected_company = st.sidebar.selectbox("Select Company", companies)

    products = list_products(minio, bucket, selected_company)
    if not products:
        st.warning(f"No products found for {selected_company}.")
        st.stop()
    selected_product = st.sidebar.selectbox("Select Product", products)

    # Load data from MinIO DuckDB
    df = load_duckdb_from_minio(minio, bucket, selected_company, selected_product)
    if df.empty:
        st.warning("No data available for this selection.")
        st.stop()

    # Convert transaction_timestamp to datetime if not already
    df["transaction_timestamp"] = pd.to_datetime(df["transaction_timestamp"], errors="coerce")
    df = df.dropna(subset=["transaction_timestamp"])

    # Platform filter
    platforms = sorted(df["source"].dropna().unique())
    selected_platforms = st.sidebar.multiselect("Select Platform(s)", platforms, default=platforms)
    df = df[df["source"].isin(selected_platforms)]

    # Date filter
    min_date = df["transaction_timestamp"].min().date()
    max_date = df["transaction_timestamp"].max().date()
    date_range = st.sidebar.date_input("Select Date Range", [min_date, max_date], min_value=min_date, max_value=max_date)
    if len(date_range) == 2:
        df = df[(df["transaction_timestamp"].dt.date >= date_range[0]) & (df["transaction_timestamp"].dt.date <= date_range[1])]

    # KPIs
    render_kpis(df)

    # Plots
    st.subheader("📈 Post Volume Over Time by Platform")
    plot_volume_over_time(df)

    st.subheader("📊 Platform Share")
    plot_platform_share(df)

    st.subheader("📈 Engagement Momentum (7-day Rolling Average)")
    plot_engagement_momentum(df)

    # Top liked posts table
    render_top_liked_posts(df)

    # Recent posts table
    st.subheader("📝 Recent Posts")
    recent_posts = df.sort_values("transaction_timestamp", ascending=False)[
        ["transaction_timestamp", "source", "title", "description", "likes", "replies"]
    ]
    st.dataframe(recent_posts.reset_index(drop=True))

    st.caption("Built with 🐍 Streamlit + 🪣 MinIO + 🔎 Altair + 🦆 DuckDB")

# --------------------------
# Entrypoint
# --------------------------

if __name__ == "__main__":
    show_layout()
