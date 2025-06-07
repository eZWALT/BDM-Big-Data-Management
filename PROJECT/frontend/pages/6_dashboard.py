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
    avg_likes = df["num_likes"].mean() if not df.empty else 0
    avg_comments = df["num_comments"].mean() if not df.empty else 0
    top_platform = df["platform"].mode()[0] if not df.empty else "N/A"

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Posts", f"{total_posts}")
    col2.metric("Avg Likes per Post", f"{avg_likes:.1f}")
    col3.metric("Avg Comments per Post", f"{avg_comments:.1f}")
    col4.metric("Top Platform", top_platform)

# --------------------------
# Plots
# --------------------------

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
    ).properties(height=300, title="📈 Engagement Momentum (7-day rolling avg)")

    st.altair_chart(chart, use_container_width=True)

# --------------------------
# Top Liked Posts Table
# --------------------------

def render_top_liked_posts(df: pd.DataFrame, n=10):
    top_likes = df.nlargest(n, "num_likes")[
        ["timestamp", "platform", "company", "product", "text", "num_likes", "num_comments"]
    ]
    st.subheader(f"🔥 Top {n} Most Liked Posts")
    st.dataframe(top_likes.reset_index(drop=True))

# --------------------------
# Main dashboard layout
# --------------------------

def show_layout():
    st.set_page_config(page_title="Engagement Dashboard", layout="wide")
    st.title("📊 Engagement & Platform Analysis Dashboard")

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

    # Platform filter
    platforms = sorted(df["platform"].dropna().unique())
    selected_platforms = st.sidebar.multiselect("Select Platform(s)", platforms, default=platforms)
    df = df[df["platform"].isin(selected_platforms)]

    # Date filter
    min_date = df["timestamp"].min().date()
    max_date = df["timestamp"].max().date()
    date_range = st.sidebar.date_input("Select Date Range", [min_date, max_date], min_value=min_date, max_value=max_date)
    if len(date_range) == 2:
        df = df[(df["timestamp"].dt.date >= date_range[0]) & (df["timestamp"].dt.date <= date_range[1])]

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
    recent_posts = df.sort_values("timestamp", ascending=False)[
        ["timestamp", "platform", "company", "product", "text", "num_likes", "num_comments"]
    ]
    st.dataframe(recent_posts.reset_index(drop=True))

    st.caption("Built with 🐍 Streamlit + 🪣 MinIO + 🔎 Altair + 🦆 DuckDB")

# --------------------------
# Entrypoint
# --------------------------

if __name__ == "__main__":
    show_layout()
