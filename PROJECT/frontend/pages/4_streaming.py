import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import streamlit as st
import streamlit_scrollable_textbox as stx
from kafka import KafkaAdminClient, KafkaConsumer
from loguru import logger

STREAM_MANAGER_URL = os.getenv("STREAM_MANAGER_URL", "http://localhost:5000")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
topics = admin.list_topics()
consumer = KafkaConsumer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=False,
)
consumer.subscribe(topics)
for topic_partition in consumer.assignment():
    # Seek to the beginning (0) for each partition
    consumer.seek_to_beginning(topic_partition)


def get_pods() -> List[Dict]:
    """
    Get the list of pods from the stream manager.
    """
    try:
        response = requests.get(f"{STREAM_MANAGER_URL}/pods")
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Failed to fetch pods: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        st.error(f"Error connecting to Stream Manager: {e}")
        return None


def start_pod(pod_id: str) -> None:
    """
    Start a specific pod by ID.
    """
    try:
        response = requests.post(f"{STREAM_MANAGER_URL}/pods/{pod_id}/start")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Error stopping pod {pod_id}: {e}")
        return None


def stop_pod(pod_id: str) -> None:
    """
    Stop a specific pod by ID.
    """
    try:
        response = requests.post(f"{STREAM_MANAGER_URL}/pods/{pod_id}/stop")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Error stopping pod {pod_id}: {e}")
        return None


# Function to display live sentiment chart
def display_live_chart():
    st.subheader("📈 Live Sentiment Tracking")

    # Create a placeholder for the chart
    chart_placeholder = st.empty()

    data = pd.DataFrame(
        {
            "timestamp": pd.date_range(start=pd.Timestamp.now(), periods=10, freq="S"),
            "sentiment_score": np.random.uniform(-1, 1, 10),
        }
    )

    fig, ax = plt.subplots()
    ax.plot(data["timestamp"], data["sentiment_score"], marker="o", linestyle="-", color="blue")
    ax.set_title("Real-Time Sentiment Score")
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("Sentiment Score")
    ax.axhline(0, color="gray", linestyle="--")

    chart_placeholder.pyplot(fig)


# Function to simulate and display event log
def display_event_feed():
    st.subheader("📡 Latest Events")

    text = ""
    messages = consumer.poll(timeout_ms=1)
    if messages:
        for topic_partition, records in messages.items():
            for record in records:
                text += str(record) + "\n"
    stx.scrollableTextbox(text, height=400)


def management_control_component():
    st.subheader("▶️ Streaming Management")

    available_pods = get_pods()
    if len(available_pods) == 0:
        st.warning("⚠️ No pods found or unable to connect to Stream Manager.")
        return
    else:
        col_distr = (3, 1, 2, 2, 1, 2)
        columns = st.columns(col_distr)
        fields = ["ID", "Client", "Query", "Producer", "State", "Controls"]
        keys = ["id", "client", "query", "producer", "state_text"]
        for col, field in zip(columns, fields):
            col.write(f"**{field}**")

        for pod in available_pods:
            pod["state_text"] = (
                ":green[Running]"
                if pod["state"] == "running"
                else (
                    ":red[Failed]"
                    if pod["state"] == "failed"
                    else ":blue[Idle]" if pod["state"] == "idle" else f":orange[{pod['state'].capitalize()}]"
                )
            )  # Add color to the state
            columns = st.columns(col_distr)
            for col, key in zip(columns, keys):
                col.write(f"{pod[key]}")
            # Display pod controls
            with columns[-1]:
                if pod["state"] == "running":
                    btn, status = st.columns(2)
                    with btn:
                        if st.button("⏸️ Stop", key=f"stop_{pod['id']}"):
                            with status:
                                with st.spinner(""):
                                    stop_pod(pod["id"])
                                    time.sleep(1)
                                    st.rerun()
                else:
                    btn, status = st.columns(2)
                    with btn:
                        if st.button("▶️ Start", key=f"start_{pod['id']}"):
                            with status:
                                with st.spinner(""):
                                    start_pod(pod["id"])
                                    time.sleep(1)
                                    st.rerun()


def show_layout():
    st.set_page_config(page_title="Streaming - VibeRadar", layout="wide")
    st.title("📡 Real-Time Streaming Data")
    st.header("Stay Ahead with Real-Time Insights")

    st.write(
        """
        VibeRadar provides real-time tracking of consumer sentiment, brand reception, and engagement metrics.
        Our streaming service ensures you can act on data as it's happening, keeping you ahead of the curve.
        """
    )

    with st.expander(label="View Streaming Zone Description"):
        st.markdown("### 🎥 Streaming Features:")
        st.write(
            """
            - **Live Analytics**: Track the latest trends and consumer behavior in real time.
            - **Real-Time Alerts**: Set up alerts for significant sentiment changes or engagement spikes.
            - **Interactive Dashboards**: Visualize streaming data through interactive, customizable dashboards.
            """
        )

    management_control_component()
    # Live chart & Event feed layout
    col1, col2 = st.columns(2)
    with col1:
        display_event_feed()

    with col2:
        display_live_chart()


if __name__ == "__main__":
    show_layout()
