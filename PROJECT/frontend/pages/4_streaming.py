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
        if response.status_code == 200:
            st.success(f"✅ Pod {pod_id} scheduled to start!")
        else:
            st.error(f"❌ Failed to start pod {pod_id}: {response.status_code}")
    except requests.exceptions.RequestException as e:
        st.error(f"Error starting pod: {e}")


def stop_pod(pod_id: str) -> None:
    """
    Stop a specific pod by ID.
    """
    try:
        response = requests.post(f"{STREAM_MANAGER_URL}/pods/{pod_id}/stop")
        if response.status_code == 200:
            st.success(f"✅ Stopping pod {pod_id}!")
        else:
            st.error(f"❌ Failed to stop pod {pod_id}: {response.status_code}")
    except requests.exceptions.RequestException as e:
        st.error(f"Error stopping pod: {e}")


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


def pod_component(pod: dict):
    """
    Display pod information and controls.
    """
    st.markdown(f"#### 📦 Pod: {pod['client']} - {pod['query']} - {pod['producer']}")

    state_colored_text = (
        ":green[Running]"
        if pod["state"] == "running"
        else (
            ":red[Failed]"
            if pod["state"] == "failed"
            else ":blue[Idle]" if pod["state"] == "idle" else f":orange[{pod['state'].capitalize()}]"
        )
    )

    # Display pod state
    st.write(f"**ID:** {pod['id']}")
    st.write(f"**State:** {state_colored_text}")
    st.write(f"**Producer:** {pod['producer']}")
    st.write(f"**Client:** {pod['client']}")
    st.write(f"**Query:** {pod['query']}")

    # Controls to start/stop the pod
    col1, col2 = st.columns(2)

    with col1:
        if st.button(f"▶️ Start Pod", use_container_width=True):
            start_pod(pod["id"])
            time.sleep(1)  # Wait for a second to allow the state to update
            st.rerun()

    with col2:
        if st.button(f"⏸️ Stop Pod", use_container_width=True):
            stop_pod(pod["id"])
            time.sleep(1)  # Wait for a second to allow the state to update
            st.rerun()


def management_control_component():
    st.subheader("▶️ Streaming Management")

    available_pods = get_pods()
    pods_map = {pod["id"]: pod for pod in available_pods} if available_pods else {}
    if len(available_pods) == 0:
        st.warning("⚠️ No pods found or unable to connect to Stream Manager.")
        return
    else:
        selected_pod = st.selectbox(
            "📌 Select a Pod to Manage:",
            [pod["id"] for pod in available_pods],
            format_func=lambda id: f"{pods_map[id]['client']} - {pods_map[id]['query']} - {pods_map[id]['producer']}",
            placeholder="Select a pod",
        )

        # Display selected pod information and controls
        selected_pod_info = next((pod for pod in available_pods if pod["id"] == selected_pod), None)
        if selected_pod_info:
            pod_component(selected_pod_info)


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

    # Live chart & Event feed layout
    col1, col2 = st.columns(2)
    with col1:
        display_event_feed()

    with col2:
        management_control_component()


if __name__ == "__main__":
    show_layout()
