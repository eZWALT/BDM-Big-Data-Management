import os
import time
from typing import Dict, List, Tuple

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
    st.subheader("📈 Cumulative Events Handled")

    # Create a placeholder for the chart
    raw_data: List[Tuple[pd.Timestamp, int]] = st.session_state.get("cumulative_events")
    if raw_data is None:
        # Create an empty dataframe
        data = pd.DataFrame(columns=["timestamp", "cumulative_events"])
    else:
        data = pd.DataFrame(raw_data, columns=["timestamp", "cumulative_events"])

    chart_placeholder = st.empty()

    fig, ax = plt.subplots()
    ax.plot(data["timestamp"], data["cumulative_events"], linestyle="-", color="blue")
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("Cumulative Events")
    chart_placeholder.pyplot(fig)
    st.write("**Note:** The chart updates every time a new event is received.")


# Function to simulate and display event log
def display_event_feed():
    st.subheader("📡 Real-Time Event Feed")

    text = ""
    messages = st.session_state.messages
    for message in messages:
        text += str(message) + "\n"
    stx.scrollableTextbox(text, height=500)


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
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "cumulative_events" not in st.session_state:
        st.session_state.cumulative_events = []

    st.set_page_config(page_title="Streaming - VibeRadar", layout="wide")
    tit, btn = st.columns((10, 1))
    with tit:
        st.title("📡 Real-Time Streaming Dashboard")
    with btn:
        if st.button("Refresh"):
            st.rerun()
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

    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    topics = admin.list_topics()
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: v.decode("utf-8"),
        auto_offset_reset="latest",
        enable_auto_commit=False,
    )
    consumer.subscribe(topics)

    logger.debug(f"[CONSUMER] Polling for new messages...")
    while True:
        try:
            new_messages = []
            messages = consumer.poll(timeout_ms=1000)
            if messages:
                for topic_partition, records in messages.items():
                    for record in records:
                        new_messages.append(record.value)
                        logger.debug(f"[CONSUMER] New message: {record.value}")

            st.session_state.cumulative_events.append(
                (
                    pd.Timestamp.now(),
                    (
                        st.session_state.cumulative_events[-1][1] + len(new_messages)
                        if st.session_state.cumulative_events
                        else len(new_messages)
                    ),
                )
            )
            # Keep only the last 2000 events
            st.session_state.cumulative_events = st.session_state.cumulative_events[-2000:]

            if new_messages:
                st.session_state.messages = st.session_state.messages + new_messages
                st.session_state.messages = st.session_state.messages[-20:]  # Keep only the last 20 messages
                st.rerun()

        except Exception as e:
            logger.error(f"[CONSUMER] Error while polling messages: {e}")
            time.sleep(1)


if __name__ == "__main__":
    show_layout()
