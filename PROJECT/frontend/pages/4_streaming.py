import time
from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import streamlit as st

STREAM_MANAGER_URL = "http://stream-manager:5000"


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
            st.success(f"✅ Pod {pod_id} started successfully!")
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
            st.success(f"✅ Pod {pod_id} stopped successfully!")
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
    st.subheader("📡 Real-Time Event Log")

    event_placeholder = st.empty()
    events = []

    for _ in range(20):  # Simulate 20 log entries
        event = f"🔔 {pd.Timestamp.now().strftime('%H:%M:%S')} - New sentiment event detected!"
        events.insert(0, event)  # Add new event to the top
        if len(events) > 5:  # Keep last 5 events
            events.pop()

        event_placeholder.write("\n".join(events))


def pod_component(pod: dict):
    """
    Display pod information and controls.
    """
    st.subheader(f"📦 Pod: {pod['id']}")

    # Display pod state
    st.write(f"**State:** {pod['state']}")
    st.write(f"**Producer:** {pod['producer']}")
    st.write(f"**Client:** {pod['client']}")
    st.write(f"**Query:** {pod['query']}")

    # Controls to start/stop the pod
    col1, col2 = st.columns(2)

    with col1:
        if st.button(f"▶️ Start Pod"):
            start_pod(pod["id"])

    with col2:
        if st.button(f"⏸️ Stop Pod"):
            stop_pod(pod["id"])


def management_control_component():
    st.subheader("▶️ Streaming Management")

    available_pods = get_pods()
    if len(available_pods) == 0:
        st.warning("⚠️ No pods found or unable to connect to Stream Manager.")
        return
    else:
        selected_pod = st.selectbox("📌 Select a Pod to Manage:", [pod["id"] for pod in available_pods])

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
        display_live_chart()

    with col2:
        display_event_feed()
        management_control_component()

    st.markdown("🔍 **Stay connected with live data and gain a competitive edge!**")
    st.markdown("[Back to Home](pages/landing.py)")


if __name__ == "__main__":
    show_layout()
