import streamlit as st
import pandas as pd
import numpy as np
import time
import matplotlib.pyplot as plt

# Function to simulate real-time data
def generate_live_data():
    return pd.DataFrame({
        "timestamp": pd.date_range(start=pd.Timestamp.now(), periods=10, freq="S"),
        "sentiment_score": np.random.uniform(-1, 1, 10)
    })

# Function to display live sentiment chart
def display_live_chart():
    st.subheader("📈 Live Sentiment Tracking")

    # Create a placeholder for the chart
    chart_placeholder = st.empty()

    while True:
        data = generate_live_data()
        
        fig, ax = plt.subplots()
        ax.plot(data["timestamp"], data["sentiment_score"], marker="o", linestyle="-", color="blue")
        ax.set_title("Real-Time Sentiment Score")
        ax.set_xlabel("Timestamp")
        ax.set_ylabel("Sentiment Score")
        ax.axhline(0, color="gray", linestyle="--")

        chart_placeholder.pyplot(fig)

        time.sleep(5)  # Update every 5 seconds

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
        time.sleep(3)  # Update every 3 seconds

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

    st.markdown("🔍 **Stay connected with live data and gain a competitive edge!**")
    st.markdown("[Back to Home](pages/landing.py)")

if __name__ == "__main__":
    show_layout()
