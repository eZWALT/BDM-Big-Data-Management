# pages/streaming.py

import streamlit as st

def streaming_page():
    st.set_page_config(page_title="Streaming - VibeRadar", layout="wide")
    st.title("📡 Real-Time Streaming Data")
    st.header("Stay Ahead with Real-Time Insights")

    st.write(
        """
        VibeRadar provides real-time tracking of consumer sentiment, brand reception, and engagement metrics.
        Our streaming service ensures you can act on data as it's happening, keeping you ahead of the curve.
        """
    )

    st.markdown("### Streaming Features:")
    st.write(
        """
        - **Live Analytics**: Track the latest trends and consumer behavior in real time.
        - **Real-Time Alerts**: Set up alerts for significant sentiment changes or engagement spikes.
        - **Interactive Dashboards**: Visualize streaming data through interactive, customizable dashboards.
        """
    )

    st.markdown("🔍 **Stay connected with live data and gain a competitive edge!**")
    st.markdown("[Back to Home](pages/landing.py)")

if __name__ == "__main__":
    streaming_page()
