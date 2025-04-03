import streamlit as st
import pandas as pd

# ===----------------------------------------------------------------------===#
# Home                                                                        #
#                                                                             #
# This file represents the main entry point of the Streamlit front-end.       #
# Additional pages are located under the 'pages/' directory.                  #
#                                                                             #
# Author: Walter J.T.V                                                        #
# ===----------------------------------------------------------------------===#

def show_layout():
    # Set page configuration
    st.set_page_config(page_title="Home - VibeRadar", layout="centered")

    # Title and description
    st.title("🗣️🔥🗿 VibeRadar 🗣️🔥🗿")
    st.header("Real-time Consumer Sentiment & Impact Analysis")

    st.write(
        """
    In today’s digital era, social media shapes consumer behavior like never before.  
    **VibeRadar** is an advanced SaaS platform that provides real-time insights into **product launches, brand perception, and audience engagement** across global social platforms.  
    With **VibeRadar**, businesses can **optimize marketing strategies, enhance decision-making, and stay ahead of emerging trends**—all with a **scalable, secure, and continuously evolving** analytics solution.
    """
    )

    # Logo image
    #st.image("resources/logo2.jpeg", caption="", width=720)

    st.markdown("### What We Offer:")
    st.write(
        """
    - **📊 Impact Analytics** → Daily or weekly sentiment reports to track product reception.  
    - **📡 Real-time Monitoring** → Instant insights on consumer sentiment, engagement, and trends.  
    - **🔎 Deep Insights** → AI-driven analysis of demographics, view counts, and public discussions.  
    """
    )

    st.markdown("💡 **Leverage data-driven insights to make smarter business decisions today!**")

    # Navigation tutorial & Call to action
    st.header("Get Started!")
    st.write(
        """
    - **🥉 Landing Zone** → View and interact with raw ingested data.  
    - **🥈 Trusted Zone** → Explore transformed and validated data.  
    - **🏆 Exploitation Zone** → Access high-quality, analytics-ready datasets.  
    - **📡 Streaming** → Monitor real-time consumer sentiment and brand impact.  
    """
    )

    
    # Team credits
    st.header("👥 Team")
    st.write("- **Walter J. Troiani Vargas**")
    st.write("- **Mateja Zetazalo**")
    st.write("- **Marc Parcerisa**")

if __name__ == "__main__":
    show_layout()
