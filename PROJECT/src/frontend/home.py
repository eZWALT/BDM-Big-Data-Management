import streamlit as st
import pandas as pd

# ===----------------------------------------------------------------------===#
# Home                                                                        #
#                                                                             #
# This file represents the main entrypoint of the streamlit front-end         #
# and can be extended by the pages/ relative directory which allows for multi #
# pages frontend. The homepage is simple and merely informative of the product#
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
    In today’s fast-moving digital world, social networks shape consumer behavior like never before. 
    **VibeRadar** is a cutting-edge SaaS platform that empowers businesses with real-time insights into 
    **product launches, brand perception, and audience engagement** across global social platforms.
    With VibeRadar, companies can **optimize marketing strategies, improve decision-making, and stay ahead of trends**—all backed by a scalable, secure, and continuously evolving analytics platform.
    """
    )

    st.image("resources/logo2.jpeg", caption="", width=720)

    st.markdown("### 🚀 What We Offer:")
    st.write(
        """
    - **Impact Analytics**: Daily or weekly sentiment reports to track product reception.  
    - **Real-time Monitoring**: Instant feedback on consumer sentiment, engagement, and trends.  
    - **Deep Insights**: AI-driven analysis of demographics, view counts, and public discussions.  
    """
    )

    st.markdown(
        "🔍 **Explore our platform and start making data-driven decisions today!**"
    )

    # Navigation tutorial & Call to action
    st.header("How to Navigate")
    st.write(
        """
    - **Data Ingestion**: Interact and test out data ingestion for your product!
    - **Landing Zone**: Visualize and interact with the landing zone!
    """
    )

    st.header("Get Started!")
    st.write(
        """
    Click below to execute the 2 data pipelines and visualize the interactive analytics:
    """
    )
    st.page_link(
        "pages/ingestion.py", label="Data Ingestion", icon="🛠️", use_container_width=True
    )
    st.page_link(
        "pages/landing.py",
        label="Landing Zone (Bronze)",
        icon="🥉",
        use_container_width=True,
    )

    # Team credits
    st.header("Credits")
    st.write("### Team Members")
    st.write("- **Walter J. Troiani Vargas**")
    st.write("- **Mateja Zetazalo**")
    st.write("- **Marc Parcerisa**")


if __name__ == "__main__":
    show_layout()
