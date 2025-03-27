# pages/trusted.py

import streamlit as st

def trusted_page():
    st.set_page_config(page_title="Trusted - VibeRadar", layout="centered")
    st.title("🔒 Trusted Zone")
    st.header("Why VibeRadar is Trusted by Businesses")

    st.write(
        """
        Our platform is built on robust, secure, and scalable infrastructure that guarantees the privacy 
        and accuracy of your data. We use cutting-edge machine learning algorithms and data encryption 
        protocols to ensure the highest level of trust for our clients.
        """
    )
    st.markdown("### Key Trust Features:")
    st.write(
        """
        - **Data Privacy**: We prioritize the protection of your data with encryption at rest and in transit.
        - **Security**: VibeRadar follows best industry practices for securing consumer data and business insights.
        - **Transparency**: We provide complete transparency in data collection and analysis processes.
        """
    )

    st.markdown("🔍 **Explore more about our trusted platform and data security features!**")
    st.markdown("[Back to Home](pages/landing.py)")

if __name__ == "__main__":
    trusted_page()
