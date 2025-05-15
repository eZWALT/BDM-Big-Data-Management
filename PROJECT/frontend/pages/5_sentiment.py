import os
import streamlit as st
import pandas as pd
import duckdb
from datetime import datetime, timedelta
import random
from typing import List
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import torch.nn.functional as F

# --------------------------
# MODEL CLASS
# --------------------------

class SentimentAnalysisTask:
    def __init__(self, texts: List[str], model_name: str = "cardiffnlp/twitter-roberta-base-emotion"):
        self.texts = texts
        self.model_name = model_name
        self.tokenizer = None
        self.model = None
        self.labels = None

    def setup(self):
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
        self.labels = self.model.config.id2label

    def execute(self) -> pd.DataFrame:
        self.setup()
        results = []

        for text in self.texts:
            inputs = self.tokenizer(text, return_tensors="pt", truncation=True)
            outputs = self.model(**inputs)
            probs = F.softmax(outputs.logits, dim=-1)
            result = {self.labels[i]: float(p) for i, p in enumerate(probs[0])}
            result["text"] = text
            result["predicted_label"] = self.labels[int(torch.argmax(probs))]
            results.append(result)

        return pd.DataFrame(results)


# --------------------------
# DATA LOADING
# --------------------------

def generate_fake_sentiment_data(n=100) -> pd.DataFrame:
    companies = {
        "OpenAI": ["ChatGPT", "DALL·E"],
        "Google": ["Gemini", "Bard"],
        "Meta": ["Threads", "Llama"]
    }
    platforms = ["Twitter", "YouTube", "Bluesky"]
    emotions = {
        "joy": [
            "Absolutely love this!", "Best thing ever!", "Super impressed!", "It made my day!"
        ],
        "optimism": [
            "Hope this keeps improving.", "Looking forward to what's next.", "This has potential.", "Very promising!"
        ],
        "sadness": [
            "Really disappointed...", "Let down by this.", "Expected more.", "Not what I hoped for."
        ],
        "anger": [
            "This is awful!", "Really mad about this.", "Total disaster.", "Never using this again!"
        ]
    }

    base_date = datetime.today()
    data = []

    for i in range(n):
        company = random.choice(list(companies.keys()))
        product = random.choice(companies[company])
        platform = random.choice(platforms)
        timestamp = base_date - timedelta(days=random.randint(0, 30))

        emotion = random.choice(list(emotions.keys()))
        emotion_text = random.choice(emotions[emotion])
        text = f"{emotion_text} It's about {product} by {company}."

        # For development mode sentiment mock
        score = {
            "joy": 0.9,
            "optimism": 0.6,
            "sadness": -0.6,
            "anger": -0.9
        }[emotion]

        data.append({
            "timestamp": timestamp,
            "company": company,
            "product": product,
            "platform": platform,
            "text": text,
            "score": score,
            "label": emotion,
            "num_likes": random.randint(0, 300),
            "num_comments": random.randint(0, 50)
        })

    return pd.DataFrame(data)

def load_sentiment_from_duckdb(path='data/sentiment.duckdb') -> pd.DataFrame:
    con = duckdb.connect(path)
    df = con.execute("SELECT timestamp, company, product, platform, text, score, label FROM sentiment").df()
    con.close()
    return df

def load_sentiment_data(environment: str) -> pd.DataFrame:
    return load_sentiment_from_duckdb() if environment == "production" else generate_fake_sentiment_data()


# --------------------------
# VISUALIZATION
# --------------------------

def plot_sentiment_over_time(df: pd.DataFrame):
    import altair as alt
    st.markdown("Shows how average sentiment changes over time.")
    chart = alt.Chart(df).mark_line().encode(
        x='timestamp:T',
        y='score:Q',
        color='platform:N'
    ).properties(height=300)
    st.altair_chart(chart, use_container_width=True)

def plot_label_distribution(df: pd.DataFrame, label_col="label"):
    import altair as alt
    label_df = df[label_col].value_counts().reset_index()
    label_df.columns = ["label", "count"]
    chart = alt.Chart(label_df).mark_bar().encode(
        x='label:N',
        y='count:Q',
        color='label:N'
    ).properties(height=300)
    st.altair_chart(chart, use_container_width=True)


# --------------------------
# MAIN LAYOUT
# --------------------------

def show_layout():
    st.set_page_config(page_title="Sentiment", layout="wide")
    st.title("💬 Sentiment Analysis Dashboard")

    environment = os.getenv("ENVIRONMENT_TYPE", "development")
    df = load_sentiment_data(environment)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Sidebar filters
    st.sidebar.header("🔍 Filter Data")
    selected_company = st.sidebar.selectbox("Select Company", sorted(df["company"].unique()))
    df = df[df["company"] == selected_company]

    selected_product = st.sidebar.selectbox("Select Product", sorted(df["product"].unique()))
    df = df[df["product"] == selected_product]

    selected_platforms = st.sidebar.multiselect(
        "Select Platform(s)", sorted(df["platform"].unique()), default=sorted(df["platform"].unique())
    )
    df = df[df["platform"].isin(selected_platforms)]

    # About section
    with st.expander(f"About this Dashboard"):
        st.markdown("""
        This dashboard helps you analyze **sentiment and emotion trends** in user posts related to a specific product/company.

        **Features:**
        - 📈 Sentiment over time (average score)
        - 📊 Emotion distribution (labels)
        - 📝 Recent post sentiment samples
        - 🧪 Live model prediction (in dev mode only)
        """)

    # Plots
    st.subheader("📈 Sentiment Score Over Time")
    plot_sentiment_over_time(df)

    st.subheader("📊 Emotion Label Distribution")
    if environment == "development":
        sample_texts = df["text"].sample(min(10, len(df)), random_state=42).tolist()
        task = SentimentAnalysisTask(sample_texts)
        model_df = task.execute()
        plot_label_distribution(model_df, label_col="predicted_label")
    else:
        plot_label_distribution(df, label_col="label")

    st.subheader("🔍 Recent Comments")
    st.dataframe(df.sort_values(by="timestamp", ascending=False)[
        ["timestamp", "label", "text"]
    ].reset_index(drop=True).head(10))

    if environment == "development":
        st.subheader("🧪 Example Model Predictions")
        st.dataframe(model_df[["text", "predicted_label"]])

    st.markdown("---")
    st.caption("Built with 🐍 Streamlit + 🤗 Transformers + 🦆 DuckDB")


# --------------------------
# ENTRYPOINT
# --------------------------

if __name__ == "__main__":
    show_layout()
