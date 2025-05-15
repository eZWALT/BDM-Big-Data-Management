import os
import streamlit as st
import pandas as pd
import duckdb
import random
from typing import List
from datetime import datetime, timedelta
import torch
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import altair as alt

# -------------------------------
# Sentiment Task
# -------------------------------
class SentimentAnalysisTask:
    def __init__(self, texts: List[str], model_name: str = "cardiffnlp/twitter-roberta-base-emotion"):
        self.texts = texts
        self.model_name = model_name

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


# -------------------------------
# Data Loaders
# -------------------------------

def generate_fake_sentiment_data(n=100, product="ProductX", company="CompanyY"):
    # This is not dynamic! We need it to be dynamic depending on the model emotions
    emotions = ['joy', 'sadness', 'optimism', 'anger']
    base_date = datetime.today()
    data = []

    for i in range(n):
        score = random.uniform(-1, 1)
        label = random.choice(emotions)
        data.append({
            "timestamp": (base_date - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d'),
            "text": f"{label.title()} comment {i}",
            "score": score,
            "label": label,
            "product": product,
            "company": company
        })

    return pd.DataFrame(data)

def load_sentiment_from_duckdb(path='data/sentiment.duckdb', product=None, company=None) -> pd.DataFrame:
    con = duckdb.connect(path)
    query = "SELECT timestamp, text, score, label, product, company FROM sentiment"
    if product and company:
        query += f" WHERE product='{product}' AND company='{company}'"
    df = con.execute(query).df()
    con.close()
    return df

def load_sentiment_data(environment, product=None, company=None):
    if environment == "production":
        return load_sentiment_from_duckdb(product=product, company=company)
    else:
        return generate_fake_sentiment_data(product=product, company=company)


# -------------------------------
# UI + Plots
# -------------------------------

def render_header():
    st.set_page_config(page_title="Sentiment Analysis Dashboard", layout="wide")
    st.title("💬 Sentiment Analysis Dashboard")
    with st.expander(label="Sentiment Analysis Description:")
    st.markdown("""
        This dashboard displays **emotion-based sentiment analysis** for different product–company pairs.
        The analysis is based on transformer models that detect emotional content (like joy, anger, etc.).
    """)

def plot_sentiment_over_time(df: pd.DataFrame):
    chart = alt.Chart(df).mark_line(point=True).encode(
        x='timestamp:T',
        y='score:Q',
        color='label:N'
    ).properties(width=700, height=400)
    st.altair_chart(chart, use_container_width=True)

def plot_label_distribution(df: pd.DataFrame):
    chart = alt.Chart(df).mark_bar().encode(
        x='label:N',
        y='count():Q',
        color='label:N'
    ).properties(width=400, height=300)
    st.altair_chart(chart, use_container_width=True)

def render_dev_results(product: str, company: str):
    # Use synthetic data
    df = generate_fake_sentiment_data(n=100, product=product, company=company)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    st.subheader("📈 Sentiment Over Time")
    plot_sentiment_over_time(df)

    st.subheader("📊 Emotion Distribution")
    plot_label_distribution(df)

    st.subheader("🔍 Recent Comments")
    st.dataframe(df[['timestamp', 'label', 'text']].sort_values(by="timestamp", ascending=False).head(10))

    # (Optional) Small static prediction section using HuggingFace model
    st.subheader("🧪 Example Model Predictions")
    example_texts = [
        "Absolutely loved this product!",
        "Terrible experience, would not recommend.",
        "It was okay, nothing special.",
    ]
    task = SentimentAnalysisTask(example_texts)
    df_pred = task.execute()
    st.dataframe(df_pred)



def render_main_dashboard(df: pd.DataFrame):
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    st.subheader("📈 Sentiment Over Time")
    plot_sentiment_over_time(df)

    st.subheader("📊 Emotion Distribution")
    plot_label_distribution(df)

    st.subheader("🔍 Recent Comments")
    st.dataframe(df[['timestamp', 'label', 'text']].sort_values(by="timestamp", ascending=False).head(10))


# -------------------------------
# Main App
# -------------------------------

def main():
    render_header()

    # Read environment
    environment = os.getenv("ENVIRONMENT_TYPE", "development")

    # Sidebar controls
    st.sidebar.title("Configuration")
    product = st.sidebar.selectbox("Select Product", ["ProductX", "ProductY"])
    company = st.sidebar.selectbox("Select Company", ["CompanyA", "CompanyB"])

    df = load_sentiment_data(environment, product, company)

    if environment == "development":
        render_dev_results(product, company)
    else:
        render_main_dashboard(df)

    st.markdown("---")
    st.markdown("Built with 🐍 DuckDB + 🤗 Transformers + 📊 Streamlit")


if __name__ == "__main__":
    main()
