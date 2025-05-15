from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import torch.nn.functional as F
from typing import List
import pandas as pd


from src.utils.task import Task


class SentimentAnalysisTask(Task):
    def __init__(self, texts: List[str], model_name: str = "cardiffnlp/twitter-roberta-base-emotion"):
        super().__init__()
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

if __name__ == "__main__":
    # Sample text data
    texts = [
        "I am thrilled to be part of this journey!",
        "This is the worst experience I've ever had.",
        "I'm not sure how I feel about this.",
        "Today is just another boring day.",
        "Feeling grateful and optimistic."
    ]

    # Choose the model: multi-emotion (emotions like joy, anger, etc.)
    model_name = "cardiffnlp/twitter-roberta-base-emotion"

    # Initialize and run the task
    task = SentimentAnalysisTask(texts=texts, model_name=model_name)
    df: pd.DataFrame = task.execute()

    # Print results
    pd.set_option('display.max_columns', None)
    print("\n=== Sentiment Analysis Results ===")
    print(df)
