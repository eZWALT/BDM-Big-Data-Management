import pandas as pd 

def subset_sentiment(df: pd.DataFrame):
    return df[[
        "likes", "views", "shares", "dislikes", "reposts", "quotes", "replies", #stats
        "title", "description", "author_uri", "uri", "source", "transaction_timestamp", #metadata
        "text_sentiment", "title_sentiment", "description_sentiment", "sentiment" #sentiment
    ]]
    
def subset_dashboard(df: pd.DataFrame):
    return df[[
        "likes", "views", "shares", "dislikes", "reposts", "quotes", "replies", #stats
        "title", "description", "author_uri", "uri", "source", "transaction_timestamp", #metadata
        "sentiment" #sentiment
    ]]
    
def add_sentiment_score(df: pd.DataFrame):
    mapping = {"positive": 1, "neutral": 0, "negative": -1, "unknown": None}
    df["sentiment_score"] = df["sentiment"].map(mapping)
    return df
