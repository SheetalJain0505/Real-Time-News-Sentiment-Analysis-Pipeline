# etl_pipeline.py

import requests
import sqlite3
import pandas as pd
from config import API_KEY, API_URL, TOPIC, LANG, COUNTRY
from sentiment_utils import analyze_sentiment

def log_message(message):
    with open("logs/pipeline_log.txt", "a") as log:
        from datetime import datetime
        log.write(f"[{datetime.now()}] {message}\n")

def extract_news():
    params = {
        "apikey": API_KEY,
        "q": TOPIC,
        "language": LANG,
        "country": COUNTRY
    }

    response = requests.get(API_URL, params=params)
    data = response.json()

    articles = data.get("results", [])
    if not articles:
        log_message("No articles found in response.")
        return pd.DataFrame()

    df = pd.DataFrame(articles)
    log_message(f"Extracted {len(df)} articles.")
    return df[['title', 'description', 'pubDate', 'link']]


def transform_news(df):
    if df.empty:
        log_message("No data to transform.")
        return df

    # Combine title + description
    df["full_text"] = df["title"].fillna('') + " " + df["description"].fillna('')

    # Apply sentiment analysis
    df["sentiment"] = df["full_text"].apply(analyze_sentiment)

    log_message("Sentiment analysis completed.")
    return df

def load_to_db(df, db_name="news_data.db", table_name="news_sentiment"):
    if df.empty:
        log_message("No data to load into database.")
        return

    # Keep only useful columns for storage
    df_to_store = df[['title', 'description', 'pubDate', 'link', 'sentiment']].copy()

    try:
        conn = sqlite3.connect(db_name)
        df_to_store.to_sql(table_name, conn, if_exists='append', index=False)
        conn.close()
        log_message(f"Loaded {len(df_to_store)} records to DB table '{table_name}'.")
    except Exception as e:
        log_message(f"DB load failed: {str(e)}")



if __name__ == "__main__":
    df_raw = extract_news()
    print("\n🔹 Raw Data Sample:")
    print(df_raw.head())

    df_processed = transform_news(df_raw)
    print("\n🔹 Transformed Data with Sentiment:")
    print(df_processed[['title', 'sentiment']].head())
    print(f"✅ Records to load: {len(df_processed)}")

    load_to_db(df_processed)
