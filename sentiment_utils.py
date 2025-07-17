# sentiment_utils.py

from textblob import TextBlob
import re

def clean_text(text):
    if not isinstance(text, str):
        return ""
    # Remove URLs
    text = re.sub(r"http\S+|www\S+|https\S+", "", text)
    # Remove HTML tags
    text = re.sub(r"<.*?>", "", text)
    # Remove non-alphabetical characters
    text = re.sub(r"[^a-zA-Z\s]", "", text)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text

def analyze_sentiment(text):
    text = clean_text(text)
    if not text:
        return "neutral"  # fallback for empty or invalid input

    polarity = TextBlob(text).sentiment.polarity

    if polarity > 0.1:
        return "positive"
    elif polarity < -0.1:
        return "negative"
    else:
        return "neutral"
