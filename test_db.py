import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect("news_data.db")

# 🔍 Print all tables
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("📋 Tables in DB:", tables)

# ✅ Check if 'news_sentiment' table exists and print its contents
try:
    df = pd.read_sql("SELECT * FROM news_sentiment", conn)
    print("\n✅ Sample data from news_sentiment table:")
    print(df.head())
except Exception as e:
    print("❌ Error reading from table:", e)

conn.close()
