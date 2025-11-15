#!/usr/bin/env python3
"""
generate_dataset.py — creates a synthetic dataset for the pipeline.

This script generates FIVE CSV files:
1. fda8_full_dataset.csv: A single file with all features and labels, used for model training.
2. fda8_financial_dataset.csv: Data for the financial producer.
3. fda8_text_dataset.csv: Data for the text producer.
4. fda8_sentiment_dataset.csv: Data for the sentiment producer (includes labels for online learning).
5. fda8_volatility_dataset.csv: Data for the volatility producer.
"""
import os
import csv
import random
from datetime import datetime, timedelta
import pandas as pd

# --- Configuration ---
BASE_DIR = os.path.expanduser(os.getenv("BASE_DIR", "~/bda2"))
DATA_DIR = os.path.join(BASE_DIR, "data")
COMPANIES = ["AAPL", "MSFT", "AMZN", "GOOGL", "META"]
start = datetime(2025, 9, 14, 0, 0, 0)


print("Generating 5000 synthetic data points in memory...")
rows = []
headers = [
    "company", "timestamp", "liquidity_ratio", "cash_to_assets", "leverage_ratio",
    "profit_margin", "sentiment_score", "ma_mentions", "volatility", "ma_activity"
]

for i in range(1000):
    for c in COMPANIES:
        ts = start + timedelta(minutes=30 * i)
        liquidity = round(random.uniform(0.5, 3.0), 3)
        cash_to_assets = round(random.uniform(0.1, 0.6), 3)
        leverage = round(random.uniform(0.2, 0.9), 3)
        profit = round(random.uniform(-0.2, 0.4), 3)
        sentiment = round(random.uniform(-0.6, 0.8), 3)
        ma_mentions = random.randint(0, 6)
        volatility = round(random.uniform(4.0, 40.0), 3)
        
        label = 1 if (ma_mentions >= 2 and volatility > 20) or sentiment > 0.5 else 0
        
        rows.append([
            c, ts.isoformat(), liquidity, cash_to_assets, leverage, profit,
            sentiment, ma_mentions, volatility, label
        ])

print("Converting to DataFrame for splitting...")
df = pd.DataFrame(rows, columns=headers)

os.makedirs(DATA_DIR, exist_ok=True)

full_dataset_path = os.path.join(DATA_DIR, "fda8_full_dataset.csv")
df.to_csv(full_dataset_path, index=False)
print(f" Wrote full training dataset to: {full_dataset_path}")

financial_df = df[["company", "timestamp", "liquidity_ratio", "cash_to_assets", "leverage_ratio", "profit_margin"]]
financial_dataset_path = os.path.join(DATA_DIR, "fda8_financial_dataset.csv")
financial_df.to_csv(financial_dataset_path, index=False)
print(f" Wrote financial producer dataset to: {financial_dataset_path}")

text_df = df[["company", "timestamp", "ma_mentions"]]
text_dataset_path = os.path.join(DATA_DIR, "fda8_text_dataset.csv")
text_df.to_csv(text_dataset_path, index=False)
print(f" Wrote text producer dataset to: {text_dataset_path}")

sentiment_df = df[["company", "timestamp", "sentiment_score", "ma_activity"]]
sentiment_dataset_path = os.path.join(DATA_DIR, "fda8_sentiment_dataset.csv")
sentiment_df.to_csv(sentiment_dataset_path, index=False)
print(f" Wrote sentiment producer dataset to: {sentiment_dataset_path}")

volatility_df = df[["company", "timestamp", "volatility"]]
volatility_dataset_path = os.path.join(DATA_DIR, "fda8_volatility_dataset.csv")
volatility_df.to_csv(volatility_dataset_path, index=False)
print(f" Wrote volatility producer dataset to: {volatility_dataset_path}")

print("\nAll necessary dataset files have been generated successfully.")