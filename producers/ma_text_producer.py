#!/usr/bin/env python3
"""
ma_text_producer.py — emits M&A mention counts.
Supports three modes:
1. dataset: Replays data from a CSV file.
2. api: Simulates fetching headlines and counts M&A keywords (Mock API).
3. synthetic: Generates random integers for stress testing.
"""
import os, time, json, csv, logging, random
from datetime import datetime, timezone
from kafka import KafkaProducer
import pandas as pd

BASE_DIR = os.path.expanduser(os.getenv("BASE_DIR","~/bda2"))
MODE = os.getenv("MODE","dataset").lower()
DATASET = os.path.expanduser(os.getenv("TEXT_DATA", f"{BASE_DIR}/data/fda8_text_dataset.csv"))
OUT_CSV = os.path.expanduser(f"{BASE_DIR}/data/ma_text_stream.csv")
TOPIC = "ma_text_stream"
TIME_MULTIPLIER = float(os.getenv("TIME_MULTIPLIER","1.0"))

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("ma_text_producer")

producer = KafkaProducer(
    bootstrap_servers=[os.getenv("KAFKA_BROKER","localhost:9092")],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=3
)

def extract_mentions(title: str):
    """
    Analyzes a text string and counts the occurrence of M&A related keywords.
    """
    text = title.lower()
    keywords = ["merger", "acquisition", "acquire", "buyout",
                "m&a", "takeover", "strategic partnership", "consolidation"]
    return sum(1 for kw in keywords if kw in text)

def emit(msg):
    """Sends a message to Kafka and appends a record to the local CSV stream file."""
    producer.send(TOPIC, msg); producer.flush()
    
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    with open(OUT_CSV, "a", newline="") as f:
        csv.writer(f).writerow([msg["company"], msg["timestamp"], msg.get("ma_mentions", 0), msg.get("source","")])

if MODE == "dataset":
    log.info("TEXT producer running in DATASET mode, file=%s", DATASET)
    if not os.path.exists(DATASET):
        log.error("Dataset not found at %s. Exiting.", DATASET)
        exit(1)

    df = pd.read_csv(DATASET).sort_values("timestamp")
    prev_ts = None

    for _, row in df.iterrows():
        ts = pd.to_datetime(row["timestamp"])
        if prev_ts is not None:
            delta = max(0.01, (ts - prev_ts).total_seconds() / 3600.0) # Scaled hours
            time.sleep(delta / TIME_MULTIPLIER)
        prev_ts = ts
        
        msg = {"company": row["company"], "timestamp": ts.to_pydatetime().astimezone(timezone.utc).isoformat(),
               "ma_mentions": int(row["ma_mentions"]), "source":"dataset"}
        emit(msg)
        log.info("TEXT dataset -> %s (mentions: %d)", row["company"], msg["ma_mentions"])
        
    log.info("TEXT dataset stream finished.")

elif MODE == "api":
    log.info("TEXT producer running in API mode (Simulated Headlines).")
    COMPANIES = os.getenv("COMPANIES", "AAPL,MSFT,AMZN,GOOGL,META").split(",")
    
    def generate_fake_title(company):
        """Generates a random headline to simulate API text retrieval."""
        templates = [
            f"{company} announces new strategic partnership with AI startup.",
            f"{company} to acquire smaller rival in billion dollar deal.",
            f"Rumors circulate about hostile takeover bid for {company}.",
            f"{company} reports strong Q3 earnings, stock rises.",
            f"{company} CEO discusses future roadmap and product launches.",
            f"Market analysts predict upcoming merger for {company}.",
            f"{company} faces antitrust lawsuit over recent buyout.",
            f"{company} expands cloud infrastructure in Europe."
        ]
        return random.choice(templates)

    try:
        while True:
            for c in COMPANIES:
                title = generate_fake_title(c)
                mentions = extract_mentions(title)
                
                msg = {"company": c, "timestamp": datetime.now(timezone.utc).isoformat(),
                       "ma_mentions": mentions, "source": "simulated_api"}
                emit(msg)
                log.info("TEXT api -> %s: '%s' (mentions: %d)", c, title, mentions)
                
                time.sleep(1.0 / TIME_MULTIPLIER)
    except KeyboardInterrupt:
        log.info("Stopping text producer.")

else:
    log.info("TEXT producer running in SYNTHETIC mode.")
    COMPANIES = ["AAPL", "MSFT", "AMZN", "GOOGL", "META"]
    try:
        while True:
            for c in COMPANIES:
                mentions = random.randint(0, 8)
                
                msg = {"company": c, "timestamp": datetime.now(timezone.utc).isoformat(),
                       "ma_mentions": mentions, "source": "synthetic"}
                emit(msg)
                log.info("TEXT synthetic -> %s (mentions: %d)", c, mentions)
                
                time.sleep(1.0 / TIME_MULTIPLIER)
    except KeyboardInterrupt:
        log.info("Stopping text producer.")