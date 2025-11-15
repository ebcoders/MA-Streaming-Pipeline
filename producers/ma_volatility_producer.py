#!/usr/bin/env python3
"""
ma_volatility_producer.py — emits volatility numbers.
Supports dataset or synthetic.
"""
import os, time, json, csv, logging, random
from datetime import datetime, timezone
from kafka import KafkaProducer
import pandas as pd

BASE_DIR = os.path.expanduser(os.getenv("BASE_DIR","~/bda2"))
MODE = os.getenv("MODE","dataset").lower()
DATASET = os.path.expanduser(os.getenv("VOL_DATA", f"{BASE_DIR}/data/fda8_volatility_dataset.csv"))
OUT_CSV = os.path.expanduser(f"{BASE_DIR}/data/ma_volatility_stream.csv")
TOPIC = "ma_volatility_stream"
TIME_MULTIPLIER = float(os.getenv("TIME_MULTIPLIER","1.0"))

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("ma_volatility_producer")
producer = KafkaProducer(bootstrap_servers=[os.getenv("KAFKA_BROKER","localhost:9092")],
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))

def emit(msg):
    producer.send(TOPIC, msg); producer.flush()
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    with open(OUT_CSV,"a",newline="") as f:
        csv.writer(f).writerow([msg["company"], msg["timestamp"], msg.get("volatility"), msg.get("source","")])

if MODE == "dataset":
    log.info("VOL producer running in DATASET mode, file=%s", DATASET)
    df = pd.read_csv(DATASET).sort_values("timestamp")
    prev_ts = None
    for _, row in df.iterrows():
        ts = pd.to_datetime(row["timestamp"])
        if prev_ts is not None:
            delta = max(0.01, (ts - prev_ts).total_seconds()/3600.0)
            time.sleep(delta / TIME_MULTIPLIER)
        prev_ts = ts
        msg = {"company": row["company"], "timestamp": ts.to_pydatetime().astimezone(timezone.utc).isoformat(),
               "volatility": float(row["volatility"]), "source":"dataset"}
        emit(msg); log.info("VOL dataset -> %s", row["company"])
    log.info("VOL dataset stream finished.")
else:
    log.info("VOL producer running in SYNTHETIC mode.")
    COMPANIES = ["AAPL","MSFT","AMZN","GOOGL","META"]
    try:
        while True:
            for c in COMPANIES:
                vol = round(random.uniform(5.0,40.0),3)
                msg = {"company": c, "timestamp": datetime.now(timezone.utc).isoformat(),
                       "volatility": vol, "source":"synthetic"}
                emit(msg); log.info("VOL synthetic -> %s %s", c, vol)
                time.sleep(1.0 / TIME_MULTIPLIER)
    except KeyboardInterrupt:
        log.info("Stopped.")
