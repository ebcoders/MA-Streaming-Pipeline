#!/usr/bin/env python3
"""
drift_consumer.py — listens to feature streams, runs ADWIN per (company,feature).
Publishes drift_alerts to Kafka and logs them to a local CSV for the dashboard.
"""
import os, json, logging, time, csv
from kafka import KafkaConsumer, KafkaProducer
from river import drift

# --- Configuration ---
BASE_DIR = os.path.expanduser(os.getenv("BASE_DIR","~/bda2"))
KAFKA_BROKER = os.getenv("KAFKA_BROKER","localhost:9092")
TOPICS = ["ma_text_stream","ma_financial_stream","ma_sentiment_stream","ma_volatility_stream"]
PRODUCE_TOPIC = "drift_alerts"
DRIFT_CSV = os.path.expanduser(f"{BASE_DIR}/data/drift_alerts.csv") # <-- ADDED: Path for the new CSV

# --- Setup ---
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("drift_consumer")

consumer = KafkaConsumer(*TOPICS, bootstrap_servers=[KAFKA_BROKER],
                         value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                         auto_offset_reset="earliest", consumer_timeout_ms=1000)

producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER],
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))

windows = {}

def get_adwin(company, feature):
    key = f"{company}::{feature}"
    if key not in windows:
        windows[key] = drift.ADWIN(delta=0.002)
    return windows[key]

if not os.path.exists(DRIFT_CSV):
    os.makedirs(os.path.dirname(DRIFT_CSV), exist_ok=True)
    with open(DRIFT_CSV, "w", newline="") as f:
        csv.writer(f).writerow(["timestamp", "company", "feature", "detector", "detail"])

# --- Main Loop ---
while True:
    for msg in consumer:
        t = msg.topic; p = msg.value
        company = p.get("company")
        if not company: continue
        
        feature = None
        val = None

        if t == "ma_text_stream":
            val = float(p.get("ma_mentions",0)); feature = "ma_mentions"
        elif t == "ma_financial_stream":
            feats = p.get("features",{})
            for f,v in feats.items():
                try: v=float(v)
                except: continue
                aw = get_adwin(company,f)
                aw.update(v)
                if aw.drift_detected:
                    detail = f"ADWIN drift detected in {f}"
                    out = {"timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "company": company, "feature": f,
                           "detector":"ADWIN", "detail": detail}
                    producer.send(PRODUCE_TOPIC, out); producer.flush()
                    log.warning("⚠ Drift detected: %s / %s", company, f)
                    with open(DRIFT_CSV, "a", newline="") as file:
                        csv.writer(file).writerow([out["timestamp"], out["company"], out["feature"], out["detector"], out["detail"]])
            continue
        elif t == "ma_sentiment_stream":
            val = float(p.get("sentiment_score",0.0)); feature="sentiment_score"
        elif t == "ma_volatility_stream":
            val = float(p.get("volatility",0.0)); feature="volatility"
        else:
            continue

        if feature is not None and val is not None:
            aw = get_adwin(company, feature)
            aw.update(val)
            if aw.drift_detected:
                out = {"timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "company": company, "feature": feature,
                       "detector":"ADWIN", "detail": f"ADWIN drift detected in {feature}"}
                producer.send(PRODUCE_TOPIC, out); producer.flush()
                log.warning("⚠ Drift detected: %s / %s", company, feature)

                with open(DRIFT_CSV, "a", newline="") as file:
                    csv.writer(file).writerow([out["timestamp"], out["company"], out["feature"], out["detector"], out["detail"]])
    time.sleep(0.1)