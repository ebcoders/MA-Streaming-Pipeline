#!/usr/bin/env python3
"""
capymoa_consumer.py
Listens to feature streams, tracks per-company per-feature running means and uses River ADWIN
to detect drift. Publishes to drift_alerts and logs to CSV when drift occurs with deltas.
"""
import os, json, time, logging, csv
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
from river import drift

# --- Configuration ---
BASE_DIR = os.path.expanduser(os.getenv("BASE_DIR","~/bda2"))
KAFKA_BROKER = os.getenv("KAFKA_BROKER","localhost:9092")
TOPICS = ["ma_text_stream","ma_financial_stream","ma_sentiment_stream","ma_volatility_stream"]
OUT_TOPIC = "drift_alerts"
DRIFT_CSV = os.path.expanduser(f"{BASE_DIR}/data/drift_alerts.csv") # <-- ADDED: Path for the dashboard CSV

# --- Setup ---
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("capymoa_consumer")

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[KAFKA_BROKER], auto_offset_reset="earliest", enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")), consumer_timeout_ms=1000
)
producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER],
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))

state = {}
if not os.path.exists(DRIFT_CSV):
    os.makedirs(os.path.dirname(DRIFT_CSV), exist_ok=True)
    with open(DRIFT_CSV, "w", newline="") as f:
        csv.writer(f).writerow(["timestamp", "company", "feature", "old_mean", "new_mean", "delta", "detector", "detail"])

def publish_alert(company, feature, old_mean, new_mean, delta, detector="ADWIN", detail=""):
    """Publishes alert to Kafka and writes it to the local CSV file."""
    msg = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "company": company,
        "feature": feature,
        "old_mean": old_mean,
        "new_mean": new_mean,
        "delta": delta,
        "detector": f"capymoa.{detector}",
        "detail": detail
    }
    producer.send(OUT_TOPIC, msg); producer.flush()
    log.warning("⚠ Drift detected: %s / %s  Δ=%.4f", company, feature, delta)
    
    with open(DRIFT_CSV, "a", newline="") as f:
        csv.writer(f).writerow([
            msg["timestamp"], msg["company"], msg["feature"], msg["old_mean"],
            msg["new_mean"], msg["delta"], msg["detector"], msg["detail"]
        ])

def process_financial(company, features):
    for k in ["liquidity_ratio","cash_to_assets","leverage_ratio","profit_margin"]:
        v = float(features.get(k,0.0))
        sensor = state.setdefault(company, {}).setdefault(k, {"adwin": drift.ADWIN(delta=0.002), "mean": None})
        old = sensor["mean"]
        sensor["adwin"].update(v)
        sensor["mean"] = v if old is None else (old*0.9 + v*0.1) # Exponential moving average
        if sensor["adwin"].drift_detected:
            delta = abs((sensor["mean"] or 0) - (old or 0))
            publish_alert(company, k, old or 0.0, sensor["mean"] or 0.0, delta, "ADWIN", f"Drift in {k}")
            sensor["adwin"].reset()


def process_sentiment(company, v):
    v = float(v)
    sensor = state.setdefault(company, {}).setdefault("sentiment_score", {"adwin": drift.ADWIN(delta=0.002), "mean": None})
    old = sensor["mean"]
    sensor["adwin"].update(v)
    sensor["mean"] = v if old is None else (old*0.9 + v*0.1)
    if sensor["adwin"].drift_detected:
        delta = abs((sensor["mean"] or 0) - (old or 0))
        publish_alert(company, "sentiment_score", old or 0.0, sensor["mean"] or 0.0, delta, "ADWIN", "Drift in sentiment_score")
        sensor["adwin"].reset()

def process_text(company, ma_mentions):
    v = int(ma_mentions)
    sensor = state.setdefault(company, {}).setdefault("ma_mentions", {"adwin": drift.ADWIN(delta=0.002), "mean": None})
    old = sensor["mean"]
    sensor["adwin"].update(v)
    sensor["mean"] = v if old is None else (old*0.9 + v*0.1)
    if sensor["adwin"].drift_detected:
        delta = abs((sensor["mean"] or 0) - (old or 0))
        publish_alert(company, "ma_mentions", old or 0.0, sensor["mean"] or 0.0, delta, "ADWIN", "Drift in ma_mentions")
        sensor["adwin"].reset()

def process_volatility(company, v):
    v = float(v)
    sensor = state.setdefault(company, {}).setdefault("volatility", {"adwin": drift.ADWIN(delta=0.002), "mean": None})
    old = sensor["mean"]
    sensor["adwin"].update(v)
    sensor["mean"] = v if old is None else (old*0.9 + v*0.1)
    if sensor["adwin"].drift_detected:
        delta = abs((sensor["mean"] or 0) - (old or 0))
        publish_alert(company, "volatility", old or 0.0, sensor["mean"] or 0.0, delta, "ADWIN", "Drift in volatility")
        sensor["adwin"].reset()


# --- Main Loop ---
log.info(" capymoa_consumer started.")
try:
    while True:
        for msg in consumer:
            topic, p = msg.topic, msg.value
            company = p.get("company")
            if not company: continue
            
            if topic == "ma_financial_stream":
                process_financial(company, p.get("features",{}))
            elif topic == "ma_sentiment_stream":
                process_sentiment(company, p.get("sentiment_score",0.0))
            elif topic == "ma_text_stream":
                process_text(company, p.get("ma_mentions",0))
            elif topic == "ma_volatility_stream":
                process_volatility(company, p.get("volatility",0.0))
        time.sleep(0.1)
except KeyboardInterrupt:
    log.info("Stopping capymoa_consumer.")