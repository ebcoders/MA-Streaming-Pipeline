#!/usr/bin/env python3
"""
drift_test_producer.py - A specialized producer for demonstrating drift and alerts.

This script sends a controlled sequence of data in three phases:
1.  Phase 1: Establishes a stable baseline with low volatility and no M&A mentions.
2.  Phase 2: Intentionally causes a sharp, sustained increase in volatility to trigger a drift alert.
3.  Phase 3: Sends a specific combination of features known to cause a high M&A prediction.
"""
import os
import json
import time
import logging
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

# --- Configuration ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
COMPANIES = ["DEMO-CORP"]

FIN_TOPIC = "ma_financial_stream"
TEXT_TOPIC = "ma_text_stream"
SENT_TOPIC = "ma_sentiment_stream"
VOL_TOPIC = "ma_volatility_stream"

# --- Setup ---
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("drift_test_producer")

try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
except Exception as e:
    log.error("Could not connect to Kafka broker: %s", e)
    exit(1)

def emit(topic, company, payload):
    """Generic function to send a message to a specific topic."""
    payload["company"] = company
    payload["timestamp"] = datetime.now(timezone.utc).isoformat()
    producer.send(topic, payload)
    log.info("SENT to %s -> %s", topic, payload)

def run_demo():
    """Executes the three-phase demo sequence."""
    
    print("\n--- PHASE 1: Establishing a stable baseline (15 seconds) ---")
    print(">>> No alerts are expected during this phase.\n")
    for i in range(15):
        for company in COMPANIES:
            emit(VOL_TOPIC, company, {"volatility": round(random.uniform(10, 15), 3), "source": "demo_baseline"})
            emit(TEXT_TOPIC, company, {"ma_mentions": 0, "source": "demo_baseline"})
            emit(SENT_TOPIC, company, {"sentiment_score": round(random.uniform(-0.1, 0.1), 3), "source": "demo_baseline"})
            emit(FIN_TOPIC, company, {"features": {"profit_margin": 0.15}, "source": "demo_baseline"})
        time.sleep(1)
    producer.flush()

    print("\n--- PHASE 2: Triggering DRIFT alert (sending high volatility data) ---")
    print(">>> WATCH YOUR SLACK! A 'Data Drift Detected' alert for 'volatility' should appear shortly.\n")
    for i in range(15):
        for company in COMPANIES:
            emit(VOL_TOPIC, company, {"volatility": round(random.uniform(45, 55), 3), "source": "demo_drift_trigger"})
            emit(TEXT_TOPIC, company, {"ma_mentions": 0, "source": "demo_drift_trigger"})
            emit(SENT_TOPIC, company, {"sentiment_score": round(random.uniform(-0.1, 0.1), 3), "source": "demo_drift_trigger"})
            emit(FIN_TOPIC, company, {"features": {"profit_margin": 0.15}, "source": "demo_drift_trigger"})
        time.sleep(1)
    producer.flush()

    print("\n--- PHASE 3: Triggering HIGH M&A LIKELIHOOD alert ---")
    print(">>> WATCH YOUR SLACK AGAIN! A 'High M&A Probability' alert should appear.\n")
    for i in range(5):
        for company in COMPANIES:
            emit(VOL_TOPIC, company, {"volatility": 35.0, "source": "demo_m&a_trigger"})
            emit(TEXT_TOPIC, company, {"ma_mentions": 5, "source": "demo_m&a_trigger"})
            emit(SENT_TOPIC, company, {"sentiment_score": 0.1, "source": "demo_m&a_trigger"})
            emit(FIN_TOPIC, company, {"features": {"profit_margin": 0.15}, "source": "demo_m&a_trigger"})
        time.sleep(1)
    producer.flush()

    print("\n--- Demo sequence complete. ---")

if __name__ == "__main__":
    run_demo()