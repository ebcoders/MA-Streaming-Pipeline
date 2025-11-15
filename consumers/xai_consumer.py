#!/usr/bin/env python3
"""
xai_consumer.py - Real-time eXplainable AI Consumer.
This dedicated consumer runs a simple streaming linear model in parallel with the main predictor.
Its sole purpose is to learn continuously from incoming labels and periodically export its
feature weights, providing a real-time view of what is driving predictions.
"""
import os
import json
import logging
import time
import pandas as pd
from kafka import KafkaConsumer
from river import linear_model, preprocessing

# --- Configuration ---
BASE_DIR = os.path.expanduser(os.getenv("BASE_DIR", "~/bda2"))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPICS = [
    "ma_text_stream", "ma_financial_stream", "ma_sentiment_stream",
    "ma_volatility_stream", "ma_labels"
]

OUT_CSV = os.path.join(BASE_DIR, "models/streaming_feature_importances.csv")

WRITE_INTERVAL = 10 

# --- Setup ---
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("xai_consumer")

consumer = KafkaConsumer(
    *TOPICS, bootstrap_servers=[KAFKA_BROKER], auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")), consumer_timeout_ms=1000
)

model = preprocessing.StandardScaler() | linear_model.LogisticRegression()

state = {} 
last_write_time = time.time()

def make_features(s):
    """Helper function to create a feature dictionary from the state."""
    return {
        "liquidity_ratio":  float(s.get("fin",{}).get("liquidity_ratio",0.0)),
        "cash_to_assets":   float(s.get("fin",{}).get("cash_to_assets",0.0)),
        "leverage_ratio":   float(s.get("fin",{}).get("leverage_ratio",0.0)),
        "profit_margin":    float(s.get("fin",{}).get("profit_margin",0.0)),
        "sentiment_score":  float(s.get("sent",{}).get("sentiment_score",0.0)),
        "ma_mentions":      int(s.get("text",{}).get("ma_mentions",0)),
        "volatility":       float(s.get("vol",{}).get("volatility",0.0))
    }

# --- Main Loop ---
log.info(" Real-time XAI Consumer started.")
try:
    while True:
        for msg in consumer:
            topic, p = msg.topic, msg.value
            company = p.get("company")
            if not company: continue

            s = state.setdefault(company, {})

            if topic == "ma_text_stream": s["text"] = p
            elif topic == "ma_financial_stream": s["fin"] = p.get("features",{})
            elif topic == "ma_sentiment_stream": s["sent"] = p
            elif topic == "ma_volatility_stream": s["vol"] = p
            
            elif topic == "ma_labels":
                label = p.get("ma_activity")
                if label is None: continue

                required = ("fin", "text", "sent", "vol")
                if not all(k in s for k in required):
                    continue 

                features = make_features(s)
                model.learn_one(features, bool(label))
                log.info("XAI model learned from label for %s", company)

                if time.time() - last_write_time > WRITE_INTERVAL:
                    linear_model_component = model[1]
                    weights = linear_model_component.weights
                    
                    if not weights: continue
                    df = pd.DataFrame(list(weights.items()), columns=['feature', 'importance'])
                    df['abs_importance'] = df['importance'].abs()
                    df = df.sort_values('abs_importance', ascending=False)
                    
                    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
                    df.to_csv(OUT_CSV, index=False)
                    
                    log.info(" Wrote updated streaming feature importances to %s", OUT_CSV)
                    last_write_time = time.time()
        
        time.sleep(0.1)
except KeyboardInterrupt:
    log.info("Stopping XAI consumer.")