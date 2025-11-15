#!/usr/bin/env python3
"""
ma_sentiment_producer.py — emits sentiment scores.
(FIXED with the new HuggingFace Inference Router URL)
"""
import os, time, json, csv, logging, random, requests
from datetime import datetime, timezone
from kafka import KafkaProducer
import pandas as pd

# --- Configuration ---
BASE_DIR = os.path.expanduser(os.getenv("BASE_DIR","~/bda2"))
MODE = os.getenv("MODE","dataset").lower()
DATASET = os.path.expanduser(os.getenv("SENT_DATA", f"{BASE_DIR}/data/fda8_sentiment_dataset.csv"))
OUT_CSV = os.path.expanduser(f"{BASE_DIR}/data/ma_sentiment_stream.csv")
TOPIC = "ma_sentiment_stream"
LABEL_TOPIC = "ma_labels"
TIME_MULTIPLIER = float(os.getenv("TIME_MULTIPLIER","1.0"))
HF_TOKEN = os.getenv("HF_TOKEN", "")
HF_MODELS = [
    "ProsusAI/finbert",
    "yiyanghkust/finbert-tone",
    "cardiffnlp/twitter-roberta-base-sentiment-latest"
]

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("ma_sentiment_producer")
producer = KafkaProducer(bootstrap_servers=[os.getenv("KAFKA_BROKER","localhost:9092")],
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))

def emit(msg):
    producer.send(TOPIC, msg); producer.flush()
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    with open(OUT_CSV,"a",newline="") as f:
        csv.writer(f).writerow([msg["company"], msg["timestamp"], msg.get("sentiment_score"), msg.get("source","")])
    if "ma_activity" in msg:
        label_msg = {"company": msg["company"], "timestamp": msg["timestamp"], "ma_activity": msg["ma_activity"]}
        producer.send(LABEL_TOPIC, label_msg); producer.flush()
        log.info("SENT label -> %s (ma_activity=%s)", msg["company"], msg["ma_activity"])

def hf_infer(text, timeout=30):
    if not HF_TOKEN:
        log.warning("HF_TOKEN not set. Cannot use API mode.")
        return None
    headers = {"Authorization": f"Bearer {HF_TOKEN}"}
    payload = {"inputs": text}
    for model in HF_MODELS:

        url = f"https://router.huggingface.co/hf-inference/models/{model}"
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=timeout)
            r.raise_for_status()
            res = r.json()
            if isinstance(res, list) and isinstance(res[0], list): res = res[0]
            if isinstance(res, list) and len(res) > 0:
                best_prediction = max(res, key=lambda x: x.get('score', 0))
                label = best_prediction.get("label", "neutral").lower()
                score = best_prediction.get("score", 0.5)
                if "positive" in label or "pos" in label: return float(score)
                if "negative" in label or "neg" in label: return -float(score)
                return 0.0
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 410:
                log.warning("HF model %s is deprecated or unavailable at the new endpoint.", model)
                continue
            log.warning("HF HTTPError for model %s: %s", model, e)
        except Exception as e:
            log.warning("HF inference failed for model %s: %s", model, e)
    log.error("All HF models failed for text: '%s'", text)
    return None

if MODE == "dataset":
    log.info("SENT producer running in DATASET mode, file=%s", DATASET)
    if not os.path.exists(DATASET): log.error("Dataset not found at %s. Exiting.", DATASET); exit(1)
    df = pd.read_csv(DATASET).sort_values("timestamp")
    prev_ts = None
    for _, row in df.iterrows():
        ts = pd.to_datetime(row["timestamp"])
        if prev_ts is not None:
            delta = max(0.01, (ts - prev_ts).total_seconds()/3600.0); time.sleep(delta / TIME_MULTIPLIER)
        prev_ts = ts
        msg = {"company": row["company"], "timestamp": ts.to_pydatetime().astimezone(timezone.utc).isoformat(), "sentiment_score": float(row["sentiment_score"]), "source": "dataset"}
        if "ma_activity" in row and pd.notna(row["ma_activity"]): msg["ma_activity"] = int(row["ma_activity"])
        emit(msg); log.info("SENT dataset -> %s", row["company"])
    log.info("SENT dataset stream finished.")
elif MODE == "api":
    log.info("SENT producer running in API mode (HuggingFace).")
    COMPANIES = os.getenv("COMPANIES", "AAPL,MSFT,AMZN,GOOML,META").split(",")
    try:
        while True:
            for c in COMPANIES:
                text_to_analyze = f"Recent news and financial performance for {c}"
                score = hf_infer(text_to_analyze)
                source = "hf_api"
                if score is None:
                    score = round(random.uniform(-0.6, 0.6), 3); source = "synthetic_fallback"
                msg = {"company": c, "timestamp": datetime.now(timezone.utc).isoformat(), "sentiment_score": float(score), "source": source}
                emit(msg); log.info("SENT api -> %s (score: %.3f, source: %s)", c, score, source)
                time.sleep(1.0 / TIME_MULTIPLIER)
    except KeyboardInterrupt: log.info("Stopping sentiment producer.")
else:
    log.info("SENT producer running in SYNTHETIC mode.")
    COMPANIES = ["AAPL", "MSFT", "AMZN", "GOOGL", "META"]
    try:
        while True:
            for c in COMPANIES:
                score = round(random.uniform(-0.9, 0.9), 3)
                msg = {"company": c, "timestamp": datetime.now(timezone.utc).isoformat(), "sentiment_score": score, "source":"synthetic"}
                emit(msg); log.info("SENT synthetic -> %s (score: %.3f)", c, score)
                time.sleep(1.0 / TIME_MULTIPLIER)
    except KeyboardInterrupt: log.info("Stopping sentiment producer.")