#!/usr/bin/env python3
"""
retrain_consumer.py — listens for retrain_events or drift_alerts and retrains offline.
Uses dataset if available (with explicit ma_activity labels), otherwise tries pseudo-labeling
from ma_likelihood_stream.csv using thresholds.
"""
import os, time, csv, json, logging, pickle
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
import pandas as pd
import numpy as np
from river import linear_model, preprocessing, ensemble, tree
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

BASE_DIR = os.path.expanduser(os.getenv("BASE_DIR","~/bda2"))
KAFKA_BROKER = os.getenv("KAFKA_BROKER","localhost:9092")
DRIFT_TOPIC = "drift_alerts"
RETRAIN_TOPIC = "retrain_events"
LIKELIHOOD = os.path.expanduser(f"{BASE_DIR}/data/ma_likelihood_stream.csv")
SYNTH = os.path.expanduser(f"{BASE_DIR}/data/fda8_full_dataset.csv")  # generator writes this
MODEL_PATH = os.path.expanduser(f"{BASE_DIR}/models/river_model.pkl")
FI_PATH = os.path.expanduser(f"{BASE_DIR}/models/feature_importances.csv")

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("retrain_consumer")

consumer = KafkaConsumer(DRIFT_TOPIC, RETRAIN_TOPIC, bootstrap_servers=[KAFKA_BROKER],
                         value_deserializer=lambda m: json.loads(m.decode("utf-8")), auto_offset_reset="earliest", consumer_timeout_ms=1000)
producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER], value_serializer=lambda v: json.dumps(v).encode("utf-8"))

FEATURES = ["liquidity_ratio","cash_to_assets","leverage_ratio","profit_margin","sentiment_score","ma_mentions","volatility"]

def load_training_df():
    if os.path.exists(SYNTH):
        log.info("Using synthetic labeled dataset: %s", SYNTH)
        return pd.read_csv(SYNTH)

    if os.path.exists(LIKELIHOOD):
        dfp = pd.read_csv(LIKELIHOOD)
        if "features_snapshot" in dfp.columns:
            fs = dfp["features_snapshot"].apply(lambda x: json.loads(x) if isinstance(x,str) else {})
            feats = pd.json_normalize(fs)
            dfp = pd.concat([dfp.drop(columns=["features_snapshot"]), feats], axis=1)
        if "ma_probability" in dfp.columns:
            dfp["ma_activity"] = dfp["ma_probability"].apply(lambda p: 1 if p >= 0.5 else 0)
            df = dfp
            if len(df) > 0:
                log.info("Using pseudo-labelled data from likelihood stream (n=%d).", len(df))
                return df
        log.warning("No usable pseudo-labelled rows available in %s", LIKELIHOOD)
    log.error("No training data available.")
    return pd.DataFrame()

def build_Xy(df):
    for c in FEATURES:
        if c not in df.columns:
            df[c] = 0.0
    X = df[FEATURES].fillna(0.0).astype(float).values
    if "ma_activity" not in df.columns:
        raise ValueError("ma_activity label missing")
    y = df["ma_activity"].astype(int).values
    return X, y

def retrain_models():
    df = load_training_df()
    if df.empty:
        log.error("No samples available. Abort retrain.")
        return
    try:
        X,y = build_Xy(df)
    except Exception as e:
        log.error("Build X/y failed: %s", e)
        return
    n = len(df)
    log.info("Training models on %d samples...", n)
    base = {
        "text": preprocessing.StandardScaler() | linear_model.LogisticRegression(),
        "financial": preprocessing.StandardScaler() | ensemble.SRPClassifier(seed=42),
        "sentiment": preprocessing.StandardScaler() | tree.HoeffdingTreeClassifier(),
        "volatility": preprocessing.StandardScaler() | linear_model.LinearRegression()
    }
    meta = preprocessing.StandardScaler() | linear_model.LogisticRegression()

    for i in range(n):
        xdict = {FEATURES[j]: float(X[i,j]) for j in range(len(FEATURES))}
        label = bool(int(y[i]))

        for model in base.values():
            try:
                model.learn_one(xdict, label)
            except Exception:
                try:
                    model.learn_one(xdict, float(label))
                except:
                    pass

        base_preds = {name: model.predict_proba_one(xdict).get(True,0.5) for name, model in base.items()}
        meta.learn_one(base_preds, label)

    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    with open(MODEL_PATH,"wb") as f:
        pickle.dump((base, meta), f)
    log.info(" Saved model: %s", MODEL_PATH)

    try:
        scaler = StandardScaler()
        Xs = scaler.fit_transform(X)
        clf = LogisticRegression(max_iter=2000)
        clf.fit(Xs, y)
        coef = clf.coef_[0]
        fi_df = pd.DataFrame({"feature": FEATURES, "importance": coef, "abs_importance": np.abs(coef)}).sort_values("abs_importance", ascending=False)
        os.makedirs(os.path.dirname(FI_PATH), exist_ok=True)
        fi_df.to_csv(FI_PATH, index=False)
        log.info(" Saved feature importances: %s", FI_PATH)
    except Exception as e:
        log.error("Feature importance computation failed: %s", e)

    msg = {"event":"retrained", "timestamp":datetime.now(timezone.utc).isoformat(), "model_id": os.path.basename(MODEL_PATH), "n_samples": int(n)}
    producer.send(RETRAIN_TOPIC, msg); producer.flush()
    log.info(" Published retrain event.")

log.info(" Retrain Consumer started.")
try:
    while True:
        for msg in consumer:
            reason = msg.topic
            payload = msg.value
            log.info("Retrain trigger from %s: %s", reason, payload)
            retrain_models()
        time.sleep(0.5)
except KeyboardInterrupt:
    log.info("Stopping retrain consumer.")
