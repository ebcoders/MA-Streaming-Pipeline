#!/usr/bin/env python3
"""
river_predictor_consumer.py — streaming predictor with online learning support.
Listens: ma_text_stream, ma_financial_stream, ma_sentiment_stream, ma_volatility_stream, retrain_events, ma_labels
Publishes: ma_prediction_stream
"""
import os, json, time, csv, pickle, logging, math
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
from river import linear_model, preprocessing, ensemble, tree

BASE_DIR = os.path.expanduser(os.getenv("BASE_DIR","~/bda2"))
KAFKA_BROKER = os.getenv("KAFKA_BROKER","localhost:9092")
TOPICS = ["ma_text_stream","ma_financial_stream","ma_sentiment_stream","ma_volatility_stream","retrain_events","ma_labels"]
OUT_TOPIC = "ma_prediction_stream"
OUT_CSV = os.path.expanduser(f"{BASE_DIR}/data/ma_likelihood_stream.csv")
MODEL_PATH = os.path.expanduser(f"{BASE_DIR}/models/river_model.pkl")

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("river_predictor_consumer")

os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
if not os.path.exists(OUT_CSV):
    with open(OUT_CSV,"w") as f:
        csv.writer(f).writerow(["company","timestamp","ma_probability","model_id","features_snapshot"])

consumer = KafkaConsumer(*TOPICS, bootstrap_servers=[KAFKA_BROKER],
                         auto_offset_reset="earliest", enable_auto_commit=True,
                         value_deserializer=lambda m: json.loads(m.decode("utf-8")), consumer_timeout_ms=1000)
producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER], value_serializer=lambda v: json.dumps(v).encode("utf-8"))

def fresh_models():
    base = {
        "text": preprocessing.StandardScaler() | linear_model.LogisticRegression(),
        "financial": preprocessing.StandardScaler() | ensemble.SRPClassifier(seed=42),
        "sentiment": preprocessing.StandardScaler() | tree.HoeffdingTreeClassifier(),
        "volatility": preprocessing.StandardScaler() | linear_model.LinearRegression()
    }
    meta = preprocessing.StandardScaler() | linear_model.LogisticRegression()
    return base, meta

base_models, meta_model = fresh_models()
CURRENT_MODEL_ID = "river_stacking_stream"

def load_saved_model():
    global base_models, meta_model, CURRENT_MODEL_ID
    if not os.path.exists(MODEL_PATH):
        log.warning("No model file found — using fresh streaming models.")
        return
    try:
        with open(MODEL_PATH,"rb") as f:
            data = pickle.load(f)
        if not (isinstance(data, tuple) and len(data)==2):
            raise ValueError("Unexpected model format")
        base_models, meta_model = data
        CURRENT_MODEL_ID = os.path.basename(MODEL_PATH)
        log.info(" Reloaded model: %s", MODEL_PATH)
    except Exception as e:
        log.error("Model reload failed (%s). Using fresh models.", e)
        base_models, meta_model = fresh_models()

load_saved_model()

state = {}

def make_features(s):
    return {
        "liquidity_ratio":  float(s.get("fin",{}).get("liquidity_ratio",0.0)),
        "cash_to_assets":   float(s.get("fin",{}).get("cash_to_assets",0.0)),
        "leverage_ratio":   float(s.get("fin",{}).get("leverage_ratio",0.0)),
        "profit_margin":    float(s.get("fin",{}).get("profit_margin",0.0)),
        "sentiment_score":  float(s.get("sent",{}).get("sentiment_score",0.0)),
        "ma_mentions":      int(s.get("text",{}).get("ma_mentions",0)),
        "volatility":       float(s.get("vol",{}).get("volatility",0.0))
    }

def sigmoid(x):
    try:
        return 1.0/(1.0+math.exp(-x))
    except:
        return 0.5

def base_predict_proba(feat):
    preds = {}
    for name, model in base_models.items():
        try:
            if name == "volatility":
                raw = model.predict_one(feat)
                try:
                    raw_val = float(raw)
                except:
                    if isinstance(raw, dict):
                        raw_val = float(next(iter(raw.values())) if raw else 0.0)
                    else:
                        raw_val = 0.0
                preds[name] = sigmoid(raw_val)
            else:
                p = model.predict_proba_one(feat)
                preds[name] = float(p.get(True, 0.5))
        except Exception:
            preds[name] = 0.5
    return preds

def stacked_predict(feat):
    preds = base_predict_proba(feat)
    try:
        prob = float(meta_model.predict_proba_one(preds).get(True, 0.5))
    except Exception:
        prob = 0.5
    return prob, preds

def publish(company, prob, feat):
    record = {"company": company, "timestamp": datetime.now(timezone.utc).isoformat(),
              "ma_probability": prob, "model_id": CURRENT_MODEL_ID, "features_snapshot": feat}
    producer.send(OUT_TOPIC, record); producer.flush()
    with open(OUT_CSV,"a",newline="") as f:
        csv.writer(f).writerow([company, record["timestamp"], prob, CURRENT_MODEL_ID, json.dumps(feat)])
    log.info("%s: P=%.3f", company, prob)

# Online learning: if a true label arrives on ma_labels topic, update models
def online_learn(company, feat, label):
    # label: 0 or 1
    try:
        for m in base_models.values():
            try:
                m.learn_one(feat, bool(label))
            except Exception:
                try:
                    m.learn_one(feat, float(label))
                except:
                    pass
        base_preds = base_predict_proba(feat)
        meta_model.learn_one(base_preds, bool(label))
        log.info("Online learned for %s label=%s", company, label)
    except Exception as e:
        log.error("Online learn failed: %s", e)

log.info(" River Predictor Consumer started.")

try:
    while True:
        for msg in consumer:
            topic = msg.topic; p = msg.value
            if topic == "retrain_events":
                log.info("🔄 Retrain event received → Reload model.")
                load_saved_model()
                continue
            if topic == "ma_labels":
                company = p.get("company"); label = p.get("ma_activity")
                if company is None or label is None:
                    continue
                s = state.setdefault(company, {})
                required = ("fin", "text", "sent", "vol")
                missing = [k for k in required if k not in s]
                if missing:
                    log.warning("LABEL ARRIVED BUT INCOMPLETE FEATURES for %s — missing: %s", company, missing)
                    continue

                feat = make_features(s)
                online_learn(company, feat, int(label))

                continue

            company = p.get("company")
            if not company:
                continue
            s = state.setdefault(company, {})

            if topic == "ma_text_stream":
                s.setdefault("text", {})
                if "ma_mentions" in p:
                    s["text"]["ma_mentions"] = int(p.get("ma_mentions",0))
            elif topic == "ma_financial_stream":
                s.setdefault("fin", {})
                for k,v in p.get("features",{}).items():
                    s["fin"][k] = v
            elif topic == "ma_sentiment_stream":
                s.setdefault("sent", {})
                if "sentiment_score" in p:
                    s["sent"]["sentiment_score"] = float(p.get("sentiment_score",0.0))
            elif topic == "ma_volatility_stream":
                s.setdefault("vol", {})
                if "volatility" in p:
                    s["vol"]["volatility"] = float(p.get("volatility",0.0))

            required = ("fin", "text", "sent", "vol")
            missing = [k for k in required if k not in s]
            if missing:
                log.info("WAITING %s — missing streams: %s", company, missing)
                continue

            feat = make_features(s)
            log.info("FEAT_READY %s → %s", company, feat)
            prob, preds = stacked_predict(feat)
            publish(company, prob, feat)

        time.sleep(0.1)
except KeyboardInterrupt:
    log.info("Stopping predictor consumer.")
