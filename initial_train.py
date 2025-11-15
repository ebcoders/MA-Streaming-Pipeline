#!/usr/bin/env python3
"""
initial_train.py — builds the first version of the model from the full dataset.
Run this script once before starting the pipeline to avoid the "0.5 prediction" problem.
"""
import os, json, logging, pickle
import pandas as pd
import numpy as np
from river import linear_model, preprocessing, ensemble, tree
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

# --- Configuratio ---
BASE_DIR = os.path.expanduser(os.getenv("BASE_DIR","~/bda2"))
SYNTH = os.path.expanduser(f"{BASE_DIR}/data/fda8_full_dataset.csv")
MODEL_PATH = os.path.expanduser(f"{BASE_DIR}/models/river_model.pkl")
FI_PATH = os.path.expanduser(f"{BASE_DIR}/models/feature_importances.csv")
FEATURES = ["liquidity_ratio","cash_to_assets","leverage_ratio","profit_margin","sentiment_score","ma_mentions","volatility"]

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("initial_train")

def load_training_df():
    if not os.path.exists(SYNTH):
        log.error("Synthetic labeled dataset not found: %s", SYNTH)
        log.error("Please generate it first using: python data/generate_dataset.py")
        return pd.DataFrame()
    log.info("Using synthetic labeled dataset: %s", SYNTH)
    return pd.read_csv(SYNTH)

def build_Xy(df):
    for c in FEATURES:
        if c not in df.columns:
            df[c] = 0.0
    X = df[FEATURES].fillna(0.0).astype(float).values
    if "ma_activity" not in df.columns:
        raise ValueError("ma_activity label missing")
    y = df["ma_activity"].astype(int).values
    return X, y

def train_and_save_models():
    """Main training function, adapted from retrain_consumer."""
    df = load_training_df()
    if df.empty:
        log.error("No samples available. Aborting initial training.")
        return
    try:
        X,y = build_Xy(df)
    except Exception as e:
        log.error("Build X/y failed: %s", e)
        return

    n = len(df)
    log.info("Training initial models on %d samples...", n)

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
            except TypeError:
                model.learn_one(xdict, float(label))
            except Exception as e:
                log.warning("Learning failed for a base model: %s", e)
                pass

        base_preds = {}
        for name, model in base.items():
            try:
                if isinstance(model[-1], linear_model.LinearRegression):
                    pred = model.predict_one(xdict)
                    base_preds[name] = 1.0 / (1.0 + np.exp(-pred))
                else:
                    base_preds[name] = model.predict_proba_one(xdict).get(True, 0.5)
            except Exception:
                base_preds[name] = 0.5
        
        meta.learn_one(base_preds, label)

    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    with open(MODEL_PATH,"wb") as f:
        pickle.dump((base, meta), f)
    log.info(" Saved initial model to: %s", MODEL_PATH)

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

if __name__ == "__main__":
    train_and_save_models()