#!/usr/bin/env python3
"""
A comprehensive, multi-tab Streamlit dashboard for monitoring the M&A prediction pipeline.
"""
import os
import json
import logging
import streamlit as st
import pandas as pd
import plotly.express as px
from kafka import KafkaProducer
from streamlit_autorefresh import st_autorefresh

# --- Configuration & Setup ---
st.set_page_config(page_title="M&A Streaming Dashboard", layout="wide")
log = logging.getLogger("dashboard")
BASE_DIR = os.path.expanduser(os.getenv("BASE_DIR", "~/bda2"))

PRED_CSV = os.path.join(BASE_DIR, "data/ma_likelihood_stream.csv")
OFFLINE_FI_PATH = os.path.join(BASE_DIR, "models/feature_importances.csv")
STREAMING_FI_PATH = os.path.join(BASE_DIR, "models/streaming_feature_importances.csv") # <-- NEW
DRIFT_CSV = os.path.join(BASE_DIR, "data/drift_alerts.csv")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
RETRAIN_TOPIC = "retrain_events"

@st.cache_resource
def get_kafka_producer():
    try:
        producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER], value_serializer=lambda v: json.dumps(v).encode("utf-8"), client_id="streamlit-dashboard")
        return producer
    except Exception as e:
        st.sidebar.error(f"Kafka producer unavailable: {e}")
        return None
producer = get_kafka_producer()

def safe_read_csv(path):
    if not os.path.exists(path): return pd.DataFrame()
    try: return pd.read_csv(path)
    except Exception as e:
        log.warning(f"Could not read {os.path.basename(path)}: {e}")
        return pd.DataFrame()

# --- Main Dashboard UI ---
st.title("📈 Real-time M&A Likelihood Dashboard")
st.sidebar.header("Controls")
if st.sidebar.checkbox("Auto-refresh data", value=True):
    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 15)
    st_autorefresh(interval=refresh_interval * 1000, key="data_refresh")
st.sidebar.markdown("---")
st.sidebar.info(f"Monitoring data from: `{BASE_DIR}`")

tab1, tab2, tab3 = st.tabs(["Live Predictions", "Feature Importance (XAI)", "Drift Alerts"])

with tab1:
    st.header("Latest M&A Probabilities")
    df = safe_read_csv(PRED_CSV)
    if df.empty: st.info("Waiting for prediction data...")
    else:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce"); df = df.dropna(subset=["timestamp", "company", "ma_probability"])
        latest_df = df.sort_values("timestamp").groupby("company").last().reset_index(); latest_df["ma_probability"] = pd.to_numeric(latest_df["ma_probability"])
        st.subheader("Top 10 Companies by M&A Likelihood"); st.dataframe(latest_df.sort_values("ma_probability", ascending=False).head(10).style.format({"ma_probability": "{:.2%}"}).background_gradient(cmap='Reds', subset=['ma_probability']), use_container_width=True)
        fig = px.bar(latest_df.sort_values("ma_probability", ascending=False).head(10), x="company", y="ma_probability", title="Live M&A Probability", labels={"company": "Company", "ma_probability": "M&A Probability"}, height=400); st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.header("Explainable AI (XAI): Feature Importance Comparison")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Real-time (Streaming) Importance")
        streaming_fi_df = safe_read_csv(STREAMING_FI_PATH)
        if streaming_fi_df.empty:
            st.info("Waiting for real-time feature importance data... (requires labels to be streamed)")
        else:
            streaming_fi_df = streaming_fi_df.sort_values("abs_importance", ascending=True)
            fig_stream = px.bar(
                streaming_fi_df, x="importance", y="feature", orientation='h',
                title="Evolving Feature Weights", height=500
            )
            st.plotly_chart(fig_stream, use_container_width=True)

    with col2:
        st.subheader("Latest Batch (Offline) Importance")
        offline_fi_df = safe_read_csv(OFFLINE_FI_PATH)
        if offline_fi_df.empty:
            st.info("Offline feature importance file not found. A model must be retrained first.")
        else:
            offline_fi_df = offline_fi_df.sort_values("abs_importance", ascending=True)
            fig_offline = px.bar(
                offline_fi_df, x="importance", y="feature", orientation='h',
                title="Stable Weights (Post-Retrain)", height=500
            )
            st.plotly_chart(fig_offline, use_container_width=True)

    st.markdown("---")
    st.subheader("Manual Model Retraining")
    if st.button("🚀 Trigger Retrain Now"):
        if producer:
            event = {"event": "manual_retrain_trigger", "source": "dashboard", "timestamp": datetime.now(timezone.utc).isoformat()}
            producer.send(RETRAIN_TOPIC, event); producer.flush()
            st.success(" Retrain event published to Kafka!"); st.toast("Model retraining process initiated.")
        else: st.error("Cannot trigger retrain: Kafka producer is not connected.")

with tab3:
    st.header("Data Drift Alerts")
    drift_df = safe_read_csv(DRIFT_CSV)
    if drift_df.empty: st.info("No drift alerts have been logged yet.")
    else:
        drift_df["timestamp"] = pd.to_datetime(drift_df["timestamp"], errors="coerce"); drift_df = drift_df.dropna(subset=["timestamp"])
        st.subheader("Most Recent Drift Events")
        display_cols = ["timestamp", "company", "feature", "old_mean", "new_mean", "delta", "detail"]; cols_to_show = [col for col in display_cols if col in drift_df.columns]
        st.dataframe(drift_df.sort_values("timestamp", ascending=False).head(20)[cols_to_show], use_container_width=True, column_config={"old_mean": st.column_config.NumberColumn(format="%.4f"), "new_mean": st.column_config.NumberColumn(format="%.4f"), "delta": st.column_config.NumberColumn(format="%.4f 🔥")})
        st.subheader("Drift Events Over Time"); drift_counts = drift_df.groupby([pd.Grouper(key='timestamp', freq='min'), 'feature']).size().reset_index(name='count')
        if not drift_counts.empty:
            fig = px.bar(drift_counts, x='timestamp', y='count', color='feature', title='Drift Alerts per Minute', labels={"timestamp": "Time", "count": "Number of Drift Events", "feature": "Feature"}, height=500); st.plotly_chart(fig, use_container_width=True)