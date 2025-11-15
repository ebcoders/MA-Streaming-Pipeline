#!/usr/bin/env bash
# stop_pipeline.sh — stops all producers & consumers for the pipeline.
set -euo pipefail

echo "Stopping all pipeline processes..."

pkill -f ma_text_producer.py || true
pkill -f ma_financial_producer.py || true
pkill -f ma_sentiment_producer.py || true
pkill -f ma_volatility_producer.py || true

pkill -f river_predictor_consumer.py || true
pkill -f retrain_consumer.py || true
pkill -f slack_alert_consumer.py || true
pkill -f capymoa_consumer.py || true
pkill -f xai_consumer.py || true

pkill -f drift_consumer.py || true

pkill -f streamlit || true

echo " All pipeline processes stopped."