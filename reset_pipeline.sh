#!/usr/bin/env bash
# reset_pipeline.sh — Wipes all data, models, logs & deletes Kafka topics.
set -euo pipefail

BASE_DIR="${BASE_DIR:-$HOME/bda2}"
KAFKA_DIR="${KAFKA_DIR:-$HOME/kafka}"

echo "--- Starting Full Pipeline Reset ---"

echo "STEP 1: Stopping all pipeline processes..."
bash "$BASE_DIR/stop_pipeline.sh" || true
sleep 1

echo "STEP 2: Deleting streaming data CSVs..."
rm -f "$BASE_DIR/data"/*.csv || true

echo "STEP 3: Deleting trained models and XAI files..."
rm -f "$BASE_DIR/models"/*.pkl "$BASE_DIR/models"/*.csv || true

echo "STEP 4: Clearing log files..."
rm -f "$BASE_DIR/logs"/* || true

echo "STEP 5: Deleting Kafka topics..."
if "$KAFKA_DIR/bin/kafka-topics.sh" --version >/dev/null 2>&1; then
  TOPICS=(
    "ma_text_stream" "ma_financial_stream" "ma_sentiment_stream" 
    "ma_volatility_stream" "ma_prediction_stream" "drift_alerts" 
    "retrain_events" "ma_labels"
  )
  for t in "${TOPICS[@]}"; do
    echo "  - Deleting topic: $t"
    "$KAFKA_DIR/bin/kafka-topics.sh" --delete --topic "$t" --bootstrap-server localhost:9092 2>/dev/null || true
  done
else
  echo "  - Warning: kafka-topics.sh not found, skipping topic deletion."
fi

echo " Reset complete. Run kafka_setup.sh and generate_dataset.py before starting again."