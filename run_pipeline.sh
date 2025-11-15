#!/usr/bin/env bash
# run_pipeline.sh [mode]
# Starts all producers and consumers for the M&A prediction pipeline.
set -euo pipefail

# --- Configuration ---
BASE_DIR="${BASE_DIR:-$HOME/bda2}"
MODE="${1:-dataset}"   # Can be: api | dataset | synthetic
LOG_DIR="$BASE_DIR/logs"
mkdir -p "$LOG_DIR"

export KAFKA_BROKER="${KAFKA_BROKER:-localhost:9092}"
export MODE
export TIME_MULTIPLIER="${TIME_MULTIPLIER:-3.0}" # Speeds up dataset replay
export BASE_DIR # Make sure BASE_DIR is available to all scripts

echo "--- Starting M&A Pipeline in mode: $MODE ---"
echo "Find logs in: $LOG_DIR"

if [ -f "$BASE_DIR/venv/bin/activate" ]; then
  source "$BASE_DIR/venv/bin/activate"
  echo "Activated Python venv."
fi

# --- Start Data Producers ---
echo "Starting data producers..."
PRODS=(ma_text_producer ma_financial_producer ma_sentiment_producer ma_volatility_producer)
for p in "${PRODS[@]}"; do
  nohup python "$BASE_DIR/producers/${p}.py" > "$LOG_DIR/${p}_${MODE}.log" 2>&1 &
  sleep 0.2
done

# --- Start Core Consumers ---
echo "Starting core consumers..."

nohup python "$BASE_DIR/consumers/capymoa_consumer.py" > "$LOG_DIR/capymoa_consumer_${MODE}.log" 2>&1 &
sleep 0.2

nohup python "$BASE_DIR/consumers/xai_consumer.py" > "$LOG_DIR/xai_consumer_${MODE}.log" 2>&1 &
sleep 0.2

nohup python "$BASE_DIR/consumers/river_predictor_consumer.py" > "$LOG_DIR/predictor_${MODE}.log" 2>&1 &
sleep 0.2

nohup python "$BASE_DIR/consumers/retrain_consumer.py" > "$LOG_DIR/retrain_${MODE}.log" 2>&1 &
sleep 0.2

nohup python "$BASE_DIR/consumers/slack_alert_consumer.py" > "$LOG_DIR/slack_alert_consumer_${MODE}.log" 2>&1 &
sleep 0.2

# --- Start Dashboard ---
if command -v streamlit >/dev/null 2>&1; then
  echo "Starting Streamlit dashboard..."
  nohup streamlit run "$BASE_DIR/dashboard/app.py" --server.port 8501 --server.headless true > "$LOG_DIR/streamlit_${MODE}.log" 2>&1 &
else
  echo "Warning: streamlit command not found. Dashboard will not start."
fi

echo " Pipeline started successfully. View dashboard at http://localhost:8501"