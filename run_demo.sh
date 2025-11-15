#!/usr/bin/env bash
# run_demo.sh
# A dedicated script to demonstrate the pipeline's alerting capabilities.
# It starts all consumers and then runs a special producer to force-trigger alerts.
set -euo pipefail

# --- Configuration ---
BASE_DIR="${BASE_DIR:-$HOME/bda2}"
LOG_DIR="$BASE_DIR/logs"
mkdir -p "$LOG_DIR"

export KAFKA_BROKER="${KAFKA_BROKER:-localhost:9092}"
export MODE="demo"
export BASE_DIR

echo "--- Starting M&A Pipeline DEMO ---"
echo "This will start all consumers and then run a test producer to trigger alerts."
echo "Please monitor your Slack channel and the Streamlit dashboard."
echo "----------------------------------------"

echo "STEP 1: Stopping any existing pipeline processes..."
bash "$BASE_DIR/stop_pipeline.sh" || true
sleep 2

if [ -f "$BASE_DIR/venv/bin/activate" ]; then
  source "$BASE_DIR/venv/bin/activate"
  echo "Activated Python venv."
fi

echo "STEP 2: Starting all consumers and dashboard in the background..."

nohup python "$BASE_DIR/consumers/capymoa_consumer.py" > "$LOG_DIR/capymoa_consumer_${MODE}.log" 2>&1 &

nohup python "$BASE_DIR/consumers/river_predictor_consumer.py" > "$LOG_DIR/predictor_${MODE}.log" 2>&1 &

nohup python "$BASE_DIR/consumers/retrain_consumer.py" > "$LOG_DIR/retrain_${MODE}.log" 2>&1 &

nohup python "$BASE_DIR/consumers/slack_alert_consumer.py" > "$LOG_DIR/slack_alert_consumer_${MODE}.log" 2>&1 &

nohup python "$BASE_DIR/consumers/xai_consumer.py" > "$LOG_DIR/xai_consumer_${MODE}.log" 2>&1 &


nohup streamlit run "$BASE_DIR/dashboard/app.py" --server.port 8501 --server.headless true > "$LOG_DIR/streamlit_${MODE}.log" 2>&1 &

echo "Consumers and dashboard are running. Waiting 5 seconds for them to initialize..."
sleep 5

echo "STEP 3: Launching the drift_test_producer. This will run for about 40 seconds."
echo ">>> Watch the terminal for phase changes and your Slack for alerts. <<<"
python "$BASE_DIR/producers/drift_test_producer.py"

echo "----------------------------------------"
echo " Demo complete."
echo "Run 'bash stop_pipeline.sh' to stop all background processes."