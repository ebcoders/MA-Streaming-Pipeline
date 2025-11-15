#!/usr/bin/env bash
# kafka_setup.sh — format & start single-node KRaft Kafka, create topics
set -euo pipefail
BASE_DIR="${BASE_DIR:-$HOME/bda2}"
KAFKA_DIR="${KAFKA_DIR:-$HOME/kafka}"
CONF="${CONF:-$BASE_DIR/config/kraft-server.properties}"
KRAFT_DATA="${KRAFT_DATA:-$BASE_DIR/kafka-data}"
LOG_DIR="${LOG_DIR:-$BASE_DIR/logs}"
TOPICS=("ma_text_stream" "ma_financial_stream" "ma_sentiment_stream" "ma_volatility_stream" "ma_prediction_stream" "drift_alerts" "retrain_events" "ma_labels")

mkdir -p "$KRAFT_DATA" "$LOG_DIR"

timestamp() { date -u +"%a %b %d %T UTC %Y"; }
echo "[SETUP][$(timestamp)]  Kafka directory found at $KAFKA_DIR"
echo "[SETUP][$(timestamp)]  Using config: $CONF"
echo "[SETUP][$(timestamp)]  KRaft log directory: $KRAFT_DATA"

echo "[SETUP][$(timestamp)] Stopping any running Kafka brokers..."
pkill -f kafka.Kafka || true
pkill -f kafka.storage.StorageTool || true
sleep 1

echo "[SETUP][$(timestamp)] Wiping old Kafka data..."
rm -rf "$KRAFT_DATA"
mkdir -p "$KRAFT_DATA"

echo "[SETUP][$(timestamp)] Generating new cluster ID..."
CLUSTER_ID=$("$KAFKA_DIR/bin/kafka-storage.sh" random-uuid)
echo "[SETUP][$(timestamp)]  Cluster ID = $CLUSTER_ID"

echo "[SETUP][$(timestamp)] Formatting storage..."
"$KAFKA_DIR/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$CONF" > "$LOG_DIR/kafka_storage_format.log" 2>&1 || {
  echo "[SETUP][$(timestamp)] ERROR: storage format failed. Check $LOG_DIR/kafka_storage_format.log"
  exit 1
}
echo "[SETUP][$(timestamp)]  Storage formatted."

echo "[SETUP][$(timestamp)] Starting Kafka..."
nohup "$KAFKA_DIR/bin/kafka-server-start.sh" "$CONF" > "$LOG_DIR/kafka_setup_start.log" 2>&1 &
sleep 6

if nc -z localhost 9092; then
  echo "[SETUP][$(timestamp)]  Kafka broker is running on 9092"
else
  echo "[SETUP][$(timestamp)] ERROR: Kafka broker not listening on 9092. Check $LOG_DIR/kafka_setup_start.log"
  exit 1
fi

echo "[SETUP][$(timestamp)] Creating topics..."
for t in "${TOPICS[@]}"; do
  "$KAFKA_DIR/bin/kafka-topics.sh" --create --topic "$t" --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>/dev/null || true
done

echo "[SETUP][$(timestamp)]  All topics created."
echo "[SETUP][$(timestamp)] Kafka setup completed successfully."
