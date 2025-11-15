#!/usr/bin/env bash
set -e
KAFKA_DIR="$HOME/kafka"
TOPICS=("ma_text_stream" "ma_financial_stream" "ma_sentiment_stream" \
        "ma_volatility_stream" "ma_prediction_stream" "drift_alerts" \
        "retrain_events" "ma_feature_importance")
for t in "${TOPICS[@]}"; do
  "$KAFKA_DIR/bin/kafka-topics.sh" --delete --topic "$t" --bootstrap-server localhost:9092 >/dev/null 2>&1 || true
done
echo "[KAFKA][$(date)] Topics deletion requested (some brokers may not allow delete immediately)."
