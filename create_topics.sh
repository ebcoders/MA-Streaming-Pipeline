#!/usr/bin/env bash
set -e
KAFKA_DIR="${KAFKA_DIR:-$HOME/kafka}"
echo "Topics currently in cluster:"
"$KAFKA_DIR/bin/kafka-topics.sh" --list --bootstrap-server localhost:9092
