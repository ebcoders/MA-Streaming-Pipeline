#!/usr/bin/env python3
"""
slack_alert_consumer.py — sends Slack alerts for drift or high M&A likelihood.
Requires SLACK_WEBHOOK_URL env variable.
"""
import os
import json
import logging
import requests
from kafka import KafkaConsumer

# --- Configuration ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:92")
WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")
TOPICS = ["drift_alerts", "ma_prediction_stream"]

# --- Setup ---
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("slack_alert_consumer")

if not WEBHOOK:
    log.warning("⚠ SLACK_WEBHOOK_URL not set — Slack alerts disabled.")

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[KAFKA_BROKER],
    group_id="slack-alert-group",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    consumer_timeout_ms=1000,
)

def send_msg(txt):
    if not WEBHOOK: return
    try:
        requests.post(WEBHOOK, json={"text": txt})
        log.info(" Slack message sent")
    except Exception as e:
        log.error("Slack send failed: %s", e)

log.info(" Slack Alert Consumer started.")
try:
    while True:
        for msg in consumer:
            topic = msg.topic
            p = msg.value

            if topic == "drift_alerts":
                company = p.get('company', 'N/A'); feature = p.get('feature', 'N/A')
                old_mean = p.get('old_mean', 0.0); new_mean = p.get('new_mean', 0.0)
                delta = p.get('delta', 0.0)
                alert = f"""
🚨 *Data Drift Detected!*
*Company:* {company}
*Feature:* `{feature}`
*Change (Delta):* `Δ = {delta:.4f}`
*Mean Before:* `{old_mean:.4f}`
*Mean After:* `{new_mean:.4f}`
                """
                send_msg(alert)

            elif topic == "ma_prediction_stream":
                prob = float(p.get("ma_probability", 0))
                if prob >= 0.80:
                    txt = f"""
🔥 *High M&A Probability Alert!*
*Company:* {p['company']}
*Probability:* {prob:.2%}
                    """
                    send_msg(txt)
except KeyboardInterrupt:
    log.info("Stopped.")