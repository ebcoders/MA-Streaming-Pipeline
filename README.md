# MA-Streaming-Pipeline – Real-Time M&A Prediction System

This repository houses the complete implementation of MA-Streaming-Pipeline, a cutting-edge real-time analytics platform built to predict Mergers & Acquisitions (M&A) activity with high accuracy. The system utilizes a modern streaming architecture powered by Apache Kafka for data orchestration, River for incremental machine learning, and CapyMOA-based drift detection to maintain continuous model performance.

## Features

* **Comprehensive Streaming Architecture**: Fully automated data flow from ingestion to actionable insights.
* **Real-Time Data Integration**: Connects to live data streams from SEC EDGAR, yfinance, and HuggingFace APIs.
* **Sophisticated Online Learning**: Implements a Stacked Ensemble approach using River for complex, real-time predictions on multimodal datasets.
* **Adaptive Drift Management**: Employs ADWIN-based concept drift detection with automated closed-loop model retraining for sustained accuracy.
* **Live Explainability System (XAI)**: Dedicated consumer service for tracking and visualizing dynamic feature importance patterns.
* **Multi-Tab Monitoring Dashboard**: Interactive Streamlit interface for observing predictions, feature weights, and drift occurrences in real time.
* **Instant Slack Notifications**: Automated alert system for high-probability M&A predictions and critical drift warnings.

## System Architecture

The pipeline follows a decoupled, event-driven design with Apache Kafka serving as the central message broker. Data producers feed information from external APIs, which is then analyzed in parallel by specialized consumer services. Results are displayed on a dashboard and sent via Slack notifications.

**Click to view Mermaid Code for Architecture Diagram**

**Click to view Mermaid Code for Architecture Diagram**

## Quick Start & Setup

Follow the instructions below to configure and launch the complete pipeline.

### 1. Prerequisites

* **Apache Kafka**: A functioning Kafka installation is required. This project assumes a local setup at `~/kafka`.
* **Python 3.8+**: A recent Python version is necessary.

### 2. Installation & Configuration

**Clone the Repository:**

```bash
git clone https://github.com/Rishab-Bo/Stream-MA-Prediction-Pipeline
cd <your-repo-directory>
```

**Create a Python Virtual Environment:**

```bash
python -m venv venv
source venv/bin/activate
```

**Install Dependencies:** (A `requirements.txt` file should be created for this step, containing all libraries below)

```bash
pip install kafka-python river pandas scikit-learn streamlit streamlit-autorefresh plotly requests yfinance
```

**Configure Environment Variables:** Copy the template `.env` file and populate it with your credentials.

```bash
cp config/template.env .env
```

Now, edit the `.env` file with your information:

```bash
# .env
KAFKA_BROKER=localhost:9092
BASE_DIR=/path/to/your/project/folder # IMPORTANT: Use the absolute path

# API Keys & URLs
HF_TOKEN="hf_YourHuggingFaceToken"
SLACK_WEBHOOK_URL="https://hooks.slack.com/services/Your/Slack/Webhook"
SEC_USER_AGENT="YourName YourEmail@example.com" # Required by SEC
```

### 3. Running the Pipeline (Full Workflow)

Execute these steps sequentially to launch the pipeline from a fresh state.

**Run Kafka Setup (One-Time):** This script formats the KRaft storage and establishes all required Kafka topics.

```bash
bash kafka_setup.sh
```

**Generate Datasets:** This produces the `fda8_*_dataset.csv` files utilized by the producers in dataset mode and for initial model training.

```bash
python data/generate_dataset.py
```

**Perform Initial Model Training (CRITICAL):** This trains the baseline version of the predictive model, ensuring the pipeline can generate meaningful predictions from the start rather than beginning "cold".

```bash
python initial_train.py
```

**Run the Full Pipeline:** This script launches all producers and consumers. The mode parameter accepts `dataset`, `api`, or `synthetic`. The `api` mode is recommended for live data streams.

```bash
bash run_pipeline.sh api
```

**View the Dashboard:** Open your web browser and navigate to `http://localhost:8501`.

### 4. Demo Mode

To demonstrate the system's alerting functionality in a controlled environment, use the dedicated demo script. This automatically starts the consumer services and runs a specialized producer to trigger a drift alert followed by a high-M&A-probability notification.

```bash
# Ensure you have completed steps 1-3 from the full workflow above first.
bash run_demo.sh
```

### 5. Stopping & Resetting

To terminate all pipeline processes:

```bash
bash stop_pipeline.sh
```

To perform a complete reset (removes all logs, data, models, and Kafka topics):

```bash
bash reset_pipeline.sh
```

## Troubleshooting

* **Predictions are stuck at 0.5**: This indicates an untrained model. You must execute the `initial_train.py` script before starting the pipeline.
* **Slack alerts are not being sent**:
  * Verify that your `SLACK_WEBHOOK_URL` in the `.env` file is accurate.
  * Confirm the `slack_alert_consumer.py` process is active (`ps aux | grep slack`).
* **Producers in API mode fail**:
  * Validate your `HF_TOKEN` is correct.
  * Ensure you have configured a valid `SEC_USER_AGENT` as mandated by the SEC.
* **Kafka connection errors**:
  * Confirm your Kafka server is operational.
  * Verify that `KAFKA_BROKER` in your `.env` file points to the correct address (e.g., `localhost:9092`).
