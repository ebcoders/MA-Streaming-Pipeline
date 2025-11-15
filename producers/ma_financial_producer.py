#!/usr/bin/env python3
"""
ma_financial_producer.py — emits financial features.
Supports three modes:
1. dataset: Replays data from a CSV file.
2. api: Fetches live financial data from the SEC EDGAR API. (FIXED for AMZN)
3. synthetic: Generates random data for testing.
"""
import os, time, json, csv, logging, random, requests
from datetime import datetime, timezone
from kafka import KafkaProducer
import pandas as pd

# --- Configuration ---
BASE_DIR = os.path.expanduser(os.getenv("BASE_DIR","~/bda2"))
MODE = os.getenv("MODE","dataset").lower()
DATASET = os.path.expanduser(os.getenv("FIN_DATA", f"{BASE_DIR}/data/fda8_financial_dataset.csv"))
OUT_CSV = os.path.expanduser(f"{BASE_DIR}/data/ma_financial_stream.csv")
TOPIC = "ma_financial_stream"
TIME_MULTIPLIER = float(os.getenv("TIME_MULTIPLIER","1.0"))
FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:0>10}.json"
SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "YourName YourEmail@example.com")
COMPANIES_CIK = {
    "AAPL": 320193, "MSFT": 789019, "AMZN": 1018724,
    "GOOGL": 1652044, "META": 1326801
}

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("ma_financial_producer")
producer = KafkaProducer(bootstrap_servers=[os.getenv("KAFKA_BROKER","localhost:9092")],
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))

def emit(msg):
    producer.send(TOPIC, msg); producer.flush()
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    with open(OUT_CSV,"a",newline="") as f:
        csv.writer(f).writerow([msg["company"], msg["timestamp"], json.dumps(msg.get("features",{})), msg.get("source","")])

def fetch_xbrl(cik):
    try:
        url = FACTS_URL.format(cik=cik)
        log.info("Fetching SEC data from: %s", url)
        r = requests.get(url, headers={"User-Agent": SEC_USER_AGENT}, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning("XBRL fetch failed for CIK %s: %s", cik, e)
        return None

if MODE == "dataset":
    log.info("FIN producer running in DATASET mode, file=%s", DATASET)
    if not os.path.exists(DATASET):
        log.error("Dataset not found at %s. Exiting.", DATASET)
        exit(1)
    df = pd.read_csv(DATASET).sort_values("timestamp")
    prev_ts = None
    for _, row in df.iterrows():
        ts = pd.to_datetime(row["timestamp"])
        if prev_ts is not None:
            delta = max(0.01, (ts - prev_ts).total_seconds()/3600.0)
            time.sleep(delta / TIME_MULTIPLIER)
        prev_ts = ts
        features = {k: float(row.get(k,0)) for k in ("liquidity_ratio","cash_to_assets","leverage_ratio","profit_margin")}
        msg = {"company": row["company"], "timestamp": ts.to_pydatetime().astimezone(timezone.utc).isoformat(),
               "features": features, "source":"dataset"}
        emit(msg); log.info("FIN dataset -> %s", row["company"])
    log.info("FIN dataset stream finished.")

elif MODE == "api":
    log.info("FIN producer running in API mode (SEC XBRL). User-Agent: %s", SEC_USER_AGENT)
    COMPANIES = os.getenv("COMPANIES", "AAPL,MSFT,AMZN,GOOGL,META").split(",")
    try:
        while True:
            for company_ticker in COMPANIES:
                cik = COMPANIES_CIK.get(company_ticker)
                if not cik:
                    log.warning("No CIK found for %s, skipping.", company_ticker)
                    continue

                data = fetch_xbrl(cik)
                features = None
                source = "xbrl_fail"

                if data:
                    facts = data.get("facts", {}).get("us-gaap", {})
                    
                    def get_latest_fact_value(fact_name_list):
                        """
                        Safely extracts the most recent value for a given fact.
                        Tries a list of possible tag names.
                        """
                        for fact_name in fact_name_list:
                            try:
                                all_filings = facts[fact_name]["units"]["USD"]
                                return float(all_filings[-1]["val"])
                            except (KeyError, IndexError, TypeError):
                                continue 
                        return None

                    assets = get_latest_fact_value(["Assets"])
                    cash = get_latest_fact_value(["CashAndCashEquivalentsAtCarryingValue"])
                    net_income = get_latest_fact_value(["NetIncomeLoss"])
                    
                    revenues = get_latest_fact_value([
                        "Revenues",
                        "RevenueFromContractWithCustomerExcludingAssessedTax" # Amazon's tag
                    ])

                    liabilities = get_latest_fact_value(["Liabilities"])
                    if liabilities is None:
                        liab_current = get_latest_fact_value(["LiabilitiesCurrent"])
                        liab_noncurrent = get_latest_fact_value(["LiabilitiesNoncurrent"])
                        if liab_current is not None and liab_noncurrent is not None:
                            liabilities = liab_current + liab_noncurrent
                    if all(v is not None for v in [assets, liabilities, cash, revenues, net_income]):
                        features = {
                            "liquidity_ratio": round(cash / liabilities, 4) if liabilities else 0.0,
                            "cash_to_assets": round(cash / assets, 4) if assets else 0.0,
                            "leverage_ratio": round(liabilities / assets, 4) if assets else 0.0,
                            "profit_margin": round(net_income / revenues, 4) if revenues else 0.0
                        }
                        source = "xbrl"
                    else:
                        log.warning("Incomplete XBRL data for %s. Missing one or more key facts.", company_ticker)
                        source = "xbrl_incomplete"

                if features is None:
                    log.info("Using synthetic fallback for %s.", company_ticker)
                    features = { "liquidity_ratio": round(random.uniform(0.5, 3.0), 3), "cash_to_assets": round(random.uniform(0.1, 0.6), 3), "leverage_ratio": round(random.uniform(0.2, 0.9), 3), "profit_margin": round(random.uniform(-0.2, 0.4), 3) }
                    source = "synthetic_fallback"

                msg = {"company": company_ticker, "timestamp": datetime.now(timezone.utc).isoformat(),
                       "features": features, "source": source}
                emit(msg)
                log.info("FIN api -> %s (source: %s)", company_ticker, source)
                
                time.sleep(1.0 / TIME_MULTIPLIER)
                
    except KeyboardInterrupt:
        log.info("Stopping financial producer.")

else:
    log.info("FIN producer running in SYNTHETIC mode.")
    COMPANIES = ["AAPL", "MSFT", "AMZN", "GOOGL", "META"]
    try:
        while True:
            for c in COMPANIES:
                features = { "liquidity_ratio": round(random.uniform(0.5, 3.0), 3), "cash_to_assets": round(random.uniform(0.1, 0.6), 3), "leverage_ratio": round(random.uniform(0.2, 0.9), 3), "profit_margin": round(random.uniform(-0.2, 0.4), 3)}
                msg = {"company": c, "timestamp": datetime.now(timezone.utc).isoformat(), "features": features, "source":"synthetic"}
                emit(msg)
                log.info("FIN synthetic -> %s", c)
                time.sleep(1.0 / TIME_MULTIPLIER)
    except KeyboardInterrupt:
        log.info("Stopping financial producer.")