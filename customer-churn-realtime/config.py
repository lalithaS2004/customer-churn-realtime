"""
Central configuration for the Customer Churn Real-Time Analytics System.

All modules import settings from here instead of reading environment
variables directly. This keeps configuration in one place and makes the
project easy to reconfigure without touching source code.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Project root (directory containing this file)
BASE_DIR = Path(__file__).resolve().parent

# Load variables from .env if present, otherwise defaults below are used.
load_dotenv(BASE_DIR / ".env")


def _get_path(env_var: str, default: str) -> str:
    """Resolve a possibly-relative path from an env var against BASE_DIR."""
    value = os.getenv(env_var, default)
    path = Path(value)
    if not path.is_absolute():
        path = BASE_DIR / path
    return str(path)


def _get_float(env_var: str, default: float) -> float:
    try:
        return float(os.getenv(env_var, default))
    except (TypeError, ValueError):
        return default


def _get_int(env_var: str, default: int) -> int:
    try:
        return int(os.getenv(env_var, default))
    except (TypeError, ValueError):
        return default


# --- Kafka ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "customer-events")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "churn-consumer-group")

# --- Database ---
DATABASE_PATH = _get_path("DATABASE_PATH", "data/processed/churn_analytics.db")

# --- Model ---
MODEL_PATH = _get_path("MODEL_PATH", "models/churn_model.pkl")
MODEL_METADATA_PATH = _get_path("MODEL_METADATA_PATH", "models/model_metadata.json")

# --- Data ---
RAW_DATA_PATH = _get_path("RAW_DATA_PATH", "data/raw/telecom_customer_churn.csv")

# --- Simulator ---
SIMULATION_INTERVAL = _get_float("SIMULATION_INTERVAL", 1.0)
SIMULATION_NUM_CUSTOMERS = _get_int("SIMULATION_NUM_CUSTOMERS", 200)
SIMULATION_RISK_PROBABILITY = _get_float("SIMULATION_RISK_PROBABILITY", 0.25)
SIMULATION_MAX_EVENTS = _get_int("SIMULATION_MAX_EVENTS", 0)  # 0 = unlimited

# --- Alerts ---
ALERT_HIGH_THRESHOLD = _get_float("ALERT_HIGH_THRESHOLD", 0.6)
ALERT_CRITICAL_THRESHOLD = _get_float("ALERT_CRITICAL_THRESHOLD", 0.8)

# Ensure required directories exist
Path(DATABASE_PATH).parent.mkdir(parents=True, exist_ok=True)
Path(MODEL_PATH).parent.mkdir(parents=True, exist_ok=True)
Path(RAW_DATA_PATH).parent.mkdir(parents=True, exist_ok=True)
