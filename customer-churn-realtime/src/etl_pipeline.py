"""
ETL Pipeline.

Extract  -> raw JSON customer event (from Kafka, passed in as a dict)
Transform -> validate, clean, engineer features
Load      -> persist processed event + prediction + (optional) alert to SQLite

This module is intentionally decoupled from Kafka itself: `process_event`
takes a plain dict so it can be unit-tested or reused outside the
consumer (e.g. from a notebook or a batch backfill script).
"""
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))
from src import alerts, database, health_score, prediction
from src.feature_engineering import engineer_features, validate_event

# In-memory cache of last-seen data_usage per customer, used to compute a
# real usage_change for streaming events (reset when the app restarts).
_previous_usage_cache: dict = {}


class ETLError(Exception):
    """Raised when an event cannot be processed."""


def extract(raw_event: dict) -> dict:
    """Extract step: currently a passthrough since Kafka already hands us
    a parsed dict, but kept as an explicit stage for clarity/extension."""
    return dict(raw_event)


def transform(raw_event: dict) -> dict:
    """Validate + clean + engineer features. Returns an engineered dict."""
    is_valid, error = validate_event(raw_event)
    if not is_valid:
        raise ETLError(f"Invalid event: {error}")

    # Duplicate detection is handled at load time via the DB's UNIQUE
    # constraint on event_id (INSERT OR IGNORE), so we simply proceed here.

    df = pd.DataFrame([raw_event])
    customer_id = raw_event.get("customer_id")
    engineered_df = engineer_features(df, previous_usage=_previous_usage_cache)
    engineered_row = engineered_df.iloc[0].to_dict()

    # Update usage cache for the next event from this customer
    if customer_id is not None:
        _previous_usage_cache[customer_id] = engineered_row.get("data_usage", 0)

    return engineered_row


def load(engineered_row: dict, pred: dict, health: dict, event_id: str):
    """Persist processed event, prediction, and (if warranted) an alert."""
    row_for_db = dict(engineered_row)
    row_for_db["event_id"] = event_id
    database.insert_processed_event(row_for_db)

    database.insert_prediction(
        {
            "event_id": event_id,
            "customer_id": engineered_row["customer_id"],
            "churn_probability": pred["churn_probability"],
            "risk_level": pred["risk_level"],
            "health_score": health["health_score"],
            "health_status": health["health_status"],
            "model_name": pred["model_name"],
        }
    )

    database.upsert_customer_snapshot(
        customer_id=engineered_row["customer_id"],
        event=engineered_row,
        churn_probability=pred["churn_probability"],
        risk_level=pred["risk_level"],
        health_score=health["health_score"],
        health_status=health["health_status"],
    )

    alert = None
    if alerts.should_alert(pred["risk_level"]):
        alert = alerts.generate_alert(
            event_id=event_id,
            customer_id=engineered_row["customer_id"],
            engineered_row=engineered_row,
            churn_probability=pred["churn_probability"],
            health_score=health["health_score"],
        )
        database.insert_alert(alert)

    return alert


def process_event(raw_event: dict) -> dict:
    """
    Run a raw customer event through the full ETL + ML + scoring pipeline.

    Returns a result dict with the engineered row, prediction, health
    score, and alert (if any) -- useful for logging/debugging and for
    the "Live Streaming" dashboard page.
    """
    event_id = raw_event.get("event_id") or str(uuid.uuid4())
    raw_event = dict(raw_event)
    raw_event.setdefault("timestamp", datetime.now(timezone.utc).isoformat())

    engineered_row = transform(extract(raw_event))

    try:
        pred = prediction.predict_churn(engineered_row)
    except prediction.ModelUnavailableError as e:
        raise ETLError(str(e))

    health = health_score.compute_health_score(engineered_row, pred["churn_probability"])
    alert = load(engineered_row, pred, health, event_id)

    return {
        "event_id": event_id,
        "engineered_row": engineered_row,
        "prediction": pred,
        "health": health,
        "alert": alert,
    }
