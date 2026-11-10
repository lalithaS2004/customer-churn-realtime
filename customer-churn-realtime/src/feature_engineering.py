"""
Feature engineering shared by:
  - Offline model training (train_models.py)
  - Real-time ETL pipeline (etl_pipeline.py)

Keeping this logic in one place guarantees that the features seen by the
model at training time and at inference time are computed identically.
"""
import numpy as np
import pandas as pd

CONTRACT_RISK_MAP = {
    "Month-to-month": 1.0,
    "One year": 0.5,
    "Two year": 0.15,
}

CATEGORICAL_COLUMNS = ["contract", "internet_service", "payment_method"]

# Columns the final model is trained/predicted on (order matters for the
# ColumnTransformer defined in train_models.py / prediction.py)
NUMERIC_FEATURES = [
    "tenure",
    "monthly_charges",
    "total_charges",
    "data_usage",
    "call_usage",
    "support_calls",
    "payment_failures",
    "complaints",
    "login_activity",
    "service_interruptions",
    "satisfaction_score",
    "support_call_rate",
    "payment_failure_rate",
    "usage_change",
    "complaint_frequency",
    "customer_activity_score",
    "contract_risk",
]
CATEGORICAL_FEATURES = CATEGORICAL_COLUMNS
ALL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES


def engineer_features(df: pd.DataFrame, previous_usage: dict = None) -> pd.DataFrame:
    """
    Add derived churn-related features to a dataframe of raw customer
    records. Works for both a full historical dataframe (training) and a
    single-row dataframe (real-time event), as long as required raw
    columns are present.

    `previous_usage` (optional): dict mapping customer_id -> previous
    data_usage value, used to compute a real `usage_change` for
    streaming events instead of the neutral default.
    """
    df = df.copy()

    # Ensure required raw columns exist with sane defaults
    for col, default in [
        ("tenure", 0), ("monthly_charges", 0.0), ("total_charges", 0.0),
        ("data_usage", 0.0), ("call_usage", 0.0), ("support_calls", 0),
        ("payment_failures", 0), ("complaints", 0), ("login_activity", 20.0),
        ("service_interruptions", 0), ("satisfaction_score", 7.0),
        ("contract", "Month-to-month"),
    ]:
        if col not in df.columns:
            df[col] = default
        df[col] = df[col].fillna(default)

    tenure_safe = df["tenure"].replace(0, 1)

    # support_call_rate: support calls normalized by tenure (calls/month)
    df["support_call_rate"] = df["support_calls"] / tenure_safe

    # payment_failure_rate: payment failures normalized by tenure
    df["payment_failure_rate"] = df["payment_failures"] / tenure_safe

    # usage_change: percentage change vs previous known usage (streaming),
    # or vs a population-neutral baseline (0.0 = "no known trend") for
    # historical/batch training data where no prior reading exists.
    if previous_usage:
        def _calc_change(row):
            prev = previous_usage.get(row.get("customer_id"))
            if prev is None or prev == 0:
                return 0.0
            return (row["data_usage"] - prev) / prev
        df["usage_change"] = df.apply(_calc_change, axis=1)
    elif "usage_change" not in df.columns:
        df["usage_change"] = 0.0

    # complaint_frequency: complaints per month of tenure
    df["complaint_frequency"] = df["complaints"] / tenure_safe

    # customer_activity_score: composite of login activity, penalized by
    # service interruptions and low satisfaction
    df["customer_activity_score"] = (
        df["login_activity"].clip(lower=0)
        - df["service_interruptions"] * 1.5
        + (df["satisfaction_score"] - 5) * 1.0
    ).clip(lower=0, upper=100)

    # contract_risk: numeric risk implied by contract type
    df["contract_risk"] = df["contract"].map(CONTRACT_RISK_MAP).fillna(0.7)

    return df


def validate_event(event: dict) -> (bool, str):
    """Basic validation for an incoming raw customer event dict."""
    required = ["customer_id", "tenure", "monthly_charges"]
    for field in required:
        if field not in event or event[field] is None:
            return False, f"Missing required field: {field}"
    try:
        if float(event["tenure"]) < 0:
            return False, "tenure cannot be negative"
        if float(event["monthly_charges"]) < 0:
            return False, "monthly_charges cannot be negative"
    except (TypeError, ValueError):
        return False, "tenure/monthly_charges must be numeric"
    return True, ""
