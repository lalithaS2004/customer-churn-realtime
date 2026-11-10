"""
Real-time churn prediction.

Loads the trained model pipeline (preprocessing + classifier) saved by
train_models.py, and exposes a simple function to score a single
engineered customer record.
"""
import sys
from pathlib import Path

import joblib
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))
import config
from src.feature_engineering import CATEGORICAL_FEATURES, NUMERIC_FEATURES

_model_cache = {"pipeline": None, "mtime": None}


class ModelUnavailableError(Exception):
    """Raised when the trained model file cannot be loaded."""


def load_model():
    """Load (and cache) the trained model pipeline from disk."""
    path = Path(config.MODEL_PATH)
    if not path.exists():
        raise ModelUnavailableError(
            f"No trained model found at {path}. Run `python -m src.train_models` first."
        )
    mtime = path.stat().st_mtime
    if _model_cache["pipeline"] is None or _model_cache["mtime"] != mtime:
        _model_cache["pipeline"] = joblib.load(path)
        _model_cache["mtime"] = mtime
    return _model_cache["pipeline"]


def get_model_name() -> str:
    import json
    meta_path = Path(config.MODEL_METADATA_PATH)
    if meta_path.exists():
        try:
            with open(meta_path) as f:
                return json.load(f).get("best_model", "unknown")
        except Exception:
            return "unknown"
    return "unknown"


def risk_level_from_probability(prob: float) -> str:
    if prob >= config.ALERT_CRITICAL_THRESHOLD:
        return "Critical"
    if prob >= config.ALERT_HIGH_THRESHOLD:
        return "High"
    if prob >= 0.35:
        return "Medium"
    return "Low"


def predict_churn(engineered_row: dict) -> dict:
    """
    Predict churn probability for a single engineered customer record
    (must already contain the engineered feature columns).

    Returns dict: {churn_probability, risk_level, model_name}
    """
    pipeline = load_model()
    row_df = pd.DataFrame([engineered_row])[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
    proba = float(pipeline.predict_proba(row_df)[:, 1][0])
    return {
        "churn_probability": proba,
        "risk_level": risk_level_from_probability(proba),
        "model_name": get_model_name(),
    }


def predict_churn_batch(engineered_df: pd.DataFrame) -> pd.Series:
    """Predict churn probabilities for a dataframe of engineered records."""
    pipeline = load_model()
    X = engineered_df[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
    return pd.Series(pipeline.predict_proba(X)[:, 1], index=engineered_df.index)
