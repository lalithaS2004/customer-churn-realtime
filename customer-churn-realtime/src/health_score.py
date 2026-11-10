"""
Customer Health Score (0-100).

A transparent, rule-based composite score representing a customer's
overall relationship health with the telecom company. This is
intentionally separate from (but informed by) the ML churn probability,
so the logic stays easy to explain and tune.

Weighting (sums to 100 points available, then scaled):
  - Churn risk (inverse of churn probability)         : 35%
  - Tenure (loyalty)                                   : 15%
  - Payment behavior (fewer failures = better)         : 15%
  - Support interactions (fewer/lower rate = better)   : 15%
  - Usage trend (stable/increasing = better)           : 10%
  - Complaints (fewer = better)                        : 5%
  - Customer activity (higher = better)                : 5%

All weights are defined as module-level constants so they are easy to
retune for a class project or demo.
"""
import numpy as np

WEIGHTS = {
    "churn_risk": 0.35,
    "tenure": 0.15,
    "payment_behavior": 0.15,
    "support_interactions": 0.15,
    "usage_trend": 0.10,
    "complaints": 0.05,
    "activity": 0.05,
}


def _score_churn_risk(churn_probability: float) -> float:
    return float(np.clip(1.0 - churn_probability, 0, 1)) * 100


def _score_tenure(tenure_months: float) -> float:
    # Full marks at 36+ months tenure, scaled linearly below that.
    return float(np.clip(tenure_months / 36.0, 0, 1)) * 100


def _score_payment_behavior(payment_failure_rate: float) -> float:
    # payment_failure_rate is failures per month of tenure; 0 -> perfect,
    # 0.5+ failures/month -> worst.
    return float(np.clip(1.0 - (payment_failure_rate / 0.5), 0, 1)) * 100


def _score_support_interactions(support_call_rate: float) -> float:
    # support_call_rate is calls per month of tenure; 0 -> perfect,
    # 1.0+ calls/month -> worst.
    return float(np.clip(1.0 - support_call_rate, 0, 1)) * 100


def _score_usage_trend(usage_change: float) -> float:
    # usage_change is fractional change vs previous reading.
    # 0 (stable) or positive (increasing) -> good; strongly negative -> bad.
    if usage_change >= 0:
        return 100.0
    return float(np.clip(1.0 + usage_change, 0, 1)) * 100  # e.g. -0.5 -> 50


def _score_complaints(complaint_frequency: float) -> float:
    return float(np.clip(1.0 - (complaint_frequency / 0.5), 0, 1)) * 100


def _score_activity(customer_activity_score: float) -> float:
    # customer_activity_score already roughly 0-40+ scale from feature engineering
    return float(np.clip(customer_activity_score / 40.0, 0, 1)) * 100


def health_status_from_score(score: float) -> str:
    if score >= 75:
        return "Healthy"
    if score >= 50:
        return "Moderate"
    if score >= 30:
        return "At Risk"
    return "Critical"


def compute_health_score(engineered_row: dict, churn_probability: float) -> dict:
    """
    Compute the 0-100 health score and a breakdown of each component,
    so the dashboard can display *why* a customer got their score
    (transparency requirement).
    """
    components = {
        "churn_risk": _score_churn_risk(churn_probability),
        "tenure": _score_tenure(engineered_row.get("tenure", 0)),
        "payment_behavior": _score_payment_behavior(engineered_row.get("payment_failure_rate", 0)),
        "support_interactions": _score_support_interactions(engineered_row.get("support_call_rate", 0)),
        "usage_trend": _score_usage_trend(engineered_row.get("usage_change", 0)),
        "complaints": _score_complaints(engineered_row.get("complaint_frequency", 0)),
        "activity": _score_activity(engineered_row.get("customer_activity_score", 0)),
    }

    total = sum(components[k] * WEIGHTS[k] for k in WEIGHTS)
    total = float(np.clip(total, 0, 100))

    return {
        "health_score": round(total, 1),
        "health_status": health_status_from_score(total),
        "components": {k: round(v, 1) for k, v in components.items()},
        "weights": WEIGHTS,
    }
