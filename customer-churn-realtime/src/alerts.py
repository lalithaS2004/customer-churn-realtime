"""
Early Warning Alert System.

Generates human-readable alerts for customers whose churn probability
and/or health score cross configured thresholds, along with plain
explanations of *why* the alert fired.
"""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
import config


def classify_risk(churn_probability: float) -> str:
    if churn_probability >= config.ALERT_CRITICAL_THRESHOLD:
        return "Critical"
    if churn_probability >= config.ALERT_HIGH_THRESHOLD:
        return "High"
    if churn_probability >= 0.35:
        return "Medium"
    return "Low"


def build_reasons(engineered_row: dict) -> list:
    """Translate engineered feature values into plain-language reasons."""
    reasons = []

    if engineered_row.get("support_call_rate", 0) > 0.5:
        reasons.append("Increased support calls")
    if engineered_row.get("usage_change", 0) < -0.15:
        reasons.append("Reduced usage")
    if engineered_row.get("payment_failures", 0) >= 1:
        reasons.append("Payment failure detected")
    if engineered_row.get("contract") == "Month-to-month":
        reasons.append("Month-to-month contract (no long-term commitment)")
    if engineered_row.get("complaint_frequency", 0) > 0.3:
        reasons.append("Frequent complaints")
    if engineered_row.get("service_interruptions", 0) >= 2:
        reasons.append("Repeated service interruptions")
    if engineered_row.get("satisfaction_score", 10) <= 4:
        reasons.append("Low satisfaction score")
    if engineered_row.get("tenure", 999) < 6:
        reasons.append("New customer (low tenure, higher churn risk)")

    if not reasons:
        reasons.append("Elevated churn probability from overall behavior pattern")

    return reasons


def generate_alert(event_id: str, customer_id: str, engineered_row: dict,
                    churn_probability: float, health_score: float) -> dict:
    """
    Build an alert dict for a scored event. Caller decides whether to
    persist it (typically only for Medium/High/Critical risk levels).
    """
    risk_level = classify_risk(churn_probability)
    reasons = build_reasons(engineered_row)
    return {
        "event_id": event_id,
        "customer_id": customer_id,
        "churn_probability": round(churn_probability, 4),
        "health_score": health_score,
        "risk_level": risk_level,
        "reasons": reasons,
    }


def should_alert(risk_level: str) -> bool:
    """Only raise alerts for Medium risk and above to avoid noise."""
    return risk_level in ("Medium", "High", "Critical")


def format_alert_text(alert: dict) -> str:
    icon = {"Low": "🟢", "Medium": "🟡", "High": "🟠", "Critical": "🔴"}.get(
        alert["risk_level"], "⚪"
    )
    reasons_text = "\n".join(f"  • {r}" for r in alert["reasons"])
    return (
        f"{icon} {alert['risk_level'].upper()} CHURN RISK\n\n"
        f"Customer: {alert['customer_id']}\n"
        f"Churn Probability: {alert['churn_probability'] * 100:.1f}%\n"
        f"Health Score: {alert['health_score']}\n\n"
        f"Reasons:\n{reasons_text}"
    )
