"""
Retention Analysis (rule-based).

For at-risk customers, suggests plausible retention actions based on
observed behavioral signals. These are heuristic, rule-based
recommendations only -- NOT a guarantee that any action will prevent
churn. Always label them as such in the UI.
"""

RULES = [
    {
        "condition": lambda r: r.get("monthly_charges", 0) > 80 and r.get("contract") == "Month-to-month",
        "signal": "High price sensitivity",
        "action": "Recommend discount or plan review",
        "priority": 3,
    },
    {
        "condition": lambda r: r.get("support_call_rate", 0) > 0.5 or r.get("complaint_frequency", 0) > 0.3,
        "signal": "High support complaints",
        "action": "Recommend priority customer support outreach",
        "priority": 3,
    },
    {
        "condition": lambda r: r.get("usage_change", 0) < -0.15,
        "signal": "Usage declining",
        "action": "Recommend engagement offer (bonus data, promo content)",
        "priority": 2,
    },
    {
        "condition": lambda r: r.get("payment_failures", 0) >= 1,
        "signal": "Payment problems",
        "action": "Recommend payment assistance / flexible billing options",
        "priority": 3,
    },
    {
        "condition": lambda r: r.get("service_interruptions", 0) >= 2,
        "signal": "Frequent service interruptions",
        "action": "Recommend technical service review / infrastructure check",
        "priority": 2,
    },
    {
        "condition": lambda r: r.get("satisfaction_score", 10) <= 4,
        "signal": "Low satisfaction score",
        "action": "Recommend proactive relationship/account-manager check-in",
        "priority": 2,
    },
    {
        "condition": lambda r: r.get("contract") == "Month-to-month" and r.get("tenure", 0) > 12,
        "signal": "Long-tenure customer without contract lock-in",
        "action": "Recommend loyalty discount for switching to an annual contract",
        "priority": 1,
    },
]


def recommend_actions(engineered_row: dict) -> list:
    """
    Returns a list of dicts: {signal, action, priority} for every rule
    that matches the given engineered customer record, sorted by
    priority (highest first). Rule-based only -- not a guarantee.
    """
    matches = []
    for rule in RULES:
        try:
            if rule["condition"](engineered_row):
                matches.append(
                    {
                        "signal": rule["signal"],
                        "action": rule["action"],
                        "priority": rule["priority"],
                    }
                )
        except Exception:
            continue
    matches.sort(key=lambda m: -m["priority"])
    return matches


def priority_level(matches: list) -> str:
    if not matches:
        return "None"
    top = max(m["priority"] for m in matches)
    return {3: "High Priority", 2: "Medium Priority", 1: "Low Priority"}.get(top, "Low Priority")
