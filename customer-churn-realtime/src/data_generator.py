"""
Base population dataset generator.

This project expects a telecom customer churn dataset such as the public
"Telco Customer Churn" dataset as its *base/reference population* (see
README section "Data"). Since no dataset file was supplied with this
project, this module generates a realistic synthetic dataset with the
same kind of fields, so that:

  1. `train_models.py` has data to train on.
  2. `simulator.py` has a base customer population to draw from and
     perturb over time.

If you have a real telecom churn CSV (e.g. Kaggle's Telco Customer
Churn dataset), just drop it at `data/raw/telecom_customer_churn.csv`
with equivalent column names and skip running this generator -- every
other module only cares about the column names, not the origin of the
data.

Run directly:
    python -m src.data_generator
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))
import config

RANDOM_SEED = 42
rng = np.random.default_rng(RANDOM_SEED)

CONTRACT_TYPES = ["Month-to-month", "One year", "Two year"]
INTERNET_SERVICES = ["DSL", "Fiber optic", "No"]
PAYMENT_METHODS = [
    "Electronic check",
    "Mailed check",
    "Bank transfer (automatic)",
    "Credit card (automatic)",
]


def _make_customer_id(i: int) -> str:
    """Mimic the well-known 'XXXX-XXXXX' style customer ID format."""
    digits = rng.integers(1000, 9999)
    letters = "".join(rng.choice(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), size=5))
    return f"{digits}-{letters}"


def generate_base_population(n_customers: int = 2000) -> pd.DataFrame:
    """Generate a synthetic base population of telecom customers."""
    rows = []
    for i in range(n_customers):
        tenure = int(rng.integers(0, 73))  # months, matches typical Telco range 0-72
        contract = rng.choice(CONTRACT_TYPES, p=[0.55, 0.24, 0.21])
        internet_service = rng.choice(INTERNET_SERVICES, p=[0.35, 0.44, 0.21])
        payment_method = rng.choice(PAYMENT_METHODS)

        base_monthly = rng.normal(65, 25)
        if internet_service == "Fiber optic":
            base_monthly += 15
        elif internet_service == "No":
            base_monthly -= 25
        monthly_charges = float(np.clip(base_monthly, 18.0, 120.0))
        total_charges = float(monthly_charges * tenure * rng.uniform(0.9, 1.05) + rng.normal(0, 20))
        total_charges = max(total_charges, monthly_charges)

        data_usage = float(np.clip(rng.normal(12 if internet_service != "No" else 0.2, 6), 0, 60))
        call_usage = float(np.clip(rng.normal(300, 120), 0, 1200))

        # Underlying "risk propensity" drives correlated churn-related behavior
        risk_propensity = rng.beta(2, 5)  # skewed toward lower risk
        if contract == "Month-to-month":
            risk_propensity = min(1.0, risk_propensity + 0.15)
        if tenure < 6:
            risk_propensity = min(1.0, risk_propensity + 0.1)

        support_calls = int(np.clip(rng.poisson(1 + risk_propensity * 5), 0, 15))
        payment_failures = int(np.clip(rng.poisson(risk_propensity * 2), 0, 8))
        complaints = int(np.clip(rng.poisson(risk_propensity * 3), 0, 10))
        login_activity = float(np.clip(rng.normal(30 - risk_propensity * 20, 6), 0, 40))
        service_interruptions = int(np.clip(rng.poisson(risk_propensity * 2), 0, 10))
        satisfaction_score = float(np.clip(rng.normal(8 - risk_propensity * 5, 1.2), 1, 10))

        churn_prob = np.clip(
            0.05
            + 0.35 * risk_propensity
            + 0.10 * (contract == "Month-to-month")
            + 0.05 * (payment_failures > 1)
            + 0.05 * (complaints > 3)
            - 0.15 * (tenure > 24)
            - 0.05 * (contract == "Two year"),
            0.02,
            0.95,
        )
        churn = int(rng.random() < churn_prob)

        rows.append(
            {
                "customer_id": _make_customer_id(i),
                "tenure": tenure,
                "monthly_charges": round(monthly_charges, 2),
                "total_charges": round(total_charges, 2),
                "contract": contract,
                "internet_service": internet_service,
                "payment_method": payment_method,
                "data_usage": round(data_usage, 2),
                "call_usage": round(call_usage, 1),
                "support_calls": support_calls,
                "payment_failures": payment_failures,
                "complaints": complaints,
                "login_activity": round(login_activity, 1),
                "service_interruptions": service_interruptions,
                "satisfaction_score": round(satisfaction_score, 1),
                "risk_propensity": round(float(risk_propensity), 4),  # kept for simulator use
                "churn": churn,
            }
        )

    return pd.DataFrame(rows)


def main():
    out_path = Path(config.RAW_DATA_PATH)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = generate_base_population(n_customers=2000)
    df.to_csv(out_path, index=False)
    print(f"Generated {len(df)} synthetic base customers -> {out_path}")
    print(f"Churn rate: {df['churn'].mean():.2%}")


if __name__ == "__main__":
    main()
