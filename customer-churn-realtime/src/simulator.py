"""
Real-Time Customer Data Simulator.

Draws from the base customer population (data/raw/telecom_customer_churn.csv)
and generates continuously-evolving customer events -- it does NOT just
replay the same static CSV rows. Each simulated customer carries small
amounts of internal state (recent usage, complaint counts, etc.) that
drift over time, with two behavioral modes:

  - "normal" customers : stable usage, few complaints, no payment
    failures, regular activity.
  - "increasing-risk" customers : usage decreasing, complaints and
    support calls increasing, payment failures appearing, activity
    dropping -- i.e. a believable churn trajectory.

Can be run standalone (prints events + feeds the Kafka producer) or
imported and driven programmatically (e.g. by the Streamlit app for a
no-Kafka demo mode).
"""
import argparse
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))
import config

CONTRACT_TYPES = ["Month-to-month", "One year", "Two year"]


class CustomerState:
    """Mutable, evolving state for one simulated customer."""

    def __init__(self, base_row: dict, risk_mode: bool, rng: np.random.Generator):
        self.customer_id = base_row["customer_id"]
        self.rng = rng
        self.risk_mode = risk_mode

        self.tenure = float(base_row.get("tenure", 12))
        self.monthly_charges = float(base_row.get("monthly_charges", 65.0))
        self.total_charges = float(base_row.get("total_charges", self.monthly_charges * self.tenure))
        self.contract = base_row.get("contract", "Month-to-month")
        self.internet_service = base_row.get("internet_service", "Fiber optic")
        self.payment_method = base_row.get("payment_method", "Electronic check")

        self.data_usage = float(base_row.get("data_usage", 10.0))
        self.call_usage = float(base_row.get("call_usage", 300.0))
        self.support_calls = int(base_row.get("support_calls", 0))
        self.payment_failures = int(base_row.get("payment_failures", 0))
        self.complaints = int(base_row.get("complaints", 0))
        self.login_activity = float(base_row.get("login_activity", 25.0))
        self.service_interruptions = int(base_row.get("service_interruptions", 0))
        self.satisfaction_score = float(base_row.get("satisfaction_score", 8.0))

    def step(self) -> dict:
        """Advance this customer's state by one simulated tick and return
        a fresh event dict representing their current behavior."""
        r = self.rng

        # tenure creeps forward slowly (in fractional months per tick, for demo speed)
        self.tenure += r.uniform(0.01, 0.05)
        self.total_charges += self.monthly_charges * r.uniform(0.005, 0.02)

        if self.risk_mode:
            # Increasing-risk trajectory
            self.data_usage = max(0.0, self.data_usage * r.uniform(0.90, 0.98))
            self.call_usage = max(0.0, self.call_usage * r.uniform(0.92, 0.99))
            if r.random() < 0.35:
                self.support_calls += 1
            if r.random() < 0.15:
                self.payment_failures += 1
            if r.random() < 0.30:
                self.complaints += 1
            self.login_activity = max(0.0, self.login_activity - r.uniform(0.5, 2.5))
            if r.random() < 0.12:
                self.service_interruptions += 1
            self.satisfaction_score = max(1.0, self.satisfaction_score - r.uniform(0.0, 0.4))
        else:
            # Normal / stable trajectory with small realistic noise
            self.data_usage = max(0.0, self.data_usage + r.normal(0, 0.3))
            self.call_usage = max(0.0, self.call_usage + r.normal(0, 5))
            if r.random() < 0.03:
                self.support_calls += 1
            if r.random() < 0.01:
                self.payment_failures = max(0, self.payment_failures)
            if r.random() < 0.02:
                self.complaints += 1
            self.login_activity = float(np.clip(self.login_activity + r.normal(0, 1.0), 15, 40))
            self.satisfaction_score = float(np.clip(self.satisfaction_score + r.normal(0, 0.15), 6, 10))

        event = {
            "event_id": str(uuid.uuid4()),
            "customer_id": self.customer_id,
            "tenure": round(self.tenure, 2),
            "monthly_charges": round(self.monthly_charges, 2),
            "total_charges": round(self.total_charges, 2),
            "contract": self.contract,
            "internet_service": self.internet_service,
            "payment_method": self.payment_method,
            "data_usage": round(self.data_usage, 2),
            "call_usage": round(self.call_usage, 1),
            "support_calls": int(self.support_calls),
            "payment_failures": int(self.payment_failures),
            "complaints": int(self.complaints),
            "login_activity": round(self.login_activity, 1),
            "service_interruptions": int(self.service_interruptions),
            "satisfaction_score": round(self.satisfaction_score, 1),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        return event


class CustomerSimulator:
    """Owns a pool of CustomerState objects and cycles through them,
    producing one new event per call to `next_event()`."""

    def __init__(self, num_customers: int = None, risk_probability: float = None,
                 seed: int = None):
        self.num_customers = num_customers or config.SIMULATION_NUM_CUSTOMERS
        self.risk_probability = (
            risk_probability if risk_probability is not None else config.SIMULATION_RISK_PROBABILITY
        )
        self.rng = np.random.default_rng(seed)
        self.customers = self._build_population()
        self._cursor = 0

    def _build_population(self) -> list:
        base_df = pd.read_csv(config.RAW_DATA_PATH)
        n = min(self.num_customers, len(base_df))
        sample = base_df.sample(n=n, random_state=None).to_dict("records")

        customers = []
        for row in sample:
            risk_mode = self.rng.random() < self.risk_probability
            customers.append(CustomerState(row, risk_mode, self.rng))
        return customers

    def next_event(self) -> dict:
        """Return the next event, cycling round-robin through customers."""
        if not self.customers:
            raise RuntimeError("No customers loaded in simulator population.")
        customer = self.customers[self._cursor % len(self.customers)]
        self._cursor += 1
        return customer.step()

    def population_summary(self) -> dict:
        n_risk = sum(1 for c in self.customers if c.risk_mode)
        return {
            "total_customers": len(self.customers),
            "increasing_risk_customers": n_risk,
            "normal_customers": len(self.customers) - n_risk,
        }


def run_cli():
    """Standalone CLI mode: generate events and print them (optionally
    piping to the Kafka producer via kafka_producer.py in a separate
    process, or import this module directly for tighter integration)."""
    parser = argparse.ArgumentParser(description="Real-time telecom customer event simulator")
    parser.add_argument("--interval", type=float, default=config.SIMULATION_INTERVAL,
                         help="Seconds between events")
    parser.add_argument("--num-customers", type=int, default=config.SIMULATION_NUM_CUSTOMERS)
    parser.add_argument("--risk-probability", type=float, default=config.SIMULATION_RISK_PROBABILITY)
    parser.add_argument("--max-events", type=int, default=config.SIMULATION_MAX_EVENTS,
                         help="0 = run forever")
    parser.add_argument("--produce-to-kafka", action="store_true",
                         help="Also publish each event to Kafka via kafka_producer.py")
    args = parser.parse_args()

    sim = CustomerSimulator(num_customers=args.num_customers, risk_probability=args.risk_probability)
    print(f"Simulator population: {sim.population_summary()}")

    producer = None
    if args.produce_to_kafka:
        from src.kafka_producer import ChurnEventProducer
        producer = ChurnEventProducer()

    count = 0
    try:
        while args.max_events == 0 or count < args.max_events:
            event = sim.next_event()
            print(event)
            if producer:
                producer.send_event(event)
            count += 1
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nSimulator stopped by user.")
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    run_cli()
