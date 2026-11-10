"""
Lightweight SQLite persistence layer.

Stores:
  - processed_events : every customer event after ETL + feature engineering
  - predictions       : churn probability / risk level / health score per event
  - alerts            : early-warning alerts generated for risky events
  - customers         : latest known snapshot per customer (for dropdowns / lookups)

Using SQLite keeps the project dependency-free and fully local, per the
project requirements (no external DB server needed).
"""
import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
import config

_lock = threading.Lock()


def _connect():
    Path(config.DATABASE_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(config.DATABASE_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


@contextmanager
def get_connection():
    conn = _connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create tables if they do not already exist. Safe to call repeatedly."""
    with get_connection() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS processed_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE,
                customer_id TEXT NOT NULL,
                tenure INTEGER,
                monthly_charges REAL,
                total_charges REAL,
                contract TEXT,
                internet_service TEXT,
                payment_method TEXT,
                data_usage REAL,
                call_usage REAL,
                support_calls INTEGER,
                payment_failures INTEGER,
                complaints INTEGER,
                login_activity REAL,
                service_interruptions INTEGER,
                satisfaction_score REAL,
                support_call_rate REAL,
                payment_failure_rate REAL,
                usage_change REAL,
                complaint_frequency REAL,
                customer_activity_score REAL,
                contract_risk REAL,
                timestamp TEXT,
                ingested_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT,
                customer_id TEXT NOT NULL,
                churn_probability REAL,
                risk_level TEXT,
                health_score REAL,
                health_status TEXT,
                model_name TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT,
                customer_id TEXT NOT NULL,
                churn_probability REAL,
                health_score REAL,
                risk_level TEXT,
                reasons TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS customers (
                customer_id TEXT PRIMARY KEY,
                last_event_json TEXT,
                last_churn_probability REAL,
                last_risk_level TEXT,
                last_health_score REAL,
                last_health_status TEXT,
                events_count INTEGER DEFAULT 0,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS pipeline_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_name TEXT,
                metric_value REAL,
                recorded_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_events_customer ON processed_events(customer_id);
            CREATE INDEX IF NOT EXISTS idx_predictions_customer ON predictions(customer_id);
            CREATE INDEX IF NOT EXISTS idx_alerts_customer ON alerts(customer_id);
            CREATE INDEX IF NOT EXISTS idx_alerts_risk ON alerts(risk_level);
            """
        )


def insert_processed_event(event: dict):
    with _lock, get_connection() as conn:
        cols = [
            "event_id", "customer_id", "tenure", "monthly_charges", "total_charges",
            "contract", "internet_service", "payment_method", "data_usage", "call_usage",
            "support_calls", "payment_failures", "complaints", "login_activity",
            "service_interruptions", "satisfaction_score", "support_call_rate",
            "payment_failure_rate", "usage_change", "complaint_frequency",
            "customer_activity_score", "contract_risk", "timestamp",
        ]
        values = [event.get(c) for c in cols]
        placeholders = ",".join("?" for _ in cols)
        conn.execute(
            f"INSERT OR IGNORE INTO processed_events ({','.join(cols)}) VALUES ({placeholders})",
            values,
        )


def insert_prediction(pred: dict):
    with _lock, get_connection() as conn:
        conn.execute(
            """INSERT INTO predictions
               (event_id, customer_id, churn_probability, risk_level,
                health_score, health_status, model_name)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                pred.get("event_id"),
                pred.get("customer_id"),
                pred.get("churn_probability"),
                pred.get("risk_level"),
                pred.get("health_score"),
                pred.get("health_status"),
                pred.get("model_name"),
            ),
        )


def insert_alert(alert: dict):
    with _lock, get_connection() as conn:
        conn.execute(
            """INSERT INTO alerts
               (event_id, customer_id, churn_probability, health_score, risk_level, reasons)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                alert.get("event_id"),
                alert.get("customer_id"),
                alert.get("churn_probability"),
                alert.get("health_score"),
                alert.get("risk_level"),
                json.dumps(alert.get("reasons", [])),
            ),
        )


def upsert_customer_snapshot(customer_id: str, event: dict, churn_probability: float,
                              risk_level: str, health_score: float, health_status: str):
    with _lock, get_connection() as conn:
        row = conn.execute(
            "SELECT events_count FROM customers WHERE customer_id = ?", (customer_id,)
        ).fetchone()
        count = (row["events_count"] if row else 0) + 1
        conn.execute(
            """INSERT INTO customers
                (customer_id, last_event_json, last_churn_probability, last_risk_level,
                 last_health_score, last_health_status, events_count, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(customer_id) DO UPDATE SET
                 last_event_json=excluded.last_event_json,
                 last_churn_probability=excluded.last_churn_probability,
                 last_risk_level=excluded.last_risk_level,
                 last_health_score=excluded.last_health_score,
                 last_health_status=excluded.last_health_status,
                 events_count=excluded.events_count,
                 updated_at=CURRENT_TIMESTAMP
            """,
            (customer_id, json.dumps(event), churn_probability, risk_level,
             health_score, health_status, count),
        )


def record_metric(metric_name: str, metric_value: float):
    with _lock, get_connection() as conn:
        conn.execute(
            "INSERT INTO pipeline_stats (metric_name, metric_value) VALUES (?, ?)",
            (metric_name, metric_value),
        )


def get_customer_ids(limit: int = 5000):
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT customer_id FROM customers ORDER BY updated_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return [r["customer_id"] for r in rows]


def get_customer_snapshot(customer_id: str):
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM customers WHERE customer_id = ?", (customer_id,)
        ).fetchone()
    return dict(row) if row else None


def get_customer_event_history(customer_id: str, limit: int = 100):
    with get_connection() as conn:
        rows = conn.execute(
            """SELECT pe.*, p.churn_probability, p.risk_level, p.health_score, p.health_status
               FROM processed_events pe
               LEFT JOIN predictions p ON p.event_id = pe.event_id
               WHERE pe.customer_id = ?
               ORDER BY pe.ingested_at ASC LIMIT ?""",
            (customer_id, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def get_recent_events(limit: int = 50):
    with get_connection() as conn:
        rows = conn.execute(
            """SELECT pe.*, p.churn_probability, p.risk_level, p.health_score, p.health_status
               FROM processed_events pe
               LEFT JOIN predictions p ON p.event_id = pe.event_id
               ORDER BY pe.ingested_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_recent_alerts(limit: int = 200, risk_level: str = None):
    query = "SELECT * FROM alerts"
    params = []
    if risk_level and risk_level != "All":
        query += " WHERE risk_level = ?"
        params.append(risk_level)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        try:
            d["reasons"] = json.loads(d["reasons"]) if d["reasons"] else []
        except (TypeError, json.JSONDecodeError):
            d["reasons"] = []
        result.append(d)
    return result


def get_all_predictions(limit: int = 5000):
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM predictions ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_all_customers_df():
    import pandas as pd
    with get_connection() as conn:
        df = pd.read_sql_query("SELECT * FROM customers", conn)
    return df


def get_event_count() -> int:
    with get_connection() as conn:
        row = conn.execute("SELECT COUNT(*) AS c FROM processed_events").fetchone()
    return row["c"] if row else 0


def get_alert_count(risk_level: str = None) -> int:
    query = "SELECT COUNT(*) AS c FROM alerts"
    params = []
    if risk_level and risk_level != "All":
        query += " WHERE risk_level = ?"
        params.append(risk_level)
    with get_connection() as conn:
        row = conn.execute(query, params).fetchone()
    return row["c"] if row else 0


def reset_db():
    """Danger: wipes all stored real-time data (used for demo resets)."""
    with get_connection() as conn:
        conn.executescript(
            """
            DELETE FROM processed_events;
            DELETE FROM predictions;
            DELETE FROM alerts;
            DELETE FROM customers;
            DELETE FROM pipeline_stats;
            """
        )
