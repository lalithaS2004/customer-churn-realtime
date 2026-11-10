# 📡 Telecom Customer Churn Prediction & Real-Time Analytics System

A complete, locally-runnable system that simulates a real-world telecom
environment: customer data arrives continuously, streams through **Apache
Kafka**, is cleaned and enriched by an **ETL pipeline**, scored by a
**Scikit-learn** churn model, converted into a **Customer Health Score**,
and surfaced through **Early Warning Alerts**, **Retention
Recommendations**, and a dark-themed **Streamlit** dashboard.

---

## 1. Project Objective

Telecom companies lose significant revenue to customer churn. This project
demonstrates an end-to-end, real-time analytics pipeline that:

- Simulates continuously arriving telecom customer behavior data.
- Streams that data through Kafka the way a production system would.
- Cleans, validates, and enriches it via a proper ETL pipeline.
- Predicts churn probability with a trained Scikit-learn model.
- Converts that prediction into a transparent 0–100 Customer Health Score.
- Raises early-warning alerts for at-risk customers.
- Suggests rule-based retention actions.
- Displays everything live in a dark-themed Streamlit dashboard.

It's built as a college-level Big Data / Data Analytics / Machine Learning
project — no Hadoop, no PySpark, no external services required — everything
runs on a normal Windows/Mac/Linux laptop.

---

## 2. Architecture

```text
Python Customer Data Simulator
            ↓
       Kafka Producer
            ↓
      Kafka Topic ("customer-events")
            ↓
       Kafka Consumer
            ↓
       ETL Pipeline (extract → validate/clean → feature engineering)
            ↓
    Churn Prediction Model (Scikit-learn, loaded via Joblib)
            ↓
 Customer Health Score (0-100, rule-based & transparent)
            ↓
 Early Warning Alert System (Low/Medium/High/Critical)
            ↓
 Retention Analysis (rule-based recommendations)
            ↓
      SQLite (historical storage)
            ↓
      Streamlit Dashboard (dark theme, 7 pages)
```

Every stage is a separate, independently runnable Python process/module —
nothing is crammed into one script.

---

## 3. Technologies Used

| Purpose                | Technology                          |
|-------------------------|-------------------------------------|
| Streaming               | Apache Kafka (`kafka-python`)       |
| Data processing          | Pandas, NumPy                       |
| Machine learning         | Scikit-learn, Joblib                |
| Dashboard                | Streamlit                           |
| Visualization            | Plotly, Matplotlib                  |
| Storage                  | SQLite                              |
| Messaging format         | JSON                                |
| Config                   | python-dotenv / `.env`              |

No Hadoop. No PySpark. Fully local.

---

## 4. Kafka Concepts Used

- **Topic** (`customer-events`): the single channel customer events flow through.
- **Producer** (`src/kafka_producer.py`): publishes JSON-encoded customer
  events, keyed by `customer_id` so a given customer's events land on the
  same partition (preserves per-customer ordering).
- **Consumer** (`src/kafka_consumer.py`): subscribes to the topic under a
  consumer group (`KAFKA_CONSUMER_GROUP`) and processes messages as they arrive.
- **Consumer group / offsets**: `kafka-python` auto-commits offsets so a
  restarted consumer resumes rather than reprocessing everything.
- **Backpressure/error handling**: both producer and consumer catch
  `NoBrokersAvailable` / `KafkaError` and surface a clear message instead
  of crashing.

---

## 5. ETL Pipeline

Implemented in `src/etl_pipeline.py`, `src/feature_engineering.py`:

- **Extract**: raw JSON event handed off by the Kafka consumer.
- **Transform**:
  - Field validation (`feature_engineering.validate_event`)
  - Missing-value handling with sane defaults
  - Duplicate detection (DB `UNIQUE` constraint on `event_id`)
  - Categorical / numeric handling and scaling (inside the model pipeline)
  - Feature engineering:
    - `support_call_rate` — support calls normalized by tenure
    - `payment_failure_rate` — payment failures normalized by tenure
    - `usage_change` — % change in data usage vs. the customer's last reading
    - `complaint_frequency` — complaints normalized by tenure
    - `customer_activity_score` — composite login/engagement score
    - `contract_risk` — numeric risk implied by contract type
- **Load**: processed event, prediction, and (if warranted) an alert are
  written to SQLite (`data/processed/churn_analytics.db`), and a rolling
  per-customer snapshot is upserted for fast dashboard lookups.

---

## 6. Real-Time Simulation

`src/simulator.py` does **not** replay static CSV rows. It draws a base
population from `data/raw/telecom_customer_churn.csv` and gives each
customer internal, evolving state:

- **Normal customers**: stable usage, few complaints, no payment failures,
  regular activity — small realistic noise only.
- **Increasing-risk customers**: usage trending down, complaints and
  support calls trending up, payment failures appearing, activity
  declining — a believable path toward churn.

Configurable via `.env` / CLI flags: number of customers, event interval,
risk probability, and max events.

> **Note on the dataset**: no dataset file was supplied with this project
> brief, so `src/data_generator.py` generates a synthetic base population
> with the same schema as the well-known "Telco Customer Churn" dataset.
> If you have a real churn CSV, drop it at
> `data/raw/telecom_customer_churn.csv` with equivalent column names
> (see the column list in `src/data_generator.py`) and skip regenerating it.

---

## 7. Machine Learning Pipeline

`src/train_models.py` trains and compares three models:

- Logistic Regression
- Random Forest
- Gradient Boosting

Each is evaluated on a held-out test split with **Accuracy, Precision,
Recall, F1-score, ROC-AUC, and a confusion matrix**. The best model is
selected using a combination of **F1-score and ROC-AUC** (not accuracy
alone, since churn datasets are typically imbalanced), then saved with
Joblib to `models/churn_model.pkl` along with `models/model_metadata.json`.

The Kafka consumer (and Demo Mode) load this saved pipeline
(`src/prediction.py`) to score every incoming event in real time.

---

## 8. Customer Health Score

`src/health_score.py` computes a transparent, weighted 0–100 score from:

| Component               | Weight |
|--------------------------|--------|
| Churn risk (inverse)      | 35%   |
| Tenure (loyalty)          | 15%   |
| Payment behavior          | 15%   |
| Support interactions      | 15%   |
| Usage trend               | 10%   |
| Complaints                | 5%    |
| Customer activity         | 5%    |

Status bands: **Healthy** (≥75), **Moderate** (≥50), **At Risk** (≥30),
**Critical** (<30). Weights are constants at the top of the file — easy
to retune.

---

## 9. Early Warning System

`src/alerts.py` classifies every scored event into **Low / Medium / High
/ Critical** based on churn probability thresholds (configurable via
`.env`), and generates plain-language reasons (e.g. "Increased support
calls", "Reduced usage", "Payment failure detected"). Alerts are only
persisted for Medium risk and above, to avoid noise. The **🚨 Early
Warning Alerts** dashboard page lists them with a risk-level filter.

---

## 10. Retention Analysis

`src/retention.py` applies simple, explainable rules to at-risk customers,
e.g.:

```text
High price sensitivity + month-to-month     → discount / plan review
High support complaints                     → priority customer support
Usage declining                             → engagement offer
Payment problems                            → payment assistance
```

These are clearly labeled in the UI as **rule-based recommendations** —
not a guarantee of retention.

---

## 11. Streamlit Dashboard

A fully dark-themed dashboard (`.streamlit/config.toml` + custom CSS in
`src/ui_helpers.py`) with 7 pages:

1. **🏠 Overview** — headline KPIs, risk & health distributions, event volume.
2. **📡 Live Streaming** — latest events, throughput, processing status.
3. **🔮 Churn Prediction** — per-customer churn probability & explanation.
4. **❤️ Customer Health** — health score, trends, support/payment history.
5. **🚨 Early Warning Alerts** — filterable alert feed.
6. **🔄 Retention Analysis** — high-risk customers + recommended actions.
7. **📊 Analytics** — churn/risk/health distributions and trend charts.

**Every customer selector in the app is a searchable dropdown
(`st.selectbox`, filterable by typing) — nowhere does the app ask you to
type a raw Customer ID.**

---

## 12. Project Structure

```text
customer-churn-realtime/
│
├── data/
│   ├── raw/                     # base customer population (CSV)
│   └── processed/                # SQLite database (created at runtime)
│
├── models/
│   ├── churn_model.pkl           # trained pipeline (created by train_models.py)
│   └── model_metadata.json
│
├── src/
│   ├── simulator.py               # real-time customer event simulator
│   ├── kafka_producer.py          # publishes events to Kafka
│   ├── kafka_consumer.py          # consumes events, runs the ETL pipeline
│   ├── etl_pipeline.py            # extract/transform/load orchestration
│   ├── feature_engineering.py     # shared feature logic (train + inference)
│   ├── train_models.py            # trains/evaluates/selects the ML model
│   ├── prediction.py              # loads model, scores events
│   ├── health_score.py            # 0-100 health scoring logic
│   ├── alerts.py                  # early warning alert generation
│   ├── retention.py               # rule-based retention recommendations
│   ├── database.py                # SQLite persistence layer
│   ├── data_generator.py          # synthetic base dataset generator
│   └── ui_helpers.py              # dark theme CSS + dashboard widgets
│
├── app.py                         # Streamlit dashboard entry point
├── docker-compose.yml             # optional local Kafka + Zookeeper
├── requirements.txt
├── .env.example
├── .streamlit/config.toml         # dark theme base config
└── README.md
```

---

## 13. Installation

```bash
# 1. Clone / unzip the project, then create a virtual environment
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # macOS/Linux

# 2. Install dependencies
pip install -r requirements.txt

# 3. Copy the environment template
copy .env.example .env       # Windows
# cp .env.example .env       # macOS/Linux
```

---

## 14. Kafka Setup

You need a Kafka broker reachable at `KAFKA_BOOTSTRAP_SERVERS`
(default `localhost:9092`).

**Option A — Docker (easiest on Windows with Docker Desktop):**
```bash
docker compose up -d
```

**Option B — Native Apache Kafka (KRaft mode, no ZooKeeper needed):**
Download Kafka from https://kafka.apache.org/downloads, then:
```bash
# from the extracted Kafka folder
bin\windows\kafka-storage.bat format -t <cluster-uuid> -c config\kraft\server.properties
bin\windows\kafka-server-start.bat config\kraft\server.properties
```

**No Kafka available yet?** Use **Demo Mode** in the Streamlit sidebar
(see section 18) to run the full pipeline without a broker.

---

## 15. Model Training

Generate the base dataset (already included, but regenerate anytime) and train:

```bash
python -m src.data_generator     # only needed if data/raw/telecom_customer_churn.csv is missing
python -m src.train_models
```

This prints metrics for all three models and saves the best one to
`models/churn_model.pkl`.

---

## 16. Start the Simulator + Producer

```bash
python -m src.kafka_producer
```

This loads the base population, evolves each customer's behavior every
`SIMULATION_INTERVAL` seconds, and publishes events to the
`customer-events` topic.

(You can also drive `src/simulator.py` standalone with
`python -m src.simulator` to just print events without Kafka.)

---

## 17. Start the Consumer

In a second terminal:
```bash
python -m src.kafka_consumer
```

This reads events, runs them through the ETL + ML + health-score +
alerting pipeline, and writes results to SQLite.

---

## 18. Launch the Dashboard

In a third terminal:
```bash
streamlit run app.py
```

Open the URL Streamlit prints (typically http://localhost:8501).

**Two ways to see data:**
- **Real pipeline**: keep the producer + consumer terminals running; the
  dashboard reads from SQLite and reflects new events as they arrive
  (use the Refresh button / re-navigate a page to pull the latest rows).
- **Demo Mode (no Kafka)**: use the sidebar's "Generate events now"
  button to run the simulator + full ETL/ML/alerting pipeline directly
  in-process — useful for a quick demo or if you don't want to stand up
  a Kafka broker.

---

## 19. Example Workflow

```bash
python -m src.data_generator
python -m src.train_models
python -m src.kafka_producer      # terminal 1
python -m src.kafka_consumer      # terminal 2
streamlit run app.py              # terminal 3
```

Watch the **📡 Live Streaming** page fill up, then check **🚨 Early
Warning Alerts** and **🔄 Retention Analysis** for customers whose
simulated behavior has drifted into risky territory.

---

## 20. Project Limitations

- The base dataset is synthetically generated (see section 6) since no
  real dataset file was provided; swap in a real Telco churn CSV for a
  more authentic training signal.
- `usage_change` is computed from an in-memory cache that resets when the
  consumer/app restarts (a production system would persist "previous
  reading" state in the database).
- The health-score weights and alert thresholds are reasonable defaults,
  not empirically tuned against real business outcomes.
- Single-broker Kafka (no replication) — fine for a demo, not for production.
- Retention recommendations are simple, transparent rules, not an
  optimized causal or uplift model.

---

## Future Improvements

- Persist `usage_change` baselines in SQLite instead of memory.
- Add authentication and multi-user support to the dashboard.
- Swap SQLite for a proper time-series store if scaling to very high event
  volumes.
- Add model monitoring / drift detection over time.
- A/B test retention actions and feed outcomes back into the rules (or a
  learned uplift model).
- Containerize the whole stack (producer, consumer, dashboard, Kafka) with
  a single `docker-compose up`.
