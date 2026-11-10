"""
Customer Churn Prediction & Real-Time Analytics — Streamlit Dashboard.

Two ways to feed this dashboard with data (see README for full details):

1. REAL KAFKA PIPELINE (recommended, matches the full architecture):
   Run in separate terminals:
     python -m src.kafka_producer
     python -m src.kafka_consumer
   Then just open this dashboard -- it reads everything from SQLite.

2. QUICK DEMO MODE (no Kafka required):
   Use the "Demo Mode" controls in the sidebar to generate and process
   events directly in-process. Useful for a fast local demo, grading,
   or environments where standing up a Kafka broker isn't convenient.

Run with:
    streamlit run app.py
"""
import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st

import config
from src import alerts as alerts_mod
from src import database
from src import etl_pipeline
from src import health_score as health_mod
from src import prediction
from src import retention as retention_mod
from src.simulator import CustomerSimulator
from src.ui_helpers import (
    apply_plotly_dark, customer_selectbox, health_badge_html,
    inject_dark_theme, risk_badge_html,
)

st.set_page_config(
    page_title="Telecom Churn Analytics",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_dark_theme()
database.init_db()

# ------------------------------------------------------------------ #
# Session state setup
# ------------------------------------------------------------------ #
if "demo_simulator" not in st.session_state:
    st.session_state.demo_simulator = None
if "demo_events_generated" not in st.session_state:
    st.session_state.demo_events_generated = 0
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = datetime.now()


def get_simulator() -> CustomerSimulator:
    if st.session_state.demo_simulator is None:
        st.session_state.demo_simulator = CustomerSimulator()
    return st.session_state.demo_simulator


# ------------------------------------------------------------------ #
# Sidebar: navigation + demo controls
# ------------------------------------------------------------------ #
st.sidebar.markdown("## 📡 Telecom Churn Analytics")
st.sidebar.caption("Real-time customer churn prediction & retention system")

PAGES = [
    "🏠 Overview",
    "📡 Live Streaming",
    "🔮 Churn Prediction",
    "❤️ Customer Health",
    "🚨 Early Warning Alerts",
    "🔄 Retention Analysis",
    "📊 Analytics",
]
page = st.sidebar.radio("Navigate", PAGES, label_visibility="collapsed")

st.sidebar.divider()

demo_batch_size = st.sidebar.slider("Events to generate", 1, 50, 10)
if st.sidebar.button("▶️ Generate events now", use_container_width=True):
    sim = get_simulator()
    generated, failed = 0, 0
    progress = st.sidebar.progress(0.0)
    for i in range(demo_batch_size):
        try:
            event = sim.next_event()
            etl_pipeline.process_event(event)
            generated += 1
        except prediction.ModelUnavailableError as e:
            st.sidebar.error(f"Model unavailable: {e}")
            break
        except etl_pipeline.ETLError as e:
            failed += 1
        progress.progress((i + 1) / demo_batch_size)
    st.session_state.demo_events_generated += generated
    st.sidebar.success(f"Processed {generated} events" + (f" ({failed} skipped)" if failed else ""))

if st.sidebar.button("🗑️ Reset all data", use_container_width=True):
    database.reset_db()
    st.session_state.demo_simulator = None
    st.session_state.demo_events_generated = 0
    st.sidebar.success("Database reset.")

st.sidebar.divider()
st.sidebar.caption(f"Kafka topic: `{config.KAFKA_TOPIC}`")
st.sidebar.caption(f"Kafka broker: `{config.KAFKA_BOOTSTRAP_SERVERS}`")

try:
    model_name = prediction.get_model_name()
    st.sidebar.caption(f"Active model: `{model_name}`")
except Exception:
    st.sidebar.caption("Active model: unavailable")


def model_status_banner():
    from pathlib import Path
    if not Path(config.MODEL_PATH).exists():
        st.error(
            "⚠️ No trained model found. Run `python -m src.train_models` "
            "before using prediction features."
        )
        return False
    return True


# ------------------------------------------------------------------ #
# Page: Overview
# ------------------------------------------------------------------ #
def page_overview():
    st.title("🏠 Overview")

    customers_df = database.get_all_customers_df()
    event_count = database.get_event_count()
    critical_alerts = database.get_alert_count("Critical")
    high_alerts = database.get_alert_count("High")

    total_customers = len(customers_df)
    avg_health = customers_df["last_health_score"].mean() if not customers_df.empty else 0
    high_risk_count = (
        customers_df["last_risk_level"].isin(["High", "Critical"]).sum()
        if not customers_df.empty else 0
    )
    churn_risk_pct = (high_risk_count / total_customers * 100) if total_customers else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Customers Processed", f"{total_customers:,}")
    c2.metric("Events Received", f"{event_count:,}")
    c3.metric("High-Risk Customers", f"{high_risk_count:,}")
    c4.metric("Critical Alerts", f"{critical_alerts:,}")

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Average Health Score", f"{avg_health:.1f}" if total_customers else "—")
    c6.metric("Current Churn-Risk %", f"{churn_risk_pct:.1f}%")
    c7.metric("High Alerts", f"{high_alerts:,}")
    c8.metric("Real-Time Event Counter", f"{event_count:,}")

    st.divider()

    if customers_df.empty:
        st.info(
            "No data yet. Use **Demo Mode** in the sidebar, or start the Kafka "
            "producer + consumer, to begin streaming customer events."
        )
        return

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Risk Level Distribution")
        risk_counts = customers_df["last_risk_level"].value_counts().reset_index()
        risk_counts.columns = ["risk_level", "count"]
        fig = px.pie(
            risk_counts, names="risk_level", values="count", hole=0.5,
            color="risk_level",
            color_discrete_map={"Low": "#22C55E", "Medium": "#EAB308",
                                 "High": "#F97316", "Critical": "#EF4444"},
        )
        apply_plotly_dark(fig)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Health Score Distribution")
        fig2 = px.histogram(customers_df, x="last_health_score", nbins=20,
                             color_discrete_sequence=["#5B8DEF"])
        fig2.update_layout(xaxis_title="Health Score", yaxis_title="Customers")
        apply_plotly_dark(fig2)
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Recent Event Volume")
    recent = database.get_recent_events(limit=200)
    if recent:
        rdf = pd.DataFrame(recent)
        rdf["ingested_at"] = pd.to_datetime(rdf["ingested_at"])
        volume = rdf.set_index("ingested_at").resample("min").size().reset_index(name="events")
        fig3 = px.line(volume, x="ingested_at", y="events", markers=True,
                        color_discrete_sequence=["#5B8DEF"])
        apply_plotly_dark(fig3)
        st.plotly_chart(fig3, use_container_width=True)


# ------------------------------------------------------------------ #
# Page: Live Streaming
# ------------------------------------------------------------------ #
def page_live_streaming():
    st.title("📡 Live Streaming")
    st.caption(
        "Shows the latest customer events flowing through Kafka → ETL → ML. "
        "Click Refresh, or use Demo Mode in the sidebar to generate new events."
    )

    col_a, col_b, col_c, col_d = st.columns(4)
    event_count = database.get_event_count()
    recent = database.get_recent_events(limit=100)

    latency_note = "N/A (demo/batch mode)"
    events_per_sec = 0.0
    if recent:
        rdf = pd.DataFrame(recent)
        rdf["ingested_at"] = pd.to_datetime(rdf["ingested_at"])
        span_seconds = (rdf["ingested_at"].max() - rdf["ingested_at"].min()).total_seconds()
        if span_seconds > 0:
            events_per_sec = len(rdf) / span_seconds

    col_a.metric("Total Events", f"{event_count:,}")
    col_b.metric("Events / sec (recent)", f"{events_per_sec:.2f}")
    col_c.metric("Kafka Topic", config.KAFKA_TOPIC)
    col_d.metric("Processing Status", "🟢 Active" if recent else "⚪ Idle")

    if st.button("🔄 Refresh"):
        st.rerun()

    st.divider()
    st.subheader("Latest Customer Events")
    if not recent:
        st.info("No events yet. Generate some via Demo Mode or start the Kafka pipeline.")
        return

    display_df = pd.DataFrame(recent)[
        ["customer_id", "timestamp", "tenure", "monthly_charges", "support_calls",
         "payment_failures", "data_usage", "churn_probability", "risk_level",
         "health_score", "ingested_at"]
    ]
    display_df["churn_probability"] = (display_df["churn_probability"] * 100).round(1)
    display_df = display_df.rename(columns={
        "churn_probability": "churn_prob_%", "ingested_at": "processed_at",
    })
    st.dataframe(display_df, use_container_width=True, height=420)


# ------------------------------------------------------------------ #
# Page: Churn Prediction
# ------------------------------------------------------------------ #
def page_churn_prediction():
    st.title("🔮 Churn Prediction")
    if not model_status_banner():
        return

    customer_ids = database.get_customer_ids()
    selected = customer_selectbox("Select Customer", customer_ids, key="churn_pred_customer")
    if not selected:
        return

    snapshot = database.get_customer_snapshot(selected)
    if not snapshot:
        st.warning("No data found for this customer yet.")
        return

    import json
    event = json.loads(snapshot["last_event_json"])
    prob = snapshot["last_churn_probability"]
    risk = snapshot["last_risk_level"]
    health = snapshot["last_health_score"]

    col1, col2 = st.columns([2, 1])
    with col1:
        st.subheader("Customer Information")
        info_cols = st.columns(3)
        info_cols[0].metric("Tenure (months)", f"{event.get('tenure', 0):.1f}")
        info_cols[1].metric("Monthly Charges", f"${event.get('monthly_charges', 0):.2f}")
        info_cols[2].metric("Contract", event.get("contract", "—"))
        info_cols2 = st.columns(3)
        info_cols2[0].metric("Support Calls", event.get("support_calls", 0))
        info_cols2[1].metric("Payment Failures", event.get("payment_failures", 0))
        info_cols2[2].metric("Data Usage (GB)", f"{event.get('data_usage', 0):.1f}")

    with col2:
        st.subheader("Churn Probability")
        st.markdown(
            f"<h1 style='color:{'#EF4444' if prob >= 0.6 else '#EAB308' if prob >= 0.35 else '#22C55E'};'>"
            f"{prob * 100:.1f}%</h1>",
            unsafe_allow_html=True,
        )
        st.markdown(f"Risk Level: {risk_badge_html(risk)}", unsafe_allow_html=True)
        st.metric("Health Score", f"{health:.1f}/100")

    st.divider()
    st.subheader("Main Risk Factors")
    reasons = alerts_mod.build_reasons(event)
    for r in reasons:
        st.markdown(f"- {r}")

    st.divider()
    st.subheader("Prediction Explanation")
    st.write(
        f"This customer's churn probability was computed by the "
        f"**{snapshot.get('last_event_json') and prediction.get_model_name()}** model "
        f"based on their tenure, billing, service usage, and engagement patterns. "
        f"A probability of **{prob*100:.1f}%** places them in the **{risk}** risk "
        f"category (thresholds: Medium ≥35%, High ≥{config.ALERT_HIGH_THRESHOLD*100:.0f}%, "
        f"Critical ≥{config.ALERT_CRITICAL_THRESHOLD*100:.0f}%)."
    )


# ------------------------------------------------------------------ #
# Page: Customer Health
# ------------------------------------------------------------------ #
def page_customer_health():
    st.title("❤️ Customer Health")

    customer_ids = database.get_customer_ids()
    selected = customer_selectbox("Select Customer", customer_ids, key="health_customer")
    if not selected:
        return

    history = database.get_customer_event_history(selected, limit=200)
    if not history:
        st.warning("No history found for this customer yet.")
        return

    hdf = pd.DataFrame(history)
    latest = hdf.iloc[-1]

    col1, col2, col3 = st.columns(3)
    col1.markdown(
        f"### Health Score: {latest['health_score']:.1f}/100" if pd.notna(latest['health_score']) else "### Health Score: N/A"
    )
    status = latest["health_status"] if pd.notna(latest["health_status"]) else "Unknown"
    col1.markdown(health_badge_html(status), unsafe_allow_html=True)
    col2.metric("Usage Trend (last change)", f"{latest.get('usage_change', 0)*100:.1f}%")
    col3.metric("Customer Activity Score", f"{latest.get('customer_activity_score', 0):.1f}")

    st.divider()
    tabs = st.tabs(["Health Trend", "Usage Trend", "Support History", "Payment Behavior"])

    with tabs[0]:
        if hdf["health_score"].notna().any():
            fig = px.line(hdf.reset_index(), x="index", y="health_score", markers=True,
                          color_discrete_sequence=["#5B8DEF"])
            fig.update_layout(xaxis_title="Event #", yaxis_title="Health Score")
            apply_plotly_dark(fig)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Not enough data yet.")

    with tabs[1]:
        fig = px.line(hdf.reset_index(), x="index", y="data_usage", markers=True,
                      color_discrete_sequence=["#22C55E"])
        fig.update_layout(xaxis_title="Event #", yaxis_title="Data Usage (GB)")
        apply_plotly_dark(fig)
        st.plotly_chart(fig, use_container_width=True)

    with tabs[2]:
        fig = px.line(hdf.reset_index(), x="index", y="support_calls", markers=True,
                      color_discrete_sequence=["#F97316"])
        fig.update_layout(xaxis_title="Event #", yaxis_title="Support Calls (cumulative)")
        apply_plotly_dark(fig)
        st.plotly_chart(fig, use_container_width=True)
        st.caption(f"Complaints (cumulative): {latest.get('complaints', 0)}")

    with tabs[3]:
        fig = px.line(hdf.reset_index(), x="index", y="payment_failures", markers=True,
                      color_discrete_sequence=["#EF4444"])
        fig.update_layout(xaxis_title="Event #", yaxis_title="Payment Failures (cumulative)")
        apply_plotly_dark(fig)
        st.plotly_chart(fig, use_container_width=True)
        st.caption(f"Payment method: {latest.get('payment_method', '—')}")


# ------------------------------------------------------------------ #
# Page: Early Warning Alerts
# ------------------------------------------------------------------ #
def page_alerts():
    st.title("🚨 Early Warning Alerts")

    filter_choice = st.selectbox("Filter by Risk Level", ["All", "Low", "Medium", "High", "Critical"])
    alerts_list = database.get_recent_alerts(limit=200, risk_level=filter_choice)

    if not alerts_list:
        st.info("No alerts yet for this filter.")
        return

    for alert in alerts_list:
        badge = risk_badge_html(alert["risk_level"])
        reasons_html = "".join(f"<li>{r}</li>" for r in alert["reasons"])
        st.markdown(
            f"""
            <div class="churn-card">
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <strong>Customer: {alert['customer_id']}</strong>
                    {badge}
                </div>
                <p style="margin:6px 0 0 0;">Churn Probability: <strong>{alert['churn_probability']*100:.1f}%</strong>
                &nbsp;|&nbsp; Health Score: <strong>{alert['health_score']:.1f}</strong>
                &nbsp;|&nbsp; <span style="color:#9CA3AF;">{alert['created_at']}</span></p>
                <ul style="margin:8px 0 0 18px;">{reasons_html}</ul>
            </div>
            """,
            unsafe_allow_html=True,
        )


# ------------------------------------------------------------------ #
# Page: Retention Analysis
# ------------------------------------------------------------------ #
def page_retention():
    st.title("🔄 Retention Analysis")
    st.caption(
        "⚠️ These are **rule-based retention recommendations** derived from observed "
        "behavior patterns. They are heuristics, not guarantees that any action will "
        "prevent churn."
    )

    customers_df = database.get_all_customers_df()
    if customers_df.empty:
        st.info("No customer data yet.")
        return

    at_risk = customers_df[customers_df["last_risk_level"].isin(["High", "Critical"])]
    if at_risk.empty:
        st.success("No high-risk customers detected right now.")
        return

    import json
    st.subheader(f"High-Risk Customers ({len(at_risk)})")

    for _, row in at_risk.sort_values("last_churn_probability", ascending=False).iterrows():
        event = json.loads(row["last_event_json"])
        matches = retention_mod.recommend_actions(event)
        priority = retention_mod.priority_level(matches)

        actions_html = "".join(
            f"<li><strong>{m['signal']}</strong> → {m['action']}</li>" for m in matches
        ) or "<li>No specific rule matched; consider a general relationship check-in.</li>"

        st.markdown(
            f"""
            <div class="churn-card">
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <strong>Customer: {row['customer_id']}</strong>
                    {risk_badge_html(row['last_risk_level'])}
                </div>
                <p style="margin:6px 0 0 0;">Health Score: <strong>{row['last_health_score']:.1f}</strong>
                &nbsp;|&nbsp; Priority: <strong>{priority}</strong></p>
                <ul style="margin:8px 0 0 18px;">{actions_html}</ul>
            </div>
            """,
            unsafe_allow_html=True,
        )


# ------------------------------------------------------------------ #
# Page: Analytics
# ------------------------------------------------------------------ #
def page_analytics():
    st.title("📊 Analytics")

    customers_df = database.get_all_customers_df()
    recent = database.get_recent_events(limit=1000)

    if customers_df.empty or not recent:
        st.info("No data yet. Generate events via Demo Mode or the Kafka pipeline.")
        return

    events_df = pd.DataFrame(recent)
    events_df["ingested_at"] = pd.to_datetime(events_df["ingested_at"])

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Churn Probability Distribution")
        fig = px.histogram(events_df, x="churn_probability", nbins=20,
                            color_discrete_sequence=["#5B8DEF"])
        apply_plotly_dark(fig)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Risk Level Distribution")
        counts = customers_df["last_risk_level"].value_counts().reset_index()
        counts.columns = ["risk_level", "count"]
        fig = px.bar(counts, x="risk_level", y="count", color="risk_level",
                     color_discrete_map={"Low": "#22C55E", "Medium": "#EAB308",
                                         "High": "#F97316", "Critical": "#EF4444"})
        apply_plotly_dark(fig)
        st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)
    with col3:
        st.subheader("Customer Health Distribution")
        fig = px.histogram(customers_df, x="last_health_score", nbins=20,
                            color_discrete_sequence=["#22C55E"])
        apply_plotly_dark(fig)
        st.plotly_chart(fig, use_container_width=True)

    with col4:
        st.subheader("Contract Type Analysis")
        contract_counts = events_df["contract"].value_counts().reset_index()
        contract_counts.columns = ["contract", "count"]
        fig = px.pie(contract_counts, names="contract", values="count", hole=0.4,
                     color_discrete_sequence=px.colors.sequential.Blues_r)
        apply_plotly_dark(fig)
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("Churn Probability Trend Over Time")
    trend = events_df.sort_values("ingested_at")
    fig = px.line(trend, x="ingested_at", y="churn_probability", markers=True,
                  color_discrete_sequence=["#5B8DEF"])
    apply_plotly_dark(fig)
    st.plotly_chart(fig, use_container_width=True)

    col5, col6 = st.columns(2)
    with col5:
        st.subheader("Support Call Trends")
        fig = px.line(trend, x="ingested_at", y="support_calls", markers=True,
                      color_discrete_sequence=["#F97316"])
        apply_plotly_dark(fig)
        st.plotly_chart(fig, use_container_width=True)

    with col6:
        st.subheader("Usage Trends")
        fig = px.line(trend, x="ingested_at", y="data_usage", markers=True,
                      color_discrete_sequence=["#22C55E"])
        apply_plotly_dark(fig)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Payment Failure Trends")
    fig = px.line(trend, x="ingested_at", y="payment_failures", markers=True,
                  color_discrete_sequence=["#EF4444"])
    apply_plotly_dark(fig)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Real-Time Event Volume Trend")
    volume = events_df.set_index("ingested_at").resample("min").size().reset_index(name="events")
    fig = px.bar(volume, x="ingested_at", y="events", color_discrete_sequence=["#5B8DEF"])
    apply_plotly_dark(fig)
    st.plotly_chart(fig, use_container_width=True)


# ------------------------------------------------------------------ #
# Router
# ------------------------------------------------------------------ #
PAGE_FUNCS = {
    "🏠 Overview": page_overview,
    "📡 Live Streaming": page_live_streaming,
    "🔮 Churn Prediction": page_churn_prediction,
    "❤️ Customer Health": page_customer_health,
    "🚨 Early Warning Alerts": page_alerts,
    "🔄 Retention Analysis": page_retention,
    "📊 Analytics": page_analytics,
}

try:
    PAGE_FUNCS[page]()
except database.sqlite3.Error as e:
    st.error(f"Database error: {e}")
except Exception as e:  # noqa: BLE001 - surface a friendly error instead of crashing
    st.error(f"Something went wrong loading this page: {e}")
