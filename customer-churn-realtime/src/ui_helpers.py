"""
Shared Streamlit UI helpers: dark-theme CSS injection, risk color coding,
and a reusable searchable customer-ID selector so no page ever needs a
plain text input for a customer ID.
"""
import streamlit as st

RISK_COLORS = {
    "Low": "#22C55E",       # green
    "Medium": "#EAB308",    # yellow
    "High": "#F97316",      # orange
    "Critical": "#EF4444",  # red
}

HEALTH_COLORS = {
    "Healthy": "#22C55E",
    "Moderate": "#EAB308",
    "At Risk": "#F97316",
    "Critical": "#EF4444",
}

PLOTLY_DARK_TEMPLATE = "plotly_dark"

DARK_CSS = """
<style>
    /* ---- Global app background ---- */
    .stApp {
        background-color: #0E1117;
        color: #E6E6E6;
    }

    /* ---- Sidebar ---- */
    section[data-testid="stSidebar"] {
        background-color: #12161F;
        border-right: 1px solid #262B36;
    }
    section[data-testid="stSidebar"] * {
        color: #E6E6E6 !important;
    }

    /* ---- Headers ---- */
    h1, h2, h3, h4, h5, h6 {
        color: #F5F5F5 !important;
    }

    /* ---- Metric cards ---- */
    div[data-testid="stMetric"] {
        background-color: #161B22;
        border: 1px solid #262B36;
        border-radius: 10px;
        padding: 14px 16px;
    }
    div[data-testid="stMetricLabel"] {
        color: #9CA3AF !important;
    }
    div[data-testid="stMetricValue"] {
        color: #F5F5F5 !important;
    }

    /* ---- Tables / dataframes ---- */
    div[data-testid="stDataFrame"] {
        background-color: #161B22;
        border-radius: 8px;
    }

    /* ---- Buttons ---- */
    .stButton>button {
        background-color: #1F2937;
        color: #E6E6E6;
        border: 1px solid #374151;
        border-radius: 8px;
    }
    .stButton>button:hover {
        background-color: #2D3748;
        border: 1px solid #5B8DEF;
        color: #FFFFFF;
    }

    /* ---- Inputs / selectboxes / dropdowns ---- */
    div[data-baseweb="select"] > div {
        background-color: #161B22 !important;
        border-color: #374151 !important;
        color: #E6E6E6 !important;
    }
    input, textarea {
        background-color: #161B22 !important;
        color: #E6E6E6 !important;
    }

    /* ---- Tabs ---- */
    button[data-baseweb="tab"] {
        color: #9CA3AF;
    }
    button[data-baseweb="tab"][aria-selected="true"] {
        color: #FFFFFF;
        border-bottom: 2px solid #5B8DEF;
    }

    /* ---- Expanders ---- */
    details {
        background-color: #161B22;
        border: 1px solid #262B36;
        border-radius: 8px;
    }

    /* ---- Custom card component ---- */
    .churn-card {
        background-color: #161B22;
        border: 1px solid #262B36;
        border-radius: 12px;
        padding: 18px 20px;
        margin-bottom: 12px;
    }
    .churn-badge {
        display: inline-block;
        padding: 3px 12px;
        border-radius: 999px;
        font-weight: 600;
        font-size: 0.85rem;
        color: #0E1117;
    }
</style>
"""


def inject_dark_theme():
    st.markdown(DARK_CSS, unsafe_allow_html=True)


def risk_badge_html(risk_level: str) -> str:
    color = RISK_COLORS.get(risk_level, "#9CA3AF")
    return f'<span class="churn-badge" style="background-color:{color};">{risk_level}</span>'


def health_badge_html(status: str) -> str:
    color = HEALTH_COLORS.get(status, "#9CA3AF")
    return f'<span class="churn-badge" style="background-color:{color};">{status}</span>'


def customer_selectbox(label: str, customer_ids: list, key: str, help_text: str = None):
    """
    A single, reusable searchable customer-ID selector used across every
    page that needs one. Streamlit's selectbox is natively searchable
    (type to filter), so this satisfies the "never make users type a raw
    customer ID into a text field" requirement everywhere it's used.
    """
    if not customer_ids:
        st.info("No customers available yet. Start the simulator to generate data.")
        return None
    return st.selectbox(
        label,
        options=customer_ids,
        key=key,
        help=help_text or "Type to search, or scroll to select a customer.",
    )


def apply_plotly_dark(fig):
    fig.update_layout(
        template=PLOTLY_DARK_TEMPLATE,
        paper_bgcolor="#0E1117",
        plot_bgcolor="#0E1117",
        font=dict(color="#E6E6E6"),
        margin=dict(l=30, r=20, t=50, b=30),
    )
    return fig
