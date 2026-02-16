import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz
from collections import Counter
import re

# =====================================================
# CONFIG
# =====================================================

PROJECT_ID = "app-review-analyzer-487309"
DATASET = "app_reviews_ds"
TABLE = "raw_reviews"

# =====================================================
# PAGE CONFIG
# =====================================================

st.set_page_config(
    page_title="Strategic Intelligence Platform",
    page_icon="🦅",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
.stApp { background-color: #0b0f19; color: #e2e8f0; }
div[data-testid="metric-container"] {
    background: linear-gradient(180deg, #1e293b 0%, #0f172a 100%);
    border: 1px solid #334155;
    border-radius: 12px;
    padding: 18px;
}
</style>
""", unsafe_allow_html=True)

# =====================================================
# BIGQUERY CONNECTION
# =====================================================

@st.cache_resource
def init_bq():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

bq = init_bq()

# =====================================================
# DATA LOADER
# =====================================================

@st.cache_data(ttl=600)
def load_data():
    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    """
    return bq.query(query).to_dataframe()

df = load_data()

if df.empty:
    st.error("No data found.")
    st.stop()

# =====================================================
# DATA PREP
# =====================================================

df["date"] = pd.to_datetime(df["date"])
df["Month"] = df["date"].dt.strftime("%Y-%m")
df["Week"] = df["date"].dt.strftime("%Y-W%V")
df["score"] = pd.to_numeric(df["rating"])

# Detect NET columns automatically (binary INT fields)
core_cols = [
    "app_id","brand_name","review_id","date",
    "content","rating","sentiment",
    "products","themes","Month","Week","score"
]

net_cols = [
    c for c in df.columns
    if c not in core_cols and df[c].dropna().isin([0,1]).all()
]

# =====================================================
# SIDEBAR
# =====================================================

with st.sidebar:
    st.title("🎛 Command Center")

    brands = sorted(df["brand_name"].unique())
    sel_brands = st.multiselect("Brands", brands, default=brands)

    min_d = df["date"].min()
    max_d = df["date"].max()

    date_range = st.date_input(
        "Date Range",
        [min_d, max_d],
        min_value=min_d,
        max_value=max_d
    )

    sel_ratings = st.multiselect(
        "Ratings",
        [1,2,3,4,5],
        default=[1,2,3,4,5]
    )

# =====================================================
# FILTERING
# =====================================================

mask = df["brand_name"].isin(sel_brands)
mask &= df["score"].isin(sel_ratings)

if len(date_range) == 2:
    mask &= df["date"].between(
        pd.to_datetime(date_range[0]),
        pd.to_datetime(date_range[1])
    )

df = df[mask]

# =====================================================
# HEADER
# =====================================================

st.title("🦅 Strategic Intelligence Platform")

ist = pytz.timezone("Asia/Kolkata")
st.caption(f"Live | {datetime.now(ist).strftime('%d %b %Y %I:%M %p IST')}")

nav = st.radio(
    "",
    [
        "📊 Boardroom",
        "🚀 Drivers & Barriers",
        "⚔️ Head-to-Head",
        "📅 Period Matrix",
        "📈 Trends",
        "🔡 Text Analytics"
    ],
    horizontal=True
)

st.markdown("---")

# =====================================================
# TAB 1 — BOARDROOM
# =====================================================

if nav == "📊 Boardroom":

    col1, col2, col3, col4 = st.columns(4)

    total = len(df)
    avg = df["score"].mean()

    prom = len(df[df["score"]==5])
    det = len(df[df["score"]<=3])
    nps = ((prom - det)/total*100) if total else 0
    risk = (len(df[df["score"]==1])/total*100) if total else 0

    col1.metric("Total Reviews", f"{total:,}")
    col2.metric("Average Rating", f"{avg:.2f} ⭐")
    col3.metric("NPS Proxy", f"{nps:.0f}")
    col4.metric("1★ Risk %", f"{risk:.1f}%")

    st.markdown("### Brand Performance")

    brand_kpi = df.groupby("brand_name").agg(
        Volume=("score","count"),
        Avg_Rating=("score","mean")
    ).reset_index()

    fig = px.scatter(
        brand_kpi,
        x="Avg_Rating",
        y="Volume",
        size="Volume",
        color="brand_name",
        title="Strategic Positioning"
    )
    fig.update_layout(template="plotly_dark")
    st.plotly_chart(fig, use_container_width=True)

# =====================================================
# TAB 2 — DRIVERS & BARRIERS
# =====================================================

elif nav == "🚀 Drivers & Barriers":

    st.subheader("Top Drivers (4–5★)")
    pos = df[df["score"]>=4]

    if not pos.empty and net_cols:
        drivers = pos[net_cols].sum().sort_values(ascending=False).head(10)
        ddf = pd.DataFrame({
            "NET": drivers.index,
            "Pct": drivers.values / len(pos) * 100
        })

        fig = px.bar(ddf, x="Pct", y="NET", orientation="h")
        fig.update_layout(template="plotly_dark")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Top Barriers (1–3★)")
    neg = df[df["score"]<=3]

    if not neg.empty and net_cols:
        barriers = neg[net_cols].sum().sort_values(ascending=False).head(10)
        bdf = pd.DataFrame({
            "NET": barriers.index,
            "Pct": barriers.values / len(neg) * 100
        })

        fig2 = px.bar(bdf, x="Pct", y="NET", orientation="h")
        fig2.update_layout(template="plotly_dark")
        st.plotly_chart(fig2, use_container_width=True)

# =====================================================
# TAB 3 — HEAD TO HEAD
# =====================================================

elif nav == "⚔️ Head-to-Head":

    if len(sel_brands) >= 2:
        b1 = sel_brands[0]
        b2 = sel_brands[1]

        df1 = df[df["brand_name"]==b1]
        df2 = df[df["brand_name"]==b2]

        comp = pd.DataFrame({
            "Metric":["Avg Rating","Volume"],
            b1:[df1["score"].mean(), len(df1)],
            b2:[df2["score"].mean(), len(df2)]
        })

        st.dataframe(comp)

# =====================================================
# TAB 4 — PERIOD MATRIX
# =====================================================

elif nav == "📅 Period Matrix":

    grain = st.selectbox("Time Grain", ["Month","Week"])

    period_col = "Month" if grain=="Month" else "Week"

    matrix = df.groupby([period_col,"brand_name"]).size().unstack(fill_value=0)

    st.dataframe(matrix)

# =====================================================
# TAB 5 — TRENDS
# =====================================================

elif nav == "📈 Trends":

    trend = df.groupby(["Month","brand_name"])["score"].mean().reset_index()

    fig = px.line(trend, x="Month", y="score", color="brand_name", markers=True)
    fig.update_layout(template="plotly_dark")
    st.plotly_chart(fig, use_container_width=True)

# =====================================================
# TAB 6 — TEXT ANALYTICS
# =====================================================

elif nav == "🔡 Text Analytics":

    st.subheader("Top Words (Negative Reviews)")

    neg = df[df["score"]<=3]

    words = Counter()
    for text in neg["content"].dropna():
        clean = re.sub(r"[^a-zA-Z ]","", text.lower())
        words.update(clean.split())

    common = words.most_common(20)
    tdf = pd.DataFrame(common, columns=["Word","Count"])

    fig = px.bar(tdf, x="Count", y="Word", orientation="h")
    fig.update_layout(template="plotly_dark")
    st.plotly_chart(fig, use_container_width=True)
