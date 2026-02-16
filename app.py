import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pytz

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
    layout="wide"
)

st.markdown("""
<style>
.stApp { background-color: #0b0f19; color: #e2e8f0; }
div[data-testid="metric-container"] {
    background: linear-gradient(180deg, #1e293b 0%, #0f172a 100%);
    border-radius: 12px;
    padding: 20px;
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
# LOAD DATA (SERVER SIDE FILTERED)
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
df["score"] = pd.to_numeric(df["rating"])

# Detect NET columns (binary INT columns except core fields)
core_cols = [
    "app_id","brand_name","review_id","date","content",
    "rating","sentiment","products","themes","Month","score"
]

net_cols = [
    c for c in df.columns
    if c not in core_cols and df[c].dropna().isin([0,1]).all()
]

# =====================================================
# SIDEBAR
# =====================================================

with st.sidebar:
    st.title("🎛 Filters")

    brands = sorted(df["brand_name"].unique())
    sel_brand = st.multiselect("Brand", brands, default=brands)

    min_date = df["date"].min()
    max_date = df["date"].max()
    sel_date = st.date_input(
        "Date Range",
        [min_date, max_date],
        min_value=min_date,
        max_value=max_date
    )

    sel_rating = st.multiselect("Rating", [1,2,3,4,5], default=[1,2,3,4,5])

# =====================================================
# APPLY FILTERS
# =====================================================

mask = df["brand_name"].isin(sel_brand)
mask &= df["score"].isin(sel_rating)

if len(sel_date) == 2:
    mask &= df["date"].between(
        pd.to_datetime(sel_date[0]),
        pd.to_datetime(sel_date[1])
    )

df = df[mask]

# =====================================================
# HEADER
# =====================================================

st.title("🦅 Strategic Intelligence Platform")

ist = pytz.timezone("Asia/Kolkata")
st.caption(f"Last refreshed: {datetime.now(ist).strftime('%d %b %Y %I:%M %p IST')}")

# =====================================================
# KPI ROW
# =====================================================

col1, col2, col3, col4 = st.columns(4)

total_vol = len(df)
avg_rating = df["score"].mean()

prom = len(df[df["score"]==5])
det = len(df[df["score"]<=3])
nps = ((prom - det) / total_vol * 100) if total_vol else 0
risk = (len(df[df["score"]==1]) / total_vol * 100) if total_vol else 0

col1.metric("Total Reviews", f"{total_vol:,}")
col2.metric("Average Rating", f"{avg_rating:.2f} ⭐")
col3.metric("NPS Proxy", f"{nps:.0f}")
col4.metric("1★ Risk %", f"{risk:.1f}%")

st.markdown("---")

# =====================================================
# BRAND BREAKDOWN
# =====================================================

brand_kpi = df.groupby("brand_name").agg(
    Volume=("score","count"),
    Avg_Rating=("score","mean")
).reset_index()

fig = px.bar(
    brand_kpi,
    x="brand_name",
    y="Volume",
    title="Volume by Brand"
)
fig.update_layout(template="plotly_dark")
st.plotly_chart(fig, use_container_width=True)

# =====================================================
# SENTIMENT STACK
# =====================================================

sent_df = df.copy()
sent_df["Sentiment_Label"] = pd.cut(
    sent_df["score"],
    bins=[0,2,3,5],
    labels=["Negative","Neutral","Positive"]
)

sent = sent_df.groupby(["brand_name","Sentiment_Label"]).size().reset_index(name="Count")
total = sent.groupby("brand_name")["Count"].transform("sum")
sent["Pct"] = sent["Count"] / total * 100

fig2 = px.bar(
    sent,
    x="brand_name",
    y="Pct",
    color="Sentiment_Label",
    barmode="stack",
    title="Sentiment Distribution (%)"
)
fig2.update_layout(template="plotly_dark")
st.plotly_chart(fig2, use_container_width=True)

# =====================================================
# TOP NET DRIVERS (4-5★)
# =====================================================

st.markdown("### 🚀 Top Drivers (4-5★)")

pos_df = df[df["score"]>=4]

if not pos_df.empty and net_cols:
    driver_counts = pos_df[net_cols].sum().sort_values(ascending=False).head(10)
    driver_df = pd.DataFrame({
        "NET": driver_counts.index,
        "Count": driver_counts.values
    })
    driver_df["Pct"] = driver_df["Count"] / len(pos_df) * 100

    fig3 = px.bar(
        driver_df,
        x="Pct",
        y="NET",
        orientation="h",
        title="Top Positive Drivers"
    )
    fig3.update_layout(template="plotly_dark")
    st.plotly_chart(fig3, use_container_width=True)

# =====================================================
# TOP NET BARRIERS (1-3★)
# =====================================================

st.markdown("### 🛑 Top Barriers (1-3★)")

neg_df = df[df["score"]<=3]

if not neg_df.empty and net_cols:
    barrier_counts = neg_df[net_cols].sum().sort_values(ascending=False).head(10)
    barrier_df = pd.DataFrame({
        "NET": barrier_counts.index,
        "Count": barrier_counts.values
    })
    barrier_df["Pct"] = barrier_df["Count"] / len(neg_df) * 100

    fig4 = px.bar(
        barrier_df,
        x="Pct",
        y="NET",
        orientation="h",
        title="Top Negative Barriers"
    )
    fig4.update_layout(template="plotly_dark")
    st.plotly_chart(fig4, use_container_width=True)
