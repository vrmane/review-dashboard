import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
from collections import Counter
import re

# ==========================================================
# PAGE CONFIG
# ==========================================================

st.set_page_config(
    page_title="Strategic Intelligence Platform",
    page_icon="🦅",
    layout="wide"
)

# ==========================================================
# DARK EXECUTIVE CSS
# ==========================================================

st.markdown("""
<style>
.stApp { background-color: #0b0f19; color: #e2e8f0; }
div[data-testid="metric-container"] {
    background: linear-gradient(180deg, #1e293b 0%, #0f172a 100%);
    border: 1px solid #334155;
    border-radius: 12px;
    padding: 20px;
}
hr { border-color: #334155; }
</style>
""", unsafe_allow_html=True)

# ==========================================================
# BIGQUERY CONNECTION
# ==========================================================

@st.cache_resource
def init_bq():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(
        credentials=credentials,
        project=credentials.project_id
    )

bq = init_bq()

# ==========================================================
# LOAD DATA FROM BIGQUERY
# ==========================================================

@st.cache_data(ttl=900)
def load_data():
    query = """
    SELECT *
    FROM `app-review-analyzer-487309.app_reviews_ds.raw_reviews`
    """
    df = bq.query(query).to_dataframe()

    # ✅ FIX TIMEZONE ISSUE (CRITICAL)
    df["date"] = (
        pd.to_datetime(df["date"], utc=True)
        .dt.tz_convert("Asia/Kolkata")
        .dt.tz_localize(None)
    )

    df["Month"] = df["date"].dt.strftime("%Y-%m")
    df["Week"] = df["date"].dt.strftime("%Y-W%V")

    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

    df["Sentiment_Label"] = pd.cut(
        df["rating"],
        bins=[0, 2, 3, 5],
        labels=["Negative", "Neutral", "Positive"]
    )

    return df

df_raw = load_data()

if df_raw.empty:
    st.error("No Data Found.")
    st.stop()

# ==========================================================
# AUTO DETECT NET COLUMNS (0/1)
# ==========================================================

def detect_net_columns(df):
    net_cols = []
    for col in df.columns:
        if df[col].dtype in [np.int64, np.int32, np.int8]:
            if set(df[col].dropna().unique()).issubset({0, 1}):
                net_cols.append(col)
    return net_cols

theme_cols = detect_net_columns(df_raw)

# ==========================================================
# SIDEBAR FILTERS
# ==========================================================

with st.sidebar:
    st.title("🎛️ Command Center")
    st.success(f"🟢 Live Rows: {len(df_raw):,}")

    min_d, max_d = df_raw["date"].min().date(), df_raw["date"].max().date()
    date_range = st.date_input(
        "Period",
        [min_d, max_d],
        min_value=min_d,
        max_value=max_d
    )

    brands = sorted(df_raw["brand_name"].dropna().unique())
    sel_brands = st.multiselect("Brands", brands, default=brands)

    ratings = st.multiselect("Ratings", [1,2,3,4,5], default=[1,2,3,4,5])

# ==========================================================
# APPLY FILTERS (FIXED VERSION)
# ==========================================================

df = df_raw.copy()

mask = pd.Series(True, index=df.index)

if len(date_range) == 2:
    start = pd.to_datetime(date_range[0])
    end = pd.to_datetime(date_range[1]) + timedelta(days=1)
    mask &= (df["date"] >= start) & (df["date"] < end)

mask &= df["brand_name"].isin(sel_brands)
mask &= df["rating"].isin(ratings)

df = df[mask]

# ==========================================================
# DASHBOARD
# ==========================================================

st.title("🦅 Strategic Intelligence Platform")
st.markdown("---")

# ==========================================================
# KPI SECTION
# ==========================================================

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Reviews", f"{len(df):,}")

with col2:
    st.metric("Avg Rating", f"{df['rating'].mean():.2f} ⭐")

with col3:
    promoters = len(df[df["rating"] == 5])
    detractors = len(df[df["rating"] <= 3])
    total = len(df)
    nps = ((promoters - detractors) / total * 100) if total else 0
    st.metric("NPS Proxy", f"{nps:.0f}")

with col4:
    risk = (len(df[df["rating"] == 1]) / total * 100) if total else 0
    st.metric("1★ Risk %", f"{risk:.1f}%")

st.markdown("---")

# ==========================================================
# BRAND SHARE
# ==========================================================

st.subheader("📊 Volume Share by Brand")

brand_vol = df.groupby("brand_name").size().reset_index(name="Volume")

fig_donut = px.pie(
    brand_vol,
    values="Volume",
    names="brand_name",
    hole=0.5
)

fig_donut.update_layout(template="plotly_dark")
st.plotly_chart(fig_donut, use_container_width=True)

# ==========================================================
# SENTIMENT SPLIT
# ==========================================================

st.subheader("😊 Sentiment Split")

sent_df = (
    df.groupby(["brand_name", "Sentiment_Label"])
    .size()
    .reset_index(name="Count")
)

fig_sent = px.bar(
    sent_df,
    x="brand_name",
    y="Count",
    color="Sentiment_Label",
    barmode="stack"
)

fig_sent.update_layout(template="plotly_dark")
st.plotly_chart(fig_sent, use_container_width=True)

# ==========================================================
# TOP THEMES (NET)
# ==========================================================

st.subheader("🚀 Top Themes (Overall)")

if theme_cols:
    theme_counts = (
        df[theme_cols]
        .sum()
        .sort_values(ascending=False)
        .head(15)
    )

    theme_df = pd.DataFrame({
        "Theme": theme_counts.index,
        "Count": theme_counts.values
    })

    fig_theme = px.bar(
        theme_df,
        x="Count",
        y="Theme",
        orientation="h"
    )

    fig_theme.update_layout(
        template="plotly_dark",
        yaxis={"categoryorder":"total ascending"}
    )

    st.plotly_chart(fig_theme, use_container_width=True)

else:
    st.info("No NET columns detected.")

# ==========================================================
# MONTHLY TREND
# ==========================================================

st.subheader("📈 Monthly CSAT Trend")

trend = (
    df.groupby(["Month", "brand_name"])["rating"]
    .mean()
    .reset_index()
)

fig_trend = px.line(
    trend,
    x="Month",
    y="rating",
    color="brand_name",
    markers=True
)

fig_trend.update_layout(template="plotly_dark")
st.plotly_chart(fig_trend, use_container_width=True)
