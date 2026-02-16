import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta

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
# LOAD DATA
# ==========================================================

@st.cache_data(ttl=900)
def load_data():
    query = """
    SELECT *
    FROM `app-review-analyzer-487309.app_reviews_ds.raw_reviews`
    """
    df = bq.query(query).to_dataframe()

    # Fix timezone
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

    # Ensure products is list
    if "products" in df.columns:
        df["products"] = df["products"].apply(
            lambda x: x if isinstance(x, list) else []
        )

    return df

df_raw = load_data()

if df_raw.empty:
    st.error("No Data Found.")
    st.stop()

# ==========================================================
# ROBUST NET COLUMN DETECTION
# ==========================================================

def detect_net_columns(df):

    exclude_cols = [
        "rating", "review_id", "date",
        "brand_name", "app_id",
        "products", "themes"
    ]

    net_cols = []

    for col in df.columns:

        if col in exclude_cols:
            continue

        series = pd.to_numeric(df[col], errors="coerce")

        if series.notna().sum() == 0:
            continue

        unique_vals = set(series.dropna().unique())

        # Accept 0 / 1 / 0.0 / 1.0
        if unique_vals.issubset({0, 1, 0.0, 1.0}):
            net_cols.append(col)

    return net_cols

theme_cols = detect_net_columns(df_raw)

# ==========================================================
# SIDEBAR FILTERS
# ==========================================================

with st.sidebar:
    st.title("🎛️ Command Center")
    st.success(f"🟢 Live Rows: {len(df_raw):,}")

    # DATE FILTER
    min_d, max_d = df_raw["date"].min().date(), df_raw["date"].max().date()
    date_range = st.date_input(
        "Period",
        [min_d, max_d],
        min_value=min_d,
        max_value=max_d
    )

    # BRAND FILTER
    brands = sorted(df_raw["brand_name"].dropna().unique())
    sel_brands = st.multiselect("Brands", brands, default=brands)

    # PRODUCT FILTER (ARRAY SUPPORT)
    all_products = sorted(
        {p for sublist in df_raw["products"] for p in sublist}
    )

    sel_products = st.multiselect("Products", all_products)

    # RATING FILTER
    ratings = st.multiselect("Ratings", [1,2,3,4,5], default=[1,2,3,4,5])

# ==========================================================
# APPLY FILTERS
# ==========================================================

df = df_raw.copy()
mask = pd.Series(True, index=df.index)

if len(date_range) == 2:
    start = pd.to_datetime(date_range[0])
    end = pd.to_datetime(date_range[1]) + timedelta(days=1)
    mask &= (df["date"] >= start) & (df["date"] < end)

mask &= df["brand_name"].isin(sel_brands)
mask &= df["rating"].isin(ratings)

# Product filter logic (ARRAY contains)
if sel_products:
    mask &= df["products"].apply(
        lambda x: any(p in x for p in sel_products)
    )

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

total = len(df)

with col1:
    st.metric("Total Reviews", f"{total:,}")

with col2:
    st.metric("Avg Rating", f"{df['rating'].mean():.2f} ⭐" if total else "0")

with col3:
    promoters = len(df[df["rating"] == 5])
    detractors = len(df[df["rating"] <= 3])
    nps = ((promoters - detractors) / total * 100) if total else 0
    st.metric("NPS Proxy", f"{nps:.0f}")

with col4:
    risk = (len(df[df["rating"] == 1]) / total * 100) if total else 0
    st.metric("1★ Risk %", f"{risk:.1f}%")

st.markdown("---")

# ==========================================================
# 🚀 DRIVERS & 🛑 BARRIERS
# ==========================================================

st.header("🚀 Drivers & 🛑 Barriers")

if not theme_cols:
    st.warning("No NET columns detected.")
else:

    drivers_df = df[df["rating"] >= 4]
    barriers_df = df[df["rating"] <= 3]

    col_d, col_b = st.columns(2)

    with col_d:
        st.subheader("🚀 Top Drivers")

        if drivers_df.empty:
            st.info("No positive reviews.")
        else:
            base = len(drivers_df)
            counts = drivers_df[theme_cols].sum().sort_values(ascending=False).head(10)
            pct = (counts / base * 100).round(1)

            fig = px.bar(
                x=pct.values,
                y=pct.index,
                orientation="h",
                text=pct.values,
                color_discrete_sequence=["#10b981"]
            )
            fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig.update_layout(template="plotly_dark", yaxis={"categoryorder":"total ascending"})
            st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.subheader("🛑 Top Barriers")

        if barriers_df.empty:
            st.info("No negative reviews.")
        else:
            base = len(barriers_df)
            counts = barriers_df[theme_cols].sum().sort_values(ascending=False).head(10)
            pct = (counts / base * 100).round(1)

            fig = px.bar(
                x=pct.values,
                y=pct.index,
                orientation="h",
                text=pct.values,
                color_discrete_sequence=["#ef4444"]
            )
            fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig.update_layout(template="plotly_dark", yaxis={"categoryorder":"total ascending"})
            st.plotly_chart(fig, use_container_width=True)
