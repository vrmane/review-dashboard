import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import re

# ==========================================================
# 1. PAGE CONFIG
# ==========================================================

st.set_page_config(
    page_title="Strategic Intelligence Platform",
    page_icon="🦅",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==========================================================
# 2. EXECUTIVE DARK CSS
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

div[role="radiogroup"] {
    background-color: #1e293b;
    padding: 8px;
    border-radius: 12px;
    display: flex;
    justify-content: space-around;
    margin-bottom: 20px;
    border: 1px solid #334155;
}
div[role="radiogroup"] label[data-checked="true"] {
    background-color: #38bdf8 !important;
    color: #0f172a !important;
    font-weight: bold;
}
</style>
""", unsafe_allow_html=True)

# ==========================================================
# 3. BIGQUERY CONNECTION
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
# 4. LOAD DATA
# ==========================================================

@st.cache_data(ttl=900)
def load_data():
    query = """
    SELECT *
    FROM `app-review-analyzer-487309.app_reviews_ds.raw_reviews`
    """
    df = bq.query(query).to_dataframe()

    # Date fix
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
        bins=[0,2,3,5],
        labels=["Negative","Neutral","Positive"]
    )

    # Ensure products is always list
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
# 5. ROBUST NET DETECTION (LIKE SUPABASE VERSION)
# ==========================================================

def detect_net_columns(df):

    exclude = [
        "review_id","date","brand_name","app_id",
        "content","rating","products","themes",
        "Month","Week","Sentiment_Label"
    ]

    theme_cols = []

    for col in df.columns:

        if col in exclude:
            continue

        col_upper = str(col).upper()

        # Strategy 1: Name contains NET
        if "NET" in col_upper:
            theme_cols.append(col)
            continue

        # Strategy 2: Binary numeric 0/1
        series = pd.to_numeric(df[col], errors="coerce")
        unique_vals = set(series.dropna().unique())

        if unique_vals.issubset({0,1,0.0,1.0}) and len(unique_vals) <= 2:
            theme_cols.append(col)

    return theme_cols

theme_cols = detect_net_columns(df_raw)

# ==========================================================
# 6. SIDEBAR FILTERS
# ==========================================================

with st.sidebar:

    st.title("🎛️ Command Center")
    st.success(f"🟢 Live: {len(df_raw):,} Rows")
    st.markdown("---")

    min_d = df_raw["date"].min().date()
    max_d = df_raw["date"].max().date()

    date_range = st.date_input(
        "Period",
        [min_d, max_d],
        min_value=min_d,
        max_value=max_d
    )

    brands = sorted(df_raw["brand_name"].dropna().unique())
    sel_brands = st.multiselect("Brands", brands, default=brands)

    # PRODUCT FILTER (FIXED)
    all_products = sorted(
        {p for sub in df_raw["products"] for p in sub}
    )

    sel_products = st.multiselect("Products", all_products)

    sel_ratings = st.multiselect("Ratings",[1,2,3,4,5],[1,2,3,4,5])

# ==========================================================
# 7. APPLY FILTERS
# ==========================================================

df = df_raw.copy()
mask = pd.Series(True, index=df.index)

if len(date_range) == 2:
    start = pd.to_datetime(date_range[0])
    end = pd.to_datetime(date_range[1]) + timedelta(days=1)
    mask &= (df["date"] >= start) & (df["date"] < end)

mask &= df["brand_name"].isin(sel_brands)
mask &= df["rating"].isin(sel_ratings)

if sel_products:
    mask &= df["products"].apply(
        lambda x: any(p in x for p in sel_products)
    )

df = df[mask]

# ==========================================================
# 8. NAVIGATION
# ==========================================================

st.title("🦅 Strategic Intelligence Platform")

nav = st.radio(
    "Navigation",
    ["📊 Boardroom Summary","🚀 Drivers & Barriers"],
    horizontal=True,
    label_visibility="collapsed"
)

st.markdown("---")

# ==========================================================
# TAB 1 — BOARDROOM
# ==========================================================

if nav == "📊 Boardroom Summary":

    total = len(df)
    avg_rating = df["rating"].mean()

    col1,col2,col3,col4 = st.columns(4)

    with col1:
        st.metric("Total Volume", f"{total:,}")

    with col2:
        st.metric("Avg Rating", f"{avg_rating:.2f} ⭐")

    with col3:
        prom = len(df[df["rating"]==5])
        det = len(df[df["rating"]<=3])
        nps = ((prom-det)/total*100) if total else 0
        st.metric("NPS Proxy", f"{nps:.0f}")

    with col4:
        risk = (len(df[df["rating"]==1])/total*100) if total else 0
        st.metric("1★ Risk %", f"{risk:.1f}%")

# ==========================================================
elif nav == "🚀 Drivers & Barriers":

    st.markdown("### 🎯 Strategic Impact Matrix")

    if not theme_cols:
        st.warning("No NET columns detected.")
        st.stop()

    total_reviews = len(df)

    stats = []

    for t in theme_cols:

        # ✅ FORCE NUMERIC CONVERSION
        series = pd.to_numeric(df[t], errors="coerce").fillna(0)

        count = series.sum()

        # ✅ Safe numeric comparison
        if float(count) > 0:

            avg = df.loc[series == 1, "rating"].mean()

            stats.append({
                "Theme": t,
                "Frequency (%)": (count / total_reviews) * 100,
                "Avg Rating When Present": avg
            })

    impact_df = pd.DataFrame(stats)

    if impact_df.empty:
        st.warning("Themes detected but no positive frequency found.")
        st.stop()

    fig = px.scatter(
        impact_df,
        x="Frequency (%)",
        y="Avg Rating When Present",
        text="Theme",
        size="Frequency (%)",
        color="Avg Rating When Present",
        color_continuous_scale="RdYlGn"
    )

    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ======================================================
    # AGGREGATED DRIVERS & BARRIERS (SAFE VERSION)
    # ======================================================

    pos_df = df[df["rating"] >= 4]
    neg_df = df[df["rating"] <= 3]

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🚀 Top Drivers")

        if not pos_df.empty:

            base = len(pos_df)

            theme_data = {}

            for t in theme_cols:
                series = pd.to_numeric(pos_df[t], errors="coerce").fillna(0)
                theme_data[t] = series.sum()

            counts = pd.Series(theme_data).sort_values(ascending=False).head(10)
            pct = counts / base * 100

            fig = px.bar(
                x=pct.values,
                y=pct.index,
                orientation="h",
                color_discrete_sequence=["#10b981"]
            )

            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("🛑 Top Barriers")

        if not neg_df.empty:

            base = len(neg_df)

            theme_data = {}

            for t in theme_cols:
                series = pd.to_numeric(neg_df[t], errors="coerce").fillna(0)
                theme_data[t] = series.sum()

            counts = pd.Series(theme_data).sort_values(ascending=False).head(10)
            pct = counts / base * 100

            fig = px.bar(
                x=pct.values,
                y=pct.index,
                orientation="h",
                color_discrete_sequence=["#ef4444"]
            )

            st.plotly_chart(fig, use_container_width=True)
