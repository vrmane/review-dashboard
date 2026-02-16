import streamlit as st
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account

# =====================================================
# PAGE CONFIG
# =====================================================
st.set_page_config(layout="wide", page_title="Monthly Brand Comparison")

st.title("📊 Monthly Brand Comparison")

# =====================================================
# BIGQUERY CONNECTION
# =====================================================
@st.cache_resource
def get_client():
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(credentials=creds)

# =====================================================
# LOAD DATA
# =====================================================
@st.cache_data(ttl=600)
def load_data():
    client = get_client()

    query = """
        SELECT *
        FROM `app-review-analyzer-487309.app_reviews_ds.raw_reviews`
        WHERE DATE(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
    """

    df = client.query(query).to_dataframe()

    # ---------- STANDARDIZE COLUMN NAMES ----------
    cols = {c.lower(): c for c in df.columns}

    def find(name_options):
        for n in name_options:
            if n.lower() in cols:
                return cols[n.lower()]
        return None

    col_brand = find(["brand_name","app_name","brand"])
    col_date = find(["date","review_date"])
    col_rating = find(["rating","score"])

    if not col_brand or not col_date or not col_rating:
        st.error("Required columns missing in table.")
        st.stop()

    df = df.rename(columns={
        col_brand: "Brand",
        col_date: "Date",
        col_rating: "Rating"
    })

    # ---------- DATE PARSE ----------
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True)
    df = df.dropna(subset=["Date"])

    df["Month"] = df["Date"].dt.to_period("M").astype(str)

    # ---------- FIND THEME COLUMNS ----------
    non_theme = {"Brand","Date","Rating","Month","review_id","content","sentiment","products","themes","app_id"}
    theme_cols = [c for c in df.columns if c not in non_theme and df[c].dropna().isin([0,1]).all()]

    return df, theme_cols


df, theme_cols = load_data()

if df.empty:
    st.warning("No data found.")
    st.stop()

# =====================================================
# HELPERS
# =====================================================
def build_table(ratings):

    d = df[df["Rating"].isin(ratings)].copy()
    if d.empty:
        return pd.DataFrame()

    brands = sorted(d["Brand"].dropna().unique())
    months = sorted(d["Month"].unique())[-6:]

    # ---------- BASE ----------
    base = d.groupby(["Brand","Month"]).size().unstack(fill_value=0)

    # ---------- THEME COUNTS ----------
    records = []

    for theme in theme_cols:

        temp = d[d[theme]==1]
        if temp.empty:
            continue

        counts = temp.groupby(["Brand","Month"]).size().unstack(fill_value=0)

        row = {"Theme":theme}

        for m in months:
            for b in brands:
                b_base = base.get(m, pd.Series()).get(b,0) if m in base.columns else 0
                val = counts.get(m, pd.Series()).get(b,0) if m in counts.columns else 0
                row[f"{m}|{b}"] = val/b_base if b_base else 0

        records.append(row)

    if not records:
        return pd.DataFrame()

    out = pd.DataFrame(records).set_index("Theme")
    return out.sort_index()


# =====================================================
# TABS
# =====================================================
tab1, tab2 = st.tabs(["⭐ Drivers (4-5★)", "⚠️ Barriers (1-3★)"])

# =====================================================
# DRIVERS
# =====================================================
with tab1:

    st.subheader("Monthly Drivers Comparison")

    tbl = build_table([4,5])

    if tbl.empty:
        st.info("No driver data.")
    else:
        st.dataframe(
            tbl.style.format("{:.0%}"),
            use_container_width=True
        )

# =====================================================
# BARRIERS
# =====================================================
with tab2:

    st.subheader("Monthly Barriers Comparison")

    tbl = build_table([1,2,3])

    if tbl.empty:
        st.info("No barrier data.")
    else:
        st.dataframe(
            tbl.style.format("{:.0%}"),
            use_container_width=True
        )
