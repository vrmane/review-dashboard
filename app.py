import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery

# =====================================================
# CONFIG
# =====================================================

PROJECT_ID = "app-review-analyzer-487309"
DATASET_ID = "app_reviews_ds"
TABLE_ID = "raw_reviews"

st.set_page_config(layout="wide")
st.title("📊 App Review Dashboard")

# =====================================================
# FAST DATA LOAD (Optimized)
# =====================================================

@st.cache_data(ttl=600)
def load_data(limit=50000):

    client = bigquery.Client()

    query = f"""
        SELECT
            review_id,
            brand_name,
            rating,
            sentiment,
            themes,
            date
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE DATE(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        ORDER BY date DESC
        LIMIT {limit}
    """

    return client.query(query).to_dataframe()

df = load_data()

if df.empty:
    st.warning("No data available.")
    st.stop()

# =====================================================
# DATA CLEANING
# =====================================================

df = df.copy()
df = df.replace({pd.NA: None})

if "date" in df.columns:
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["Month"] = df["date"].dt.to_period("M").astype(str)

if "sentiment" in df.columns:
    df["Sentiment_Label"] = df["sentiment"].fillna("Neutral")
else:
    df["Sentiment_Label"] = "Neutral"

# =====================================================
# TABS
# =====================================================

tabs = st.tabs(["Overview", "Trends", "Brands", "Themes"])

# =====================================================
# OVERVIEW
# =====================================================

with tabs[0]:

    total_reviews = len(df)
    avg_rating = round(df["rating"].mean(), 2) if "rating" in df.columns else 0
    negative_pct = round(
        (df["Sentiment_Label"] == "Negative").mean() * 100, 2
    )

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Reviews", total_reviews)
    col2.metric("Average Rating", avg_rating)
    col3.metric("Negative %", f"{negative_pct}%")

# =====================================================
# TRENDS
# =====================================================

with tabs[1]:

    if "Month" in df.columns and "rating" in df.columns:

        monthly = df.groupby("Month").agg(
            Reviews=("review_id", "count"),
            Rating=("rating", "mean")
        ).reset_index()

        monthly = monthly.fillna(0)

        fig_reviews = px.line(
            monthly,
            x="Month",
            y="Reviews",
            title="Monthly Reviews"
        )

        st.plotly_chart(
            fig_reviews,
            use_container_width=True,
            key="monthly_reviews_chart"
        )

        fig_rating = px.line(
            monthly,
            x="Month",
            y="Rating",
            title="Monthly Average Rating"
        )

        st.plotly_chart(
            fig_rating,
            use_container_width=True,
            key="monthly_rating_chart"
        )

    else:
        st.info("Required columns missing.")

# =====================================================
# BRANDS
# =====================================================

with tabs[2]:

    if "brand_name" not in df.columns:
        st.info("No brand column found.")
    else:

        brand = df.groupby("brand_name").agg(
            Reviews=("review_id", "count"),
            Rating=("rating", "mean"),
            Negative=("Sentiment_Label",
                      lambda x: (x == "Negative").mean() * 100)
        ).reset_index()

        brand = brand.sort_values("Reviews", ascending=False)
        brand = brand.fillna(0)

        st.dataframe(brand, use_container_width=True)

        fig_brand_reviews = px.bar(
            brand,
            x="brand_name",
            y="Reviews",
            title="Reviews by Brand"
        )

        st.plotly_chart(
            fig_brand_reviews,
            use_container_width=True,
            key="brand_reviews_chart"
        )

# =====================================================
# THEMES
# =====================================================

with tabs[3]:

    if "themes" not in df.columns:
        st.info("No themes column available.")
    else:

        theme_df = df.copy()

        theme_df["themes"] = theme_df["themes"].apply(
            lambda x: x if isinstance(x, list) else []
        )

        theme_df = theme_df.explode("themes")

        theme_df["themes"] = theme_df["themes"].fillna("Unknown")

        th = theme_df.groupby("themes").agg(
            Reviews=("review_id", "count"),
            Negative=("Sentiment_Label",
                      lambda x: (x == "Negative").mean() * 100)
        ).reset_index()

        th = th.rename(columns={"themes": "Theme"})
        th = th.fillna(0)

        st.dataframe(
            th.sort_values("Reviews", ascending=False),
            use_container_width=True
        )

        th = th.dropna(subset=["Reviews", "Negative"])

        fig_scatter = px.scatter(
            th,
            x="Reviews",
            y="Negative",
            size="Reviews",
            color="Negative",
            hover_name="Theme",
            title="Theme Performance (Volume vs Negative %)"
        )

        st.plotly_chart(
            fig_scatter,
            use_container_width=True,
            key="theme_scatter_chart"
        )
