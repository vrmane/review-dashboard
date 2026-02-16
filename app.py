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
st.title("📊 App Review Intelligence Dashboard")

# =====================================================
# DATA LOAD
# =====================================================

@st.cache_data(ttl=600)
def load_data(days=120, limit=150000):

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
        WHERE DATE(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        ORDER BY date DESC
        LIMIT {limit}
    """

    df = client.query(query).to_dataframe()
    return df


df = load_data()

if df.empty:
    st.warning("No data available.")
    st.stop()

# =====================================================
# CLEANING
# =====================================================

df = df.copy()
df = df.replace({pd.NA: None})

df["date"] = pd.to_datetime(df["date"], errors="coerce")
df["Month"] = df["date"].dt.to_period("M").astype(str)

df["Sentiment_Label"] = df["sentiment"].fillna("Neutral")

df["Rating_Bucket"] = pd.cut(
    df["rating"],
    bins=[0,2,3,4,5],
    labels=["1-2","3","4","5"]
)

# =====================================================
# SIDEBAR FILTERS
# =====================================================

st.sidebar.header("Filters")

brand_filter = st.sidebar.multiselect(
    "Brand",
    sorted(df["brand_name"].dropna().unique())
)

sentiment_filter = st.sidebar.multiselect(
    "Sentiment",
    sorted(df["Sentiment_Label"].unique())
)

rating_filter = st.sidebar.multiselect(
    "Rating Bucket",
    sorted(df["Rating_Bucket"].dropna().unique())
)

date_range = st.sidebar.date_input(
    "Date Range",
    [df["date"].min(), df["date"].max()]
)

# APPLY FILTERS

mask = pd.Series(True, index=df.index)

if brand_filter:
    mask &= df["brand_name"].isin(brand_filter)

if sentiment_filter:
    mask &= df["Sentiment_Label"].isin(sentiment_filter)

if rating_filter:
    mask &= df["Rating_Bucket"].isin(rating_filter)

if len(date_range) == 2:
    mask &= df["date"].between(
        pd.to_datetime(date_range[0]),
        pd.to_datetime(date_range[1])
    )

df = df[mask]

# =====================================================
# TABS
# =====================================================

tabs = st.tabs([
    "Overview",
    "Trends",
    "Brand Intelligence",
    "Theme Intelligence",
    "Deep Dive Table"
])

# =====================================================
# OVERVIEW
# =====================================================

with tabs[0]:

    total_reviews = len(df)
    avg_rating = round(df["rating"].mean(),2)
    neg_pct = (df["Sentiment_Label"]=="Negative").mean()*100
    pos_pct = (df["Sentiment_Label"]=="Positive").mean()*100

    col1,col2,col3,col4 = st.columns(4)

    col1.metric("Total Reviews", total_reviews)
    col2.metric("Avg Rating", avg_rating)
    col3.metric("Positive %", f"{pos_pct:.1f}%")
    col4.metric("Negative %", f"{neg_pct:.1f}%")

    st.divider()

    st.subheader("Sentiment Distribution")

    fig = px.histogram(
        df,
        x="Sentiment_Label",
        color="Sentiment_Label"
    )
    st.plotly_chart(fig, use_container_width=True)

# =====================================================
# TRENDS
# =====================================================

with tabs[1]:

    monthly = df.groupby("Month").agg(
        Reviews=("review_id","count"),
        Rating=("rating","mean"),
        Negative=("Sentiment_Label",
                  lambda x:(x=="Negative").mean()*100)
    ).reset_index()

    st.subheader("Review Trend")

    st.plotly_chart(
        px.line(monthly,x="Month",y="Reviews"),
        use_container_width=True
    )

    st.subheader("Rating Trend")

    st.plotly_chart(
        px.line(monthly,x="Month",y="Rating"),
        use_container_width=True
    )

    st.subheader("Negative Trend")

    st.plotly_chart(
        px.line(monthly,x="Month",y="Negative"),
        use_container_width=True
    )

# =====================================================
# BRAND INTELLIGENCE
# =====================================================

with tabs[2]:

    brand = df.groupby("brand_name").agg(
        Reviews=("review_id","count"),
        Rating=("rating","mean"),
        Negative=("Sentiment_Label",
                  lambda x:(x=="Negative").mean()*100)
    ).reset_index().sort_values("Reviews",ascending=False)

    st.dataframe(brand,use_container_width=True)

    st.subheader("Review Volume")

    st.plotly_chart(
        px.bar(brand,x="brand_name",y="Reviews"),
        use_container_width=True
    )

    st.subheader("Brand Perception Map")

    st.plotly_chart(
        px.scatter(
            brand,
            x="Reviews",
            y="Negative",
            size="Reviews",
            hover_name="brand_name"
        ),
        use_container_width=True
    )

# =====================================================
# THEME INTELLIGENCE
# =====================================================

with tabs[3]:

    theme_df = df.copy()

    theme_df["themes"] = theme_df["themes"].apply(
        lambda x: x if isinstance(x,list) else []
    )

    theme_df = theme_df.explode("themes")
    theme_df["themes"] = theme_df["themes"].fillna("Unknown")

    th = theme_df.groupby("themes").agg(
        Reviews=("review_id","count"),
        Negative=("Sentiment_Label",
                  lambda x:(x=="Negative").mean()*100),
        Rating=("rating","mean")
    ).reset_index()

    th["Impact_Score"] = th["Reviews"] * th["Negative"]

    st.dataframe(
        th.sort_values("Impact_Score",ascending=False),
        use_container_width=True
    )

    st.subheader("Top Complaint Drivers")

    st.plotly_chart(
        px.bar(
            th.sort_values("Impact_Score",ascending=False).head(15),
            x="themes",
            y="Impact_Score"
        ),
        use_container_width=True
    )

    st.subheader("Theme Performance Matrix")

    st.plotly_chart(
        px.scatter(
            th,
            x="Reviews",
            y="Negative",
            size="Impact_Score",
            hover_name="themes"
        ),
        use_container_width=True
    )

# =====================================================
# RAW DATA TABLE
# =====================================================

with tabs[4]:

    st.dataframe(df,use_container_width=True)

    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download Filtered Data",
        csv,
        "filtered_reviews.csv",
        "text/csv"
    )

# =====================================================
# AUTO INSIGHTS
# =====================================================

st.divider()
st.subheader("Automated Insights")

if not df.empty:

    worst_theme = th.sort_values("Impact_Score",ascending=False).iloc[0]
    best_brand = brand.sort_values("Rating",ascending=False).iloc[0]
    worst_brand = brand.sort_values("Negative",ascending=False).iloc[0]

    st.info(f"""
    • Biggest pain point: **{worst_theme.themes}**
    appears in **{int(worst_theme.Reviews)} reviews**
    with **{worst_theme.Negative:.1f}% negative sentiment**

    • Best performing brand: **{best_brand.brand_name}**
    with avg rating **{best_brand.Rating:.2f}**

    • Brand needing attention: **{worst_brand.brand_name}**
    showing **{worst_brand.Negative:.1f}% negative sentiment**
    """)
