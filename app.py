import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# ======================================================
# CONFIG
# ======================================================

PROJECT_ID = "app-review-analyzer-487309"
DATASET_ID = "app_reviews_ds"
TABLE_ID = "raw_reviews"

st.set_page_config(
    page_title="Review Intelligence",
    layout="wide"
)

# ======================================================
# BIGQUERY CONNECTION (Streamlit Cloud Safe)
# ======================================================

@st.cache_resource
def init_bq():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(
        credentials=credentials,
        project=PROJECT_ID,
    )

bq = init_bq()

# ======================================================
# QUERY FUNCTION (FILTERED, SCALABLE)
# ======================================================

@st.cache_data(ttl=600)
def load_data(start_date, end_date, brands, ratings):

    brand_filter = ""
    rating_filter = ""

    if brands:
        brand_list = ",".join([f"'{b}'" for b in brands])
        brand_filter = f"AND brand_name IN ({brand_list})"

    if ratings:
        rating_list = ",".join(map(str, ratings))
        rating_filter = f"AND rating IN ({rating_list})"

    query = f"""
        SELECT
            brand_name,
            DATE(date) as review_date,
            rating,
            sentiment,
            products,
            themes
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE DATE(date) BETWEEN '{start_date}' AND '{end_date}'
        {brand_filter}
        {rating_filter}
        LIMIT 200000
    """

    df = bq.query(query).to_dataframe()

    # Ensure ARRAY columns are lists
    df["products"] = df["products"].apply(lambda x: x if isinstance(x, list) else [])
    df["themes"] = df["themes"].apply(lambda x: x if isinstance(x, list) else [])

    return df


# ======================================================
# UI
# ======================================================

st.title("📊 Review Intelligence Dashboard")

# Sidebar filters
with st.sidebar:

    st.header("Filters")

    # Date range
    min_date = pd.to_datetime("2025-12-01").date()
    max_date = pd.Timestamp.today().date()

    date_range = st.date_input(
        "Date Range",
        [min_date, max_date],
        min_value=min_date,
        max_value=max_date
    )

    # Brand filter
    brands = st.multiselect(
        "Brand",
        ["MoneyView", "Navi", "KreditBee", "EarlySalary", "Kissht"],
        default=["MoneyView", "Navi", "KreditBee", "EarlySalary", "Kissht"]
    )

    # Rating filter
    ratings = st.multiselect(
        "Ratings",
        [1, 2, 3, 4, 5],
        default=[1, 2, 3, 4, 5]
    )

# Validate date selection
if len(date_range) != 2:
    st.warning("Please select a valid date range.")
    st.stop()

start_date = date_range[0]
end_date = date_range[1]

# Load data
with st.spinner("Loading data from BigQuery..."):
    df = load_data(start_date, end_date, brands, ratings)

if df.empty:
    st.warning("No data found for selected filters.")
    st.stop()

# ======================================================
# KPIs
# ======================================================

col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Total Reviews", f"{len(df):,}")

with col2:
    st.metric("Average Rating", f"{df['rating'].mean():.2f} ⭐")

with col3:
    neg_pct = (len(df[df["rating"] <= 3]) / len(df)) * 100
    st.metric("Negative %", f"{neg_pct:.1f}%")

st.divider()

# ======================================================
# SENTIMENT BREAKDOWN
# ======================================================

st.subheader("Sentiment Breakdown")

sentiment_counts = df["sentiment"].value_counts().reset_index()
sentiment_counts.columns = ["Sentiment", "Count"]

st.bar_chart(sentiment_counts.set_index("Sentiment"))

st.divider()

# ======================================================
# TOP THEMES
# ======================================================

st.subheader("Top Themes")

themes_exploded = df.explode("themes")
theme_counts = (
    themes_exploded["themes"]
    .value_counts()
    .head(15)
    .reset_index()
)
theme_counts.columns = ["Theme", "Count"]

st.dataframe(theme_counts, use_container_width=True)

st.divider()

# ======================================================
# TOP PRODUCTS
# ======================================================

st.subheader("Top Products")

products_exploded = df.explode("products")
product_counts = (
    products_exploded["products"]
    .value_counts()
    .head(10)
    .reset_index()
)
product_counts.columns = ["Product", "Count"]

st.dataframe(product_counts, use_container_width=True)

st.divider()

st.caption("Data Source: BigQuery → raw_reviews")
