import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# =========================================
# CONFIG
# =========================================

PROJECT_ID = "app-review-analyzer-487309"
DATASET = "app_reviews_ds"
TABLE = "raw_reviews"

st.set_page_config(
    page_title="App Review Intelligence",
    layout="wide"
)

# =========================================
# BIGQUERY CONNECTION
# =========================================

@st.cache_resource
def init_bq():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(
        credentials=credentials,
        project=PROJECT_ID
    )

bq = init_bq()

# =========================================
# LOAD DATA (SERVER SIDE FILTERING)
# =========================================

@st.cache_data(ttl=600)
def load_data(start_date, end_date):

    query = f"""
    SELECT
        review_id,
        brand_name,
        rating,
        sentiment,
        products,
        themes,
        DATE(date) as review_date
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE DATE(date) BETWEEN @start AND @end
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start", "DATE", start_date),
            bigquery.ScalarQueryParameter("end", "DATE", end_date),
        ]
    )

    df = bq.query(query, job_config=job_config).to_dataframe()

    # Ensure arrays are lists
    df["products"] = df["products"].apply(lambda x: x if isinstance(x, list) else [])
    df["themes"] = df["themes"].apply(lambda x: x if isinstance(x, list) else [])

    return df


# =========================================
# SIDEBAR FILTERS
# =========================================

st.sidebar.title("Filters")

min_date = pd.to_datetime("2025-12-01").date()
max_date = pd.Timestamp.today().date()

date_range = st.sidebar.date_input(
    "Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

if len(date_range) != 2:
    st.stop()

df = load_data(date_range[0], date_range[1])

if df.empty:
    st.warning("No data found.")
    st.stop()

brands = sorted(df["brand_name"].unique())
selected_brands = st.sidebar.multiselect(
    "Brand",
    brands,
    default=brands
)

df = df[df["brand_name"].isin(selected_brands)]

# =========================================
# HEADER METRICS
# =========================================

st.title("📊 App Review Intelligence Dashboard")

col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Total Reviews", f"{len(df):,}")

with col2:
    st.metric("Avg Rating", f"{df['rating'].mean():.2f}")

with col3:
    negative_pct = (len(df[df["rating"] <= 3]) / len(df)) * 100
    st.metric("Negative %", f"{negative_pct:.1f}%")

st.markdown("---")

# =========================================
# BRAND SUMMARY
# =========================================

st.subheader("Brand Performance")

summary = (
    df.groupby("brand_name")
    .agg(
        Volume=("review_id", "count"),
        Avg_Rating=("rating", "mean"),
    )
    .reset_index()
)

summary["Avg_Rating"] = summary["Avg_Rating"].round(2)

st.dataframe(summary, use_container_width=True)

# =========================================
# THEME EXPLOSION
# =========================================

st.subheader("Top Themes")

theme_df = df.explode("themes")
theme_counts = (
    theme_df.groupby("themes")
    .size()
    .reset_index(name="count")
    .sort_values("count", ascending=False)
    .head(20)
)

if not theme_counts.empty:
    st.bar_chart(
        theme_counts.set_index("themes")["count"]
    )
else:
    st.info("No themes available.")

# =========================================
# PRODUCT FILTER VIEW
# =========================================

st.subheader("Personal Loan View")

pl_df = df[df["products"].apply(
    lambda arr: any(
        p.lower() == "personal loan"
        for p in arr
    )
)]

if not pl_df.empty:
    st.metric("Personal Loan Reviews", f"{len(pl_df):,}")
else:
    st.info("No Personal Loan reviews in selected range.")
