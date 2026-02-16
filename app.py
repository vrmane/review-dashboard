import streamlit as st
import pandas as pd
from google.cloud import bigquery

PROJECT_ID = "app-review-analyzer-487309"
DATASET = "app_reviews_ds"
TABLE = "raw_reviews"

st.set_page_config(layout="wide")

@st.cache_resource
def init_bq():
    return bigquery.Client(project=PROJECT_ID)

bq = init_bq()

st.title("📊 Review Intelligence Dashboard")

st.sidebar.header("Filters")

start_date = st.sidebar.date_input("Start Date")
end_date = st.sidebar.date_input("End Date")

brands = st.sidebar.multiselect(
    "Brand",
    ["MoneyView", "Navi", "KreditBee", "EarlySalary", "Kissht"],
    default=["MoneyView"]
)

ratings = st.sidebar.multiselect(
    "Rating",
    [1,2,3,4,5],
    default=[1,2,3,4,5]
)

@st.cache_data(ttl=300)
def load_data(start_date, end_date, brands, ratings):

    query = f"""
        SELECT
            review_id,
            brand_name,
            rating,
            sentiment,
            products,
            themes,
            date
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE DATE(date) BETWEEN @start_date AND @end_date
          AND brand_name IN UNNEST(@brands)
          AND rating IN UNNEST(@ratings)
        LIMIT 100000
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            bigquery.ArrayQueryParameter("brands", "STRING", brands),
            bigquery.ArrayQueryParameter("ratings", "INT64", ratings),
        ]
    )

    return bq.query(query, job_config=job_config).to_dataframe()


if start_date and end_date and brands and ratings:

    df = load_data(start_date, end_date, brands, ratings)

    if not df.empty:
        col1, col2, col3 = st.columns(3)

        col1.metric("Total Reviews", f"{len(df):,}")
        col2.metric("Average Rating", f"{df['rating'].mean():.2f}")
        col3.metric(
            "Positive %",
            f"{(len(df[df['rating']>=4])/len(df))*100:.1f}%"
        )

        st.dataframe(df.head(200))
    else:
        st.warning("No data found.")
