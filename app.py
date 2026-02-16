import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import timedelta

# ======================================================
# PAGE CONFIG
# ======================================================
st.set_page_config(layout="wide", page_title="Period Matrix")
st.title("📅 Period-Over-Period Matrix")

# ======================================================
# BIGQUERY CLIENT
# ======================================================
@st.cache_resource
def get_client():
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(credentials=creds)

client = get_client()

# ======================================================
# SIDEBAR FILTERS
# ======================================================
st.sidebar.header("Filters")

grain = st.sidebar.selectbox(
    "Time Grain",
    ["Week", "Month", "Quarter", "Year"],
    index=1
)

range_choice = st.sidebar.selectbox(
    "Time Range",
    ["30D", "90D", "6M", "12M", "All"],
    index=2
)

# ======================================================
# SQL RANGE LOGIC
# ======================================================
range_sql_map = {
    "30D": "INTERVAL 30 DAY",
    "90D": "INTERVAL 90 DAY",
    "6M": "INTERVAL 180 DAY",
    "12M": "INTERVAL 365 DAY",
    "All": None
}

range_sql = range_sql_map[range_choice]

period_sql = {
    "Week": "FORMAT_DATE('%Y-W%V', DATE(date))",
    "Month": "FORMAT_DATE('%Y-%m', DATE(date))",
    "Quarter": "FORMAT_DATE('%Y-Q%Q', DATE(date))",
    "Year": "FORMAT_DATE('%Y', DATE(date))"
}[grain]

# ======================================================
# LOAD DATA (LIGHTWEIGHT QUERY)
# ======================================================
@st.cache_data(ttl=600)
def load_data():

    where = "" if range_sql is None else f"""
        WHERE DATE(date) >= DATE_SUB(CURRENT_DATE(), {range_sql})
    """

    query = f"""
        SELECT
            brand_name,
            rating,
            {period_sql} AS period,
            themes
        FROM `app-review-analyzer-487309.app_reviews_ds.raw_reviews`
        {where}
    """

    df = client.query(query).to_dataframe()

    if df.empty:
        return df, []

    df.rename(columns={
        "brand_name": "Brand",
        "rating": "Rating",
        "period": "Period"
    }, inplace=True)

    # ensure themes list
    df["themes"] = df["themes"].apply(
        lambda x: x if isinstance(x, list) else []
    )

    df = df.explode("themes")

    themes = sorted(df["themes"].dropna().unique())

    return df, themes

df, theme_list = load_data()

if df.empty:
    st.warning("No data returned from query.")
    st.stop()

# ======================================================
# BRAND FILTER
# ======================================================
brands = sorted(df["Brand"].dropna().unique())

selected_brands = st.sidebar.multiselect(
    "Brands",
    brands,
    default=brands
)

df = df[df["Brand"].isin(selected_brands)]

if df.empty:
    st.warning("No data after filtering.")
    st.stop()

# ======================================================
# MATRIX BUILDER (CRASH-PROOF)
# ======================================================
def build_matrix(data):

    if data.empty:
        return None

    base = data.groupby(["Period", "Brand"]).size().unstack(fill_value=0)

    if base.empty:
        return None

    top_themes = (
        data["themes"]
        .value_counts()
        .head(20)
        .index
        .tolist()
    )

    if not top_themes:
        return None

    periods = sorted(data["Period"].unique())
    brands = sorted(data["Brand"].unique())

    if not periods or not brands:
        return None

    # base row
    base_row = {}
    for p in periods:
        for b in brands:
            base_row[(p, b)] = base.get(b, pd.Series()).get(p, 0)

    # theme rows
    rows = []

    for theme in top_themes:

        row = {}

        for p in periods:
            for b in brands:

                base_val = base.get(b, pd.Series()).get(p, 0)

                cnt = len(data[
                    (data["Period"] == p) &
                    (data["Brand"] == b) &
                    (data["themes"] == theme)
                ])

                row[(p, b)] = (cnt / base_val * 100) if base_val else 0

        rows.append(row)

    if not rows:
        return None

    matrix = pd.DataFrame(rows, index=top_themes)

    if len(matrix.columns) == 0:
        return None

    matrix.columns = pd.MultiIndex.from_tuples(matrix.columns)

    base_df = pd.DataFrame([base_row], index=["Base (N)"])
    base_df.columns = matrix.columns

    return pd.concat([base_df, matrix])

# ======================================================
# STYLING
# ======================================================
def style_matrix(df, cmap):

    if df is None:
        return None

    return (
        df.style
        .background_gradient(
            cmap=cmap,
            subset=pd.IndexSlice[df.index[1:], :],
            axis=None
        )
        .format("{:.1f}", subset=pd.IndexSlice[df.index[1:], :])
        .format("{:.0f}", subset=pd.IndexSlice[["Base (N)"], :])
    )

# ======================================================
# DRIVERS
# ======================================================
st.subheader("⭐ Drivers (4-5★)")

drivers_df = df[df["Rating"] >= 4]
drivers_matrix = build_matrix(drivers_df)

if drivers_matrix is not None:
    st.dataframe(
        style_matrix(drivers_matrix, "Greens"),
        use_container_width=True
    )
else:
    st.info("No driver data for selected filters.")

# ======================================================
# BARRIERS
# ======================================================
st.subheader("⚠️ Barriers (1-3★)")

barriers_df = df[df["Rating"] <= 3]
barriers_matrix = build_matrix(barriers_df)

if barriers_matrix is not None:
    st.dataframe(
        style_matrix(barriers_matrix, "Reds"),
        use_container_width=True
    )
else:
    st.info("No barrier data for selected filters.")
