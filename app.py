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

    # --- FIX DATE ---
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
# AUTO DETECT NET COLUMNS
# ==========================================================

def detect_net_columns(df):
    net_cols = []
    for col in df.columns:
        if df[col].dtype in [np.int64, np.int32, np.int8]:
            if set(df[col].dropna().unique()).issubset({0,1}):
                net_cols.append(col)
    return net_cols

theme_cols = detect_net_columns(df_raw)

# ==========================================================
# SIDEBAR FILTERS
# ==========================================================

with st.sidebar:
    st.title("🎛️ Command Center")
    st.success(f"🟢 Live Rows: {len(df_raw):,}")

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

df = df[mask]

# ==========================================================
# DASHBOARD HEADER
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
    st.metric("Avg Rating", f"{df['rating'].mean():.2f} ⭐")

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

# ==========================================================
# ==========================================================
# DRIVERS & BARRIERS
# ==========================================================

st.markdown("---")
st.header("🚀 Drivers & 🛑 Barriers")

if not theme_cols:
    st.warning("No NET columns detected.")
else:

    drivers_df = df[df["rating"] >= 4]
    barriers_df = df[df["rating"] <= 3]

    col_d, col_b = st.columns(2)

    # ------------------- DRIVERS -------------------
    with col_d:
        st.subheader("🚀 Top Drivers")

        if drivers_df.empty:
            st.info("No positive reviews in selection.")
        else:
            base = len(drivers_df)

            counts = drivers_df[theme_cols].apply(pd.to_numeric, errors="coerce").sum()
            counts = counts.sort_values(ascending=False).head(10)

            if counts.sum() == 0:
                st.info("No driver themes detected.")
            else:
                pct = (counts / base * 100).round(1)

                plot_df = pd.DataFrame({
                    "Theme": counts.index,
                    "Pct": pct.values
                })

                fig = px.bar(
                    plot_df,
                    x="Pct",
                    y="Theme",
                    orientation="h",
                    text="Pct",
                    color_discrete_sequence=["#10b981"]
                )

                fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
                fig.update_layout(template="plotly_dark", yaxis={"categoryorder":"total ascending"})

                st.plotly_chart(fig, use_container_width=True)

    # ------------------- BARRIERS -------------------
    with col_b:
        st.subheader("🛑 Top Barriers")

        if barriers_df.empty:
            st.info("No negative reviews in selection.")
        else:
            base = len(barriers_df)

            counts = barriers_df[theme_cols].apply(pd.to_numeric, errors="coerce").sum()
            counts = counts.sort_values(ascending=False).head(10)

            if counts.sum() == 0:
                st.info("No barrier themes detected.")
            else:
                pct = (counts / base * 100).round(1)

                plot_df = pd.DataFrame({
                    "Theme": counts.index,
                    "Pct": pct.values
                })

                fig = px.bar(
                    plot_df,
                    x="Pct",
                    y="Theme",
                    orientation="h",
                    text="Pct",
                    color_discrete_sequence=["#ef4444"]
                )

                fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
                fig.update_layout(template="plotly_dark", yaxis={"categoryorder":"total ascending"})

                st.plotly_chart(fig, use_container_width=True)


# ==========================================================
# IMPACT MATRIX
# ==========================================================

st.markdown("---")
st.subheader("🎯 Strategic Impact Matrix")

impact_data = []
total_reviews = len(df)

for theme in theme_cols:
    theme_count = df[theme].sum()
    if theme_count > 0:
        avg_rating = df[df[theme] == 1]["rating"].mean()
        freq_pct = (theme_count / total_reviews) * 100

        impact_data.append({
            "Theme": theme,
            "Frequency (%)": freq_pct,
            "Avg Rating": avg_rating
        })

if impact_data:

    impact_df = pd.DataFrame(impact_data).sort_values(
        "Frequency (%)",
        ascending=False
    ).head(20)

    fig_impact = px.scatter(
        impact_df,
        x="Frequency (%)",
        y="Avg Rating",
        text="Theme",
        size="Frequency (%)",
        color="Avg Rating",
        color_continuous_scale="RdYlGn"
    )

    fig_impact.update_traces(textposition="top center")
    fig_impact.update_layout(template="plotly_dark")
    st.plotly_chart(fig_impact, use_container_width=True)

# ==========================================================
# BRAND DRIVER MATRIX
# ==========================================================

st.markdown("---")
st.subheader("🏢 Brand Driver Matrix (%)")

if theme_cols and not drivers_df.empty:

    base_counts = drivers_df.groupby("brand_name").size()

    top_themes = (
        drivers_df[theme_cols]
        .sum()
        .sort_values(ascending=False)
        .head(8)
        .index
    )

    matrix_data = []
    matrix_data.append(base_counts.to_dict())

    for theme in top_themes:
        row = {}
        for brand in sel_brands:
            brand_df = drivers_df[drivers_df["brand_name"] == brand]
            base = len(brand_df)
            if base > 0:
                row[brand] = round((brand_df[theme].sum() / base) * 100, 1)
            else:
                row[brand] = 0
        matrix_data.append(row)

    matrix_df = pd.DataFrame(
        matrix_data,
        index=["Base (N)"] + list(top_themes)
    )

    st.dataframe(
        matrix_df.style
        .format("{:.1f}", subset=pd.IndexSlice[top_themes, :])
        .format("{:.0f}", subset=pd.IndexSlice[["Base (N)"], :])
        .background_gradient(cmap="Greens", axis=None),
        use_container_width=True
    )
