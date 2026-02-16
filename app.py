import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
import re
import gc
import pytz
from datetime import datetime
from collections import Counter
from google.cloud import bigquery
from google.oauth2 import service_account

# =====================================================
# CONFIG
# =====================================================

PROJECT_ID = "app-review-analyzer-487309"
DATASET = "app_reviews_ds"
TABLE = "raw_reviews"

st.set_page_config(page_title="Strategic Intelligence Platform", layout="wide")

# =====================================================
# AUTH CLIENT
# =====================================================

@st.cache_resource
def get_client():

    if not st.secrets:
        st.error("No secrets found.")
        st.stop()

    key = None
    if "gcp_service_account" in st.secrets:
        key = "gcp_service_account"
    elif "GCP_SERVICE_ACCOUNT" in st.secrets:
        key = "GCP_SERVICE_ACCOUNT"
    else:
        st.error("Missing GCP credential block.")
        st.write("Available keys:", list(st.secrets.keys()))
        st.stop()

    creds = service_account.Credentials.from_service_account_info(
        dict(st.secrets[key])
    )

    return bigquery.Client(credentials=creds, project=creds.project_id)

# =====================================================
# SCHEMA GUARD
# =====================================================

REQUIRED_COLUMNS = {
    "date": "datetime",
    "rating": "numeric",
    "brand_name": "string"
}

OPTIONAL_COLUMNS = [
    "review_text",
    "sentiment"
]

def validate_schema(df):

    issues = []

    # missing required
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            issues.append(f"Missing column → {col}")
            df[col] = None

    # enforce types
    if "rating" in df.columns:
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    return df, issues

# =====================================================
# DATA LOADER
# =====================================================

@st.cache_data(ttl=600)
def load_data():

    client = get_client()

    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE DATE(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
    """

    df = client.query(query).to_dataframe()

    if df.empty:
        return df, ["No rows returned"]

    df, issues = validate_schema(df)

    # timezone safe parsing
    df["at"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    df = df.dropna(subset=["at"])
    df["at"] = df["at"].dt.tz_convert("Asia/Kolkata")

    df["Month"] = df["at"].dt.strftime("%Y-%m")
    df["Week"] = df["at"].dt.strftime("%Y-W%V")

    # score
    df["score"] = df["rating"]

    # sentiment derive fallback
    if "sentiment" not in df.columns:
        df["Sentiment_Label"] = pd.cut(
            df["score"],
            bins=[0,2,3,5],
            labels=["Negative","Neutral","Positive"]
        )
    else:
        df["Sentiment_Label"] = df["sentiment"]

    # text metrics
    if "review_text" in df.columns:
        df["char_count"] = df["review_text"].astype(str).str.len()

    # theme detection
    theme_cols=[]
    for col in df.columns:
        try:
            vals=df[col].dropna().unique()
            if len(vals)<=2 and set(vals).issubset({0,1}):
                theme_cols.append(col)
        except:
            pass

    st.session_state["theme_cols"]=theme_cols
    st.session_state["health_issues"]=issues
    st.session_state["last_refresh"]=datetime.now().strftime("%H:%M:%S")

    gc.collect()
    return df, issues


df_raw, issues = load_data()

if df_raw.empty:
    st.error("Dataset empty.")
    st.stop()

# =====================================================
# HEALTH PANEL
# =====================================================

with st.expander("System Diagnostics", expanded=False):

    st.write("Rows:", len(df_raw))
    st.write("Columns:", len(df_raw.columns))
    st.write("Detected themes:", len(st.session_state["theme_cols"]))

    if issues:
        st.warning("Schema issues detected")
        for i in issues:
            st.write("-", i)
    else:
        st.success("Schema OK")

# =====================================================
# FILTERS
# =====================================================

with st.sidebar:

    st.title("Filters")

    min_d=df_raw["at"].min().date()
    max_d=df_raw["at"].max().date()

    dr=st.date_input("Date",[min_d,max_d])

    brands=sorted(df_raw["brand_name"].dropna().unique())
    sel_brands=st.multiselect("Brand",brands,brands)

    ratings=st.multiselect("Rating",[1,2,3,4,5],[1,2,3,4,5])

mask=pd.Series(True,index=df_raw.index)

if len(dr)==2:
    mask &= df_raw["at"].between(
        pd.to_datetime(dr[0]).tz_localize("Asia/Kolkata"),
        pd.to_datetime(dr[1]).tz_localize("Asia/Kolkata")
    )

mask &= df_raw["brand_name"].isin(sel_brands)
mask &= df_raw["score"].isin(ratings)

df=df_raw[mask]

theme_cols=st.session_state["theme_cols"]

# =====================================================
# KPI STRIP
# =====================================================

st.title("Strategic Intelligence Platform")

c1,c2,c3,c4=st.columns(4)

vol=len(df)
avg=df["score"].mean()

prom=len(df[df.score==5])
det=len(df[df.score<=3])
nps=((prom-det)/vol*100) if vol else 0

risk=len(df[df.score==1])/vol*100 if vol else 0

c1.metric("Volume",vol)
c2.metric("Avg Rating",round(avg,2))
c3.metric("NPS Proxy",round(nps))
c4.metric("Critical Risk %",round(risk,1))

# =====================================================
# BRAND PERFORMANCE
# =====================================================

st.subheader("Brand Positioning")

brand=df.groupby("brand_name").agg(
    Reviews=("score","count"),
    Rating=("score","mean")
).reset_index()

st.dataframe(brand,use_container_width=True)

st.plotly_chart(
    px.scatter(
        brand,
        x="Reviews",
        y="Rating",
        size="Reviews",
        hover_name="brand_name"
    ),
    use_container_width=True
)

# =====================================================
# THEME IMPACT
# =====================================================

st.subheader("Theme Impact Matrix")

if theme_cols:

    rows=[]
    total=len(df)

    for t in theme_cols:
        count=df[t].sum()
        if count>0:
            avg=df.loc[df[t]==1,"score"].mean()
            rows.append([t,count/total*100,avg,count])

    if rows:
        imp=pd.DataFrame(rows,columns=["Theme","Freq","Avg","Count"])

        st.plotly_chart(
            px.scatter(
                imp,
                x="Freq",
                y="Avg",
                size="Count",
                text="Theme",
                color="Avg"
            ),
            use_container_width=True
        )
    else:
        st.info("No theme signals detected.")

# =====================================================
# TREND
# =====================================================

st.subheader("Trend")

trend=df.groupby("Month").agg(
    Reviews=("score","count"),
    Rating=("score","mean")
).reset_index()

st.plotly_chart(px.line(trend,x="Month",y="Reviews"),use_container_width=True)
st.plotly_chart(px.line(trend,x="Month",y="Rating"),use_container_width=True)

# =====================================================
# TEXT ANALYTICS
# =====================================================

if "review_text" in df.columns:

    st.subheader("Top Words")

    stop={"the","and","for","this","that","app","loan"}

    counter=Counter()

    for txt in df["review_text"].dropna():
        words=re.sub(r"[^a-z ]","",txt.lower()).split()
        words=[w for w in words if w not in stop and len(w)>2]
        counter.update(words)

    words=pd.DataFrame(counter.most_common(20),columns=["Word","Count"])

    st.plotly_chart(px.bar(words,x="Count",y="Word",orientation="h"),use_container_width=True)

# =====================================================
# RAW DATA
# =====================================================

st.subheader("Raw Data")

st.dataframe(df,use_container_width=True)

st.download_button(
    "Download CSV",
    df.to_csv(index=False).encode("utf-8"),
    "data.csv",
    "text/csv"
)
