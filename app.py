import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import json
import re
import gc
import pytz
from collections import Counter
from datetime import timedelta, datetime
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
# BIGQUERY CONNECTION
# =====================================================

@st.cache_resource
def get_client():
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["GCP_SERVICE_ACCOUNT"]
    )
    return bigquery.Client(credentials=creds, project=creds.project_id)

# =====================================================
# DATA LOADER
# =====================================================

@st.cache_data(ttl=600)
def load_data(days=365):

    client = get_client()

    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE DATE(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
    """

    df = client.query(query).to_dataframe()

    if df.empty:
        return df

    ist = pytz.timezone("Asia/Kolkata")

    # DATE
    df["at"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["at"])
    df["at"] = df["at"].dt.tz_localize("UTC").dt.tz_convert(ist)

    df["Month"] = df["at"].dt.strftime("%Y-%m")
    df["Week"] = df["at"].dt.strftime("%Y-W%V")

    # SCORE
    df["score"] = pd.to_numeric(df["rating"], errors="coerce")

    df["Sentiment_Label"] = pd.cut(
        df["score"],
        bins=[0,2,3,5],
        labels=["Negative","Neutral","Positive"]
    )

    # TEXT
    if "review_text" in df.columns:
        df["char_count"] = df["review_text"].astype(str).str.len()
        df["length_bucket"] = np.where(df["char_count"]<=29,"Brief","Detailed")

    # AUTO THEME DETECTION
    theme_cols = []
    for col in df.columns:
        try:
            if df[col].dropna().isin([0,1]).all():
                theme_cols.append(col)
        except:
            pass

    st.session_state["theme_cols"] = theme_cols
    st.session_state["last_fetched"] = datetime.now(ist).strftime("%d %b %Y %I:%M %p IST")

    gc.collect()
    return df


df_raw = load_data()

if df_raw.empty:
    st.error("No data returned.")
    st.stop()

# =====================================================
# FILTERS
# =====================================================

with st.sidebar:

    st.title("Control Panel")

    min_d = df_raw["at"].min().date()
    max_d = df_raw["at"].max().date()

    dr = st.date_input("Date Range",[min_d,max_d])

    brands = sorted(df_raw["brand_name"].dropna().unique())
    sel_brands = st.multiselect("Brand",brands,default=brands)

    ratings = st.multiselect("Ratings",[1,2,3,4,5],[1,2,3,4,5])

# APPLY FILTER

mask = pd.Series(True,index=df_raw.index)

if len(dr)==2:
    mask &= df_raw["at"].between(
        pd.to_datetime(dr[0]).tz_localize("Asia/Kolkata"),
        pd.to_datetime(dr[1]).tz_localize("Asia/Kolkata")
    )

mask &= df_raw["brand_name"].isin(sel_brands)
mask &= df_raw["score"].isin(ratings)

df = df_raw[mask]

theme_cols = st.session_state.get("theme_cols",[])

# =====================================================
# KPI HEADER
# =====================================================

st.title("Strategic Intelligence Platform")

last = st.session_state.get("last_fetched","now")
st.caption(f"Last refresh → {last}")

c1,c2,c3,c4 = st.columns(4)

vol = len(df)
avg = df["score"].mean()

prom = len(df[df.score==5])
det = len(df[df.score<=3])
nps = ((prom-det)/vol*100) if vol else 0

risk = len(df[df.score==1])/vol*100 if vol else 0

c1.metric("Volume",vol)
c2.metric("Avg Rating",f"{avg:.2f}")
c3.metric("NPS Proxy",f"{nps:.0f}")
c4.metric("Critical Risk %",f"{risk:.1f}")

# =====================================================
# BRAND TABLE
# =====================================================

st.subheader("Brand Performance")

brand = df.groupby("brand_name").agg(
    Reviews=("score","count"),
    Rating=("score","mean")
).reset_index()

st.dataframe(brand,use_container_width=True)

fig = px.scatter(
    brand,
    x="Reviews",
    y="Rating",
    size="Reviews",
    hover_name="brand_name"
)
st.plotly_chart(fig,use_container_width=True)

# =====================================================
# THEME IMPACT
# =====================================================

st.subheader("Theme Impact")

if theme_cols:

    data=[]
    total=len(df)

    for t in theme_cols:
        count=df[t].sum()
        if count>0:
            avg=df.loc[df[t]==1,"score"].mean()
            data.append([t,count/total*100,avg,count])

    imp=pd.DataFrame(data,columns=["Theme","Freq","Avg","Count"])

    fig=px.scatter(
        imp,
        x="Freq",
        y="Avg",
        size="Count",
        text="Theme",
        color="Avg"
    )
    st.plotly_chart(fig,use_container_width=True)

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

    stop=set(["the","and","for","this","that","app","loan","good","bad"])

    counter=Counter()

    for txt in df["review_text"].dropna():
        words=re.sub(r"[^a-z ]","",txt.lower()).split()
        words=[w for w in words if w not in stop and len(w)>2]
        counter.update(words)

    words=pd.DataFrame(counter.most_common(20),columns=["Word","Count"])

    fig=px.bar(words,x="Count",y="Word",orientation="h")
    st.plotly_chart(fig,use_container_width=True)

# =====================================================
# RAW
# =====================================================

st.subheader("Raw Data")

st.dataframe(df,use_container_width=True)

csv=df.to_csv(index=False).encode("utf-8")
st.download_button("Download CSV",csv,"data.csv","text/csv")
