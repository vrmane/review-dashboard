import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
import re
import gc
from collections import Counter
from datetime import datetime
import pytz
from google.cloud import bigquery
from google.oauth2 import service_account

# =====================================================
# CONFIG
# =====================================================

PROJECT_ID="app-review-analyzer-487309"
DATASET="app_reviews_ds"
TABLE="raw_reviews"

st.set_page_config(page_title="Strategic Intelligence Platform",layout="wide")

# =====================================================
# CLIENT
# =====================================================

@st.cache_resource
def get_client():
    key="gcp_service_account" if "gcp_service_account" in st.secrets else "GCP_SERVICE_ACCOUNT"
    creds=service_account.Credentials.from_service_account_info(dict(st.secrets[key]))
    return bigquery.Client(credentials=creds,project=creds.project_id)

# =====================================================
# LOAD
# =====================================================

@st.cache_data(ttl=600)
def load():

    q=f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE DATE(date)>=DATE_SUB(CURRENT_DATE(),INTERVAL 365 DAY)
    """

    df=get_client().query(q).to_dataframe()

    if df.empty:
        return df

    df["rating"]=pd.to_numeric(df["rating"],errors="coerce")
    df["at"]=pd.to_datetime(df["date"],utc=True,errors="coerce")
    df=df.dropna(subset=["at"])
    df["at"]=df["at"].dt.tz_convert("Asia/Kolkata")

    df["Month"]=df["at"].dt.to_period("M").astype(str)
    df["Week"]=df["at"].dt.strftime("%Y-W%V")

    df["score"]=df["rating"]

    if "sentiment" not in df.columns:
        df["Sentiment_Label"]=pd.cut(df["score"],[0,2,3,5],labels=["Negative","Neutral","Positive"])
    else:
        df["Sentiment_Label"]=df["sentiment"]

    # detect theme cols
    themes=[]
    for c in df.columns:
        try:
            u=df[c].dropna().unique()
            if len(u)<=2 and set(u).issubset({0,1}):
                themes.append(c)
        except:
            pass

    st.session_state["themes"]=themes
    st.session_state["last"]=datetime.now().strftime("%H:%M:%S")
    return df

df_raw=load()
if df_raw.empty:
    st.error("No data")
    st.stop()

themes=st.session_state["themes"]

# =====================================================
# FILTERS
# =====================================================

with st.sidebar:

    st.title("Filters")

    dr=st.date_input("Date",[df_raw["at"].min().date(),df_raw["at"].max().date()])
    brands=sorted(df_raw["brand_name"].dropna().unique())
    sel_brands=st.multiselect("Brand",brands,brands)
    ratings=st.multiselect("Rating",[1,2,3,4,5],[1,2,3,4,5])

    sents=sorted(df_raw["Sentiment_Label"].dropna().unique())
    sel_sents=st.multiselect("Sentiment",sents,sents)

mask=pd.Series(True,index=df_raw.index)

if len(dr)==2:
    mask &= df_raw["at"].between(
        pd.to_datetime(dr[0]).tz_localize("Asia/Kolkata"),
        pd.to_datetime(dr[1]).tz_localize("Asia/Kolkata")
    )

mask &= df_raw["brand_name"].isin(sel_brands)
mask &= df_raw["score"].isin(ratings)
mask &= df_raw["Sentiment_Label"].isin(sel_sents)

df=df_raw[mask]

# =====================================================
# KPI STRIP
# =====================================================

st.title("Strategic Intelligence Platform")
st.caption(f"Updated → {st.session_state['last']}")

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
# TABS
# =====================================================

tabs=st.tabs([
"Overview",
"Brands",
"Triggers",
"Barriers",
"Text",
"Cohorts",
"Anomalies",
"Theme Trends",
"Brand Compare",
"Executive Summary"
])

# =====================================================
# OVERVIEW
# =====================================================

with tabs[0]:
    t=df.groupby("Month").agg(Reviews=("score","count"),Rating=("score","mean")).reset_index()
    st.plotly_chart(px.line(t,x="Month",y="Reviews"),use_container_width=True)
    st.plotly_chart(px.line(t,x="Month",y="Rating"),use_container_width=True)

# =====================================================
# BRANDS
# =====================================================

with tabs[1]:
    b=df.groupby("brand_name").agg(Reviews=("score","count"),Rating=("score","mean")).reset_index()
    st.dataframe(b,use_container_width=True)
    st.plotly_chart(px.scatter(b,x="Reviews",y="Rating",size="Reviews",hover_name="brand_name"),use_container_width=True)

# =====================================================
# TRIGGERS
# =====================================================

with tabs[2]:
    pos=df[df.score>=4]
    rows=[]
    for t in themes:
        if t in pos.columns:
            c=pos[t].sum()
            if c>0:
                rows.append([t,c/len(pos)*100])
    if rows:
        d=pd.DataFrame(rows,columns=["Theme","Pct"]).sort_values("Pct",ascending=False)
        st.dataframe(d)
        st.bar_chart(d.set_index("Theme"))

# =====================================================
# BARRIERS
# =====================================================

with tabs[3]:
    neg=df[df.score<=3]
    rows=[]
    for t in themes:
        if t in neg.columns:
            c=neg[t].sum()
            if c>0:
                rows.append([t,c/len(neg)*100])
    if rows:
        d=pd.DataFrame(rows,columns=["Theme","Pct"]).sort_values("Pct",ascending=False)
        st.dataframe(d)
        st.bar_chart(d.set_index("Theme"))

# =====================================================
# TEXT
# =====================================================

with tabs[4]:
    if "review_text" in df.columns:
        stop={"the","and","for","this","that"}
        cnt=Counter()
        for txt in df["review_text"].dropna():
            words=re.sub(r"[^a-z ]","",txt.lower()).split()
            words=[w for w in words if w not in stop and len(w)>2]
            cnt.update(words)
        w=pd.DataFrame(cnt.most_common(20),columns=["Word","Count"])
        st.bar_chart(w.set_index("Word"))

# =====================================================
# COHORTS
# =====================================================

with tabs[5]:
    cohort=df.groupby(["Month","brand_name"]).size().reset_index(name="Reviews")
    st.plotly_chart(px.line(cohort,x="Month",y="Reviews",color="brand_name"),use_container_width=True)

# =====================================================
# ANOMALIES
# =====================================================

with tabs[6]:
    d=df.groupby("Week").size().reset_index(name="Reviews")
    d["z"]= (d["Reviews"]-d["Reviews"].mean())/d["Reviews"].std()
    anomalies=d[abs(d["z"])>2]
    st.plotly_chart(px.line(d,x="Week",y="Reviews"),use_container_width=True)
    st.dataframe(anomalies)

# =====================================================
# THEME TRENDS
# =====================================================

with tabs[7]:
    if themes:
        tsel=st.selectbox("Theme",themes)
        rows=[]
        for m,g in df.groupby("Month"):
            pct=g[tsel].sum()/len(g)*100 if tsel in g else 0
            rows.append([m,pct])
        td=pd.DataFrame(rows,columns=["Month","Pct"])
        st.plotly_chart(px.line(td,x="Month",y="Pct"),use_container_width=True)

# =====================================================
# BRAND COMPARE
# =====================================================

with tabs[8]:
    b1=st.selectbox("Brand A",sel_brands)
    b2=st.selectbox("Brand B",[b for b in sel_brands if b!=b1])
    if b1 and b2:
        comp=df[df.brand_name.isin([b1,b2])].groupby("brand_name")["score"].agg(["mean","count"])
        st.dataframe(comp)

# =====================================================
# EXEC SUMMARY
# =====================================================

with tabs[9]:

    worst=df.groupby("brand_name")["score"].mean().idxmin()
    best=df.groupby("brand_name")["score"].mean().idxmax()

    st.success(f"Best performing brand → {best}")
    st.error(f"Needs attention → {worst}")

    if themes:
        impact=[]
        for t in themes:
            impact.append((t,df[t].sum()))
        worst_theme=sorted(impact,key=lambda x:x[1],reverse=True)[0][0]
        st.warning(f"Top complaint driver → {worst_theme}")
