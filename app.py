import streamlit as st
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta

# =====================================================
# PAGE CONFIG
# =====================================================
st.set_page_config(layout="wide", page_title="Monthly Brand Comparison")
st.title("📊 Brand Theme Intelligence")

# =====================================================
# BIGQUERY CLIENT
# =====================================================
@st.cache_resource
def get_client():
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(credentials=creds)

# =====================================================
# LOAD DATA
# =====================================================
@st.cache_data(ttl=600)
def load_data():

    query = """
        SELECT *
        FROM `app-review-analyzer-487309.app_reviews_ds.raw_reviews`
        WHERE DATE(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
    """

    df = get_client().query(query).to_dataframe()

    # ---------- STANDARDIZE COLUMNS ----------
    cols = {c.lower():c for c in df.columns}

    def find(names):
        for n in names:
            if n.lower() in cols:
                return cols[n.lower()]
        return None

    brand_col = find(["brand","app_name","brand_name"])
    date_col = find(["date","review_date"])
    rating_col = find(["rating","score"])

    if not brand_col or not date_col or not rating_col:
        st.error("Required columns missing.")
        st.stop()

    df = df.rename(columns={
        brand_col:"Brand",
        date_col:"Date",
        rating_col:"Rating"
    })

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True)
    df = df.dropna(subset=["Date"])

    df["Month"] = df["Date"].dt.to_period("M").astype(str)
    df["Week"] = df["Date"].dt.strftime("%Y-W%U")

    # ---------- DETECT THEMES ----------
    exclude = {"Brand","Date","Rating","Month","Week","review_id","content","sentiment","products"}
    theme_cols = [c for c in df.columns if c not in exclude and df[c].dropna().isin([0,1]).all()]

    # ---------- NET MAP ----------
    net_map={}
    for col in theme_cols:

        if col.startswith("[NET]"):
            net = col.replace("[NET]","").split("_")[0]
            theme = col.replace("[NET]","")
        else:
            net = col.split("_")[0]
            theme = col

        net_map.setdefault(net,[]).append(theme)

    return df, net_map


df, net_map = load_data()

if df.empty:
    st.warning("No data found")
    st.stop()

# =====================================================
# TIME FILTER
# =====================================================
st.sidebar.header("📅 Time Filter")

filter_type = st.sidebar.selectbox(
    "Select Range",
    ["Weekly","Monthly","Last 3 Months","Quarterly","Last 6 Months","Custom"]
)

today = df["Date"].max()

if filter_type=="Weekly":
    start = today - timedelta(days=7)

elif filter_type=="Monthly":
    start = today - timedelta(days=30)

elif filter_type=="Last 3 Months":
    start = today - timedelta(days=90)

elif filter_type=="Quarterly":
    start = today - timedelta(days=120)

elif filter_type=="Last 6 Months":
    start = today - timedelta(days=180)

else:
    start,end = st.sidebar.date_input(
        "Custom Range",
        [today - timedelta(days=30), today]
    )
    start=pd.to_datetime(start,utc=True)
    end=pd.to_datetime(end,utc=True)+timedelta(days=1)

if filter_type!="Custom":
    end=today

df = df[(df["Date"]>=start)&(df["Date"]<=end)]

# =====================================================
# HEATMAP COLOR FUNCTION
# =====================================================
def heatmap_style(df):

    vals=df.values.flatten()
    vals=vals[~pd.isna(vals)]

    if len(vals)==0:
        return df

    vmin,vmax=vals.min(),vals.max()

    def color(v):
        if pd.isna(v): return ""
        if vmax==vmin: return ""
        ratio=(v-vmin)/(vmax-vmin)
        r=int(255*(1-ratio))
        g=int(255*ratio)
        return f"background-color: rgb({r},{g},120)"

    return df.style.applymap(color).format("{:.0%}")

# =====================================================
# TABLE BUILDER
# =====================================================
def build_table(ratings):

    d=df[df["Rating"].isin(ratings)].copy()
    if d.empty:
        return pd.DataFrame()

    brands=sorted(d["Brand"].dropna().unique())

    months=sorted(d["Month"].unique())[-6:]
    base=d.groupby(["Brand","Month"]).size().unstack(fill_value=0)

    rows=[]

    for net,themes in sorted(net_map.items()):

        # ---------- NET ROW ----------
        net_subset=d[d[themes].sum(axis=1)>0]

        net_row={"Label":f"▶ {net}"}

        for m in months:
            for b in brands:
                b_base=base.get(m,pd.Series()).get(b,0) if m in base.columns else 0
                val=net_subset[(net_subset["Month"]==m)&(net_subset["Brand"]==b)].shape[0]
                net_row[f"{m}|{b}"]=val/b_base if b_base else 0

        rows.append(net_row)

        # ---------- THEMES ----------
        for t in themes:

            sub=d[d[t]==1]
            row={"Label":f"   {t}"}

            for m in months:
                for b in brands:
                    b_base=base.get(m,pd.Series()).get(b,0) if m in base.columns else 0
                    val=sub[(sub["Month"]==m)&(sub["Brand"]==b)].shape[0]
                    row[f"{m}|{b}"]=val/b_base if b_base else 0

            rows.append(row)

        rows.append({"Label":""})

    return pd.DataFrame(rows).set_index("Label")


# =====================================================
# TABS
# =====================================================
tab1,tab2=st.tabs(["⭐ Drivers (4-5★)","⚠️ Barriers (1-3★)"])

# =====================================================
# DRIVERS
# =====================================================
with tab1:

    table=build_table([4,5])

    if table.empty:
        st.info("No driver data")
    else:
        st.dataframe(heatmap_style(table),use_container_width=True)


# =====================================================
# BARRIERS
# =====================================================
with tab2:

    table=build_table([1,2,3])

    if table.empty:
        st.info("No barrier data")
    else:
        st.dataframe(heatmap_style(table),use_container_width=True)
