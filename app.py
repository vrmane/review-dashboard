import streamlit as st
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import timedelta

# ======================================================
# PAGE
# ======================================================
st.set_page_config(layout="wide", page_title="Period Matrix")
st.title("📅 Period-Over-Period Matrix")

# ======================================================
# BIGQUERY CONNECTION
# ======================================================
@st.cache_resource
def get_client():
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(credentials=creds)

# ======================================================
# LOAD DATA
# ======================================================
@st.cache_data(ttl=600)
def load_data():

    query="""
        SELECT *
        FROM `app-review-analyzer-487309.app_reviews_ds.raw_reviews`
        WHERE DATE(date)>=DATE_SUB(CURRENT_DATE(),INTERVAL 12 MONTH)
    """

    df=get_client().query(query).to_dataframe()

    # ---- auto detect columns
    cols={c.lower():c for c in df.columns}

    def find(names):
        for n in names:
            if n.lower() in cols:
                return cols[n.lower()]
        return None

    brand=find(["app_name","brand"])
    date=find(["date","review_date"])
    rating=find(["rating","score"])

    if not brand or not date or not rating:
        st.error("Required columns missing.")
        st.stop()

    df=df.rename(columns={
        brand:"Brand",
        date:"Date",
        rating:"Rating"
    })

    df["Date"]=pd.to_datetime(df["Date"],errors="coerce",utc=True)
    df=df.dropna(subset=["Date"])

    # ---- detect theme columns (0/1)
    exclude={"Brand","Date","Rating"}
    theme_cols=[
        c for c in df.columns
        if c not in exclude
        and df[c].dropna().isin([0,1]).all()
    ]

    return df,theme_cols

df,theme_cols=load_data()

if df.empty:
    st.warning("No data found")
    st.stop()

# ======================================================
# FILTERS
# ======================================================
st.sidebar.header("Filters")

brands=sorted(df["Brand"].dropna().unique())
sel_brands=st.sidebar.multiselect("Brands",brands,brands)

grain=st.sidebar.selectbox("Time Grain",["Week","Month","Quarter","Year"],index=1)

range_sel=st.sidebar.selectbox(
    "Range",
    ["Last 7 Days","Last 30 Days","Last 90 Days","Last 6 Months","Last 12 Months","All Time"],
    index=3
)

max_date=df["Date"].max()

if range_sel=="Last 7 Days":
    start=max_date-timedelta(days=7)
elif range_sel=="Last 30 Days":
    start=max_date-timedelta(days=30)
elif range_sel=="Last 90 Days":
    start=max_date-timedelta(days=90)
elif range_sel=="Last 6 Months":
    start=max_date-timedelta(days=180)
elif range_sel=="Last 12 Months":
    start=max_date-timedelta(days=365)
else:
    start=df["Date"].min()

df=df[(df["Date"]>=start)&(df["Brand"].isin(sel_brands))]

# ======================================================
# PERIOD COLUMN
# ======================================================
if grain=="Week":
    df["Period"]=df["Date"].dt.strftime("%Y-W%V")
elif grain=="Month":
    df["Period"]=df["Date"].dt.to_period("M").astype(str)
elif grain=="Quarter":
    df["Period"]=df["Date"].dt.to_period("Q").astype(str)
else:
    df["Period"]=df["Date"].dt.to_period("Y").astype(str)

# ======================================================
# MATRIX BUILDER
# ======================================================
def build_matrix(sub_df):

    if sub_df.empty or not theme_cols:
        return None,None

    periods=sorted(sub_df["Period"].unique())

    base=sub_df.groupby(["Period","Brand"]).size().unstack(fill_value=0)

    top=sub_df[theme_cols].sum().sort_values(ascending=False).head(20).index

    rows=[]
    base_row={}

    for p in periods:
        for b in sel_brands:
            base_row[(p,b)]=base.get(b,pd.Series()).get(p,0)

    for theme in top:

        row={}

        for p in periods:
            for b in sel_brands:

                mask=(sub_df["Period"]==p)&(sub_df["Brand"]==b)

                base_val=base.get(b,pd.Series()).get(p,0)
                val=sub_df.loc[mask,theme].sum()

                row[(p,b)]=(val/base_val*100) if base_val>0 else 0

        rows.append(row)

    out=pd.DataFrame(rows,index=top)

    out.columns=pd.MultiIndex.from_tuples(out.columns)
    out=out.sort_index(axis=1)

    base_df=pd.DataFrame([base_row],index=["Base (N)"])
    base_df.columns=pd.MultiIndex.from_tuples(base_df.columns)

    base_df=base_df.reindex(columns=out.columns).fillna(0)

    return pd.concat([base_df,out]),top

# ======================================================
# HEATMAP
# ======================================================
def style(df,color):

    if df is None:
        return None

    return (
        df.style
        .background_gradient(cmap=color,subset=pd.IndexSlice[df.index[1:],:],axis=None)
        .format("{:.1f}",subset=pd.IndexSlice[df.index[1:],:])
        .format("{:.0f}",subset=pd.IndexSlice[["Base (N)"],:])
        .set_properties(
            subset=pd.IndexSlice[["Base (N)"],:],
            **{"background-color":"#fff2cc","color":"black","font-weight":"bold"}
        )
    )

# ======================================================
# DRIVERS
# ======================================================
st.subheader("⭐ Drivers (4-5★)")

drivers=df[df["Rating"]>=4]
mat,top=build_matrix(drivers)

if mat is not None:
    st.dataframe(style(mat,"Greens"),use_container_width=True)
else:
    st.info("No driver data.")

# ======================================================
# BARRIERS
# ======================================================
st.subheader("⚠️ Barriers (1-3★)")

bars=df[df["Rating"]<=3]
mat,top=build_matrix(bars)

if mat is not None:
    st.dataframe(style(mat,"Reds"),use_container_width=True)
else:
    st.info("No barrier data.")
