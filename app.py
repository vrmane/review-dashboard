import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# ======================================================
# PAGE
# ======================================================
st.set_page_config(layout="wide")
st.title("📅 Period-Over-Period Matrix")

# ======================================================
# CONNECTION
# ======================================================
@st.cache_resource
def client():
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(credentials=creds)

# ======================================================
# FILTERS
# ======================================================
st.sidebar.header("Controls")

grain=st.sidebar.selectbox(
    "Time Grain",
    ["Week","Month","Quarter","Year"],
    index=1
)

range_sel=st.sidebar.selectbox(
    "Range",
    ["30D","90D","6M","12M"],
    index=2
)

# ======================================================
# DATE FILTER SQL
# ======================================================
range_sql={
    "30D":"INTERVAL 30 DAY",
    "90D":"INTERVAL 90 DAY",
    "6M":"INTERVAL 180 DAY",
    "12M":"INTERVAL 365 DAY"
}[range_sel]

period_sql={
    "Week":"FORMAT_DATE('%Y-W%V', DATE(date))",
    "Month":"FORMAT_DATE('%Y-%m', DATE(date))",
    "Quarter":"FORMAT_DATE('%Y-Q%Q', DATE(date))",
    "Year":"FORMAT_DATE('%Y', DATE(date))"
}[grain]

# ======================================================
# LOAD DATA (AGGREGATED ONLY)
# ======================================================
@st.cache_data(ttl=600)
def load():

    q=f"""
    SELECT
        brand_name AS Brand,
        {period_sql} AS Period,
        rating,
        themes
    FROM `app-review-analyzer-487309.app_reviews_ds.raw_reviews`
    WHERE DATE(date)>=DATE_SUB(CURRENT_DATE(), {range_sql})
    """

    df=client().query(q).to_dataframe()

    if df.empty:
        return df,[]

    # explode themes list safely
    df["themes"]=df["themes"].apply(lambda x:x if isinstance(x,list) else [])
    df=df.explode("themes")

    return df,sorted(df["themes"].dropna().unique())

df,theme_list=load()

if df.empty:
    st.warning("No data")
    st.stop()

# ======================================================
# BRAND FILTER
# ======================================================
brands=sorted(df["Brand"].dropna().unique())
sel_brands=st.sidebar.multiselect("Brands",brands,brands)
df=df[df["Brand"].isin(sel_brands)]

# ======================================================
# MATRIX BUILDER
# ======================================================
def matrix(sub):

    if sub.empty:
        return None

    base=sub.groupby(["Period","Brand"]).size().unstack(fill_value=0)

    top=sub["themes"].value_counts().head(20).index
    periods=sorted(sub["Period"].unique())

    rows=[]
    base_row={}

    for p in periods:
        for b in sel_brands:
            base_row[(p,b)]=base.get(b,pd.Series()).get(p,0)

    for t in top:

        row={}

        for p in periods:
            for b in sel_brands:

                base_val=base.get(b,pd.Series()).get(p,0)

                cnt=len(sub[(sub["Period"]==p)&
                            (sub["Brand"]==b)&
                            (sub["themes"]==t)])

                row[(p,b)]=(cnt/base_val*100) if base_val else 0

        rows.append(row)

    out=pd.DataFrame(rows,index=top)
    out.columns=pd.MultiIndex.from_tuples(out.columns)

    base_df=pd.DataFrame([base_row],index=["Base (N)"])
    base_df.columns=out.columns

    return pd.concat([base_df,out])

# ======================================================
# STYLE
# ======================================================
def style(df,color):

    if df is None:
        return None

    return (
        df.style
        .background_gradient(cmap=color,
            subset=pd.IndexSlice[df.index[1:],:],axis=None)
        .format("{:.1f}",subset=pd.IndexSlice[df.index[1:],:])
        .format("{:.0f}",subset=pd.IndexSlice[["Base (N)"],:])
    )

# ======================================================
# DRIVERS
# ======================================================
st.subheader("⭐ Drivers (4-5★)")

d=matrix(df[df["rating"]>=4])

if d is not None:
    st.dataframe(style(d,"Greens"),use_container_width=True)
else:
    st.info("No driver data")

# ======================================================
# BARRIERS
# ======================================================
st.subheader("⚠️ Barriers (1-3★)")

b=matrix(df[df["rating"]<=3])

if b is not None:
    st.dataframe(style(b,"Reds"),use_container_width=True)
else:
    st.info("No barrier data")
