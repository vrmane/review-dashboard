import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# =====================================================
# CONFIG
# =====================================================

PROJECT="app-review-analyzer-487309"
DATASET="app_reviews_ds"
TABLE="raw_reviews"

st.set_page_config(layout="wide")

# =====================================================
# BQ CLIENT
# =====================================================

@st.cache_resource
def client():
    key="gcp_service_account" if "gcp_service_account" in st.secrets else "GCP_SERVICE_ACCOUNT"
    creds=service_account.Credentials.from_service_account_info(dict(st.secrets[key]))
    return bigquery.Client(credentials=creds,project=creds.project_id)

# =====================================================
# LOAD DATA
# =====================================================

@st.cache_data(ttl=600)
def load():

    q=f"""
    SELECT *
    FROM `{PROJECT}.{DATASET}.{TABLE}`
    WHERE DATE(date)>=DATE_SUB(CURRENT_DATE(),INTERVAL 365 DAY)
    """

    df=client().query(q).to_dataframe()

    df["rating"]=pd.to_numeric(df["rating"],errors="coerce")
    df["date"]=pd.to_datetime(df["date"],errors="coerce")
    df=df.dropna(subset=["date"])

    df["Month"]=df["date"].dt.to_period("M").astype(str)

    return df

df=load()

# =====================================================
# HELPERS
# =====================================================

def normalize_brand(x):
    s=str(x).lower()
    if "moneyview" in s: return "MoneyView"
    if "kreditbee" in s: return "KreditBee"
    if "navi" in s: return "Navi"
    if "kissht" in s: return "Kissht"
    return None

def is_pl(row):
    for c in ["Product_1","Product_2","Product_3","Product_4"]:
        if c in row and pd.notna(row[c]):
            v=str(row[c]).lower()
            if "loan" in v or "pl" in v or "personal" in v:
                return True
    return False

theme_cols=[c for c in df.columns if str(c).startswith("Theme_")]

# =====================================================
# CORE BUILDER
# =====================================================

def build_table(ratings):

    d=df[df["rating"].isin(ratings)].copy()

    d["Brand"]=d["App_Name"].apply(normalize_brand)
    d=d.dropna(subset=["Brand"])

    d=d[d.apply(is_pl,axis=1)]

    months=sorted(d["Month"].unique())[-6:]
    brands=sorted(d["Brand"].unique(),key=lambda x:(x!="MoneyView",x))

    # base
    base=d.groupby(["Brand","Month"]).size().unstack(fill_value=0)

    net_data={}

    for _,r in d.iterrows():

        nets=set()
        themes=set()

        for c in theme_cols:
            if pd.notna(r[c]) and r[c]!="":
                net=r[c]
                nets.add(net)
                themes.add((net,str(r[c]).title()))

        for net in nets:
            net_data.setdefault(net,{}).setdefault("_TOTAL",{}).setdefault(r["Brand"],{}).setdefault(r["Month"],0)
            net_data[net]["_TOTAL"][r["Brand"]][r["Month"]]+=1

        for net,theme in themes:
            net_data.setdefault(net,{}).setdefault(theme,{}).setdefault(r["Brand"],{}).setdefault(r["Month"],0)
            net_data[net][theme][r["Brand"]][r["Month"]]+=1

    # sort nets by latest MV %
    latest=months[-1] if months else None

    def mv_pct(obj):
        b=base.get(latest,{}).get("MoneyView",0) if latest in base.columns else 0
        v=obj.get("MoneyView",{}).get(latest,0)
        return v/b if b else 0

    sorted_nets=sorted(net_data.keys(),key=lambda n:mv_pct(net_data[n]["_TOTAL"]),reverse=True)

    # build table
    rows=[]

    header=["NET"]+[f"{m}-{b}" for m in months for b in brands]
    rows.append(header)

    base_row=["BASE"]
    for m in months:
        for b in brands:
            base_row.append(base.get(m,{}).get(b,0))
    rows.append(base_row)

    for net in sorted_nets:

        def make_row(label,obj):
            r=[label]
            for m in months:
                for b in brands:
                    bcount=base.get(m,{}).get(b,0)
                    v=obj.get(b,{}).get(m,0)
                    r.append(v/bcount if bcount else 0)
            return r

        rows.append(make_row(net,net_data[net]["_TOTAL"]))

        for t in sorted(net_data[net].keys()):
            if t=="_TOTAL": continue
            rows.append(make_row("   "+t,net_data[net][t]))

        rows.append([""]*len(header))

    return pd.DataFrame(rows)

# =====================================================
# UI
# =====================================================

st.title("Monthly Brand Comparison")

tab1,tab2=st.tabs(["⭐ Drivers (4-5)","⚠️ Barriers (1-3)"])

with tab1:
    st.dataframe(build_table([4,5]),use_container_width=True)

with tab2:
    st.dataframe(build_table([1,2,3]),use_container_width=True)
