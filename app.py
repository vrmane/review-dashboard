```python
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import timedelta

# =====================================================
# PAGE CONFIG
# =====================================================

st.set_page_config(
    page_title="Strategic Intelligence Platform",
    page_icon="📊",
    layout="wide"
)

# =====================================================
# BIGQUERY CONNECTION
# =====================================================

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

# =====================================================
# LOAD DATA
# =====================================================

@st.cache_data(ttl=600)
def load_data():

    query = """
    SELECT *
    FROM `app-review-analyzer-487309.app_reviews_ds.raw_reviews`
    """

    df = bq.query(query).to_dataframe()

    # ---------- SAFETY COLUMN STANDARDIZATION ----------
    df.columns = [c.strip() for c in df.columns]

    # ---------- DATE ----------
    if "date" in df.columns:
        df["date"] = (
            pd.to_datetime(df["date"], errors="coerce", utc=True)
            .dt.tz_convert("Asia/Kolkata")
            .dt.tz_localize(None)
        )

        df["Month"] = df["date"].dt.to_period("M").astype(str)
        df["Week"] = df["date"].dt.to_period("W").astype(str)

    # ---------- RATING ----------
    if "rating" in df.columns:
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

        df["Sentiment_Label"] = pd.cut(
            df["rating"],
            bins=[0,2,3,5],
            labels=["Negative","Neutral","Positive"]
        )

    # ---------- PRODUCTS ----------
    if "products" in df.columns:
        df["products"] = df["products"].apply(
            lambda x: x if isinstance(x,list) else []
        )
    else:
        df["products"] = [[] for _ in range(len(df))]

    return df

df_raw = load_data()

if df_raw.empty:
    st.error("No data returned from BigQuery.")
    st.stop()

# =====================================================
# AUTO DETECT THEMES
# =====================================================

def detect_themes(df):

    ignore = {
        "app_id","brand_name","review_id","date","content",
        "sentiment","products","themes","rating",
        "Month","Week","Sentiment_Label"
    }

    theme_cols = []

    for col in df.columns:

        if col in ignore:
            continue

        series = pd.to_numeric(df[col], errors="coerce")
        vals = set(series.dropna().unique())

        if vals.issubset({0,1}) and len(vals)<=2:
            theme_cols.append(col)

    return theme_cols

theme_cols = detect_themes(df_raw)

# =====================================================
# SIDEBAR FILTERS
# =====================================================

with st.sidebar:

    st.title("Filters")

    # DATE RANGE
    if "date" in df_raw.columns:

        min_d = df_raw["date"].min().date()
        max_d = df_raw["date"].max().date()

        date_range = st.date_input(
            "Date Range",
            [min_d,max_d],
            min_value=min_d,
            max_value=max_d
        )
    else:
        date_range=None

    # BRAND
    if "brand_name" in df_raw.columns:
        brands = sorted(df_raw["brand_name"].dropna().unique())
        sel_brands = st.multiselect("Brands",brands,brands)
    else:
        sel_brands=[]

    # RATING
    sel_ratings = st.multiselect(
        "Ratings",
        [1,2,3,4,5],
        [1,2,3,4,5]
    ) or [1,2,3,4,5]

    # PRODUCTS
    all_products = sorted({p for sub in df_raw["products"] for p in sub})
    sel_products = st.multiselect("Products",all_products)

# =====================================================
# APPLY FILTERS
# =====================================================

df = df_raw.copy()
mask = pd.Series(True,index=df.index)

# DATE
if date_range and "date" in df.columns:
    if len(date_range)==2:
        start=pd.to_datetime(date_range[0])
        end=pd.to_datetime(date_range[1])+timedelta(days=1)
        mask &= (df["date"]>=start)&(df["date"]<end)

# BRAND
if sel_brands and "brand_name" in df.columns:
    mask &= df["brand_name"].isin(sel_brands)

# RATING
if "rating" in df.columns:
    mask &= df["rating"].isin(sel_ratings)

# PRODUCTS
if sel_products:
    mask &= df["products"].apply(lambda x:any(p in x for p in sel_products))

df = df[mask]

# =====================================================
# NAVIGATION
# =====================================================

tabs = st.tabs([
"Overview",
"Ratings",
"Trends",
"Brands",
"Products",
"Themes",
"Risk",
"Raw Data"
])

# =====================================================
# OVERVIEW
# =====================================================

with tabs[0]:

    total=len(df)

    avg=df["rating"].mean() if "rating" in df else 0
    median=df["rating"].median() if "rating" in df else 0
    std=df["rating"].std() if "rating" in df else 0
    brands=df["brand_name"].nunique() if "brand_name" in df else 0

    c1,c2,c3,c4,c5=st.columns(5)

    c1.metric("Reviews",f"{total:,}")
    c2.metric("Avg Rating",f"{avg:.2f}")
    c3.metric("Median",f"{median:.2f}")
    c4.metric("Std Dev",f"{std:.2f}")
    c5.metric("Brands",brands)

    if "Month" in df.columns:
        trend=df.groupby("Month").size().reset_index(name="Reviews")
        st.plotly_chart(px.line(trend,x="Month",y="Reviews"),use_container_width=True)

# =====================================================
# RATINGS
# =====================================================

with tabs[1]:

    if "rating" not in df.columns:
        st.info("No rating column")
    else:

        dist=df.rating.value_counts().sort_index().reset_index()
        dist.columns=["Rating","Count"]
        dist["%"]=dist["Count"]/len(df)*100

        st.plotly_chart(px.bar(dist,x="Rating",y="%"),use_container_width=True)

        if "Sentiment_Label" in df.columns:
            sent=df.Sentiment_Label.value_counts().reset_index()
            sent.columns=["Sentiment","Count"]

            st.plotly_chart(px.pie(sent,names="Sentiment",values="Count"),
                            use_container_width=True)

# =====================================================
# TRENDS
# =====================================================

with tabs[2]:

    if "Month" in df.columns and "rating" in df.columns:

        monthly=df.groupby("Month").agg(
            Reviews=("review_id","count"),
            Rating=("rating","mean")
        ).reset_index()

        st.plotly_chart(px.line(monthly,x="Month",y="Reviews"),
                        use_container_width=True)

        st.plotly_chart(px.line(monthly,x="Month",y="Rating"),
                        use_container_width=True)

# =====================================================
# BRANDS
# =====================================================

with tabs[3]:

    if "brand_name" not in df.columns:
        st.info("No brand column")
    else:

        brand=df.groupby("brand_name").agg(
            Reviews=("review_id","count"),
            Rating=("rating","mean"),
            Negative=("Sentiment_Label",
                      lambda x:(x=="Negative").mean()*100)
        ).reset_index().sort_values("Reviews",ascending=False)

        st.dataframe(brand,use_container_width=True)

# =====================================================
# PRODUCTS
# =====================================================

with tabs[4]:

    if df["products"].explode().empty:
        st.info("No product data")
    else:

        prod=df.explode("products")

        prod_stats=prod.groupby("products").agg(
            Reviews=("review_id","count"),
            Rating=("rating","mean")
        ).reset_index().sort_values("Reviews",ascending=False)

        st.plotly_chart(
            px.bar(prod_stats.head(15),
                   x="Reviews",y="products",orientation="h"),
            use_container_width=True
        )

# =====================================================
# THEMES
# =====================================================

with tabs[5]:

    if not theme_cols:
        st.warning("No theme columns detected.")
    else:

        rows=[]

        for t in theme_cols:

            series=pd.to_numeric(df[t],errors="coerce").fillna(0)

            freq=series.mean()*100
            rating=df.loc[series==1,"rating"].mean()

            rows.append([t,freq,rating])

        th=pd.DataFrame(rows,columns=["Theme","Frequency","Rating"])

        th["Impact"]=th["Frequency"]*(th["Rating"]-df["rating"].mean())

        st.plotly_chart(
            px.scatter(
                th,
                x="Frequency",
                y="Rating",
                size="Frequency",
                color="Impact",
                hover_name="Theme"
            ),
            use_container_width=True
        )

# =====================================================
# RISK
# =====================================================

with tabs[6]:

    if "rating" not in df.columns:
        st.info("No rating column")
    else:

        one_star=(df.rating==1).mean()*100
        neg=(df.rating<=2).mean()*100

        c1,c2=st.columns(2)
        c1.metric("1★ Risk %",f"{one_star:.1f}%")
        c2.metric("Negative %",f"{neg:.1f}%")

        if "Week" in df.columns:
            neg_trend=df.groupby("Week")["rating"].apply(
                lambda x:(x<=2).mean()*100
            ).reset_index()

            neg_trend.columns=["Week","Negative%"]

            st.plotly_chart(
                px.line(neg_trend,x="Week",y="Negative%"),
                use_container_width=True
            )

# =====================================================
# RAW DATA
# =====================================================

with tabs[7]:

    st.dataframe(df,use_container_width=True)

    st.download_button(
        "Download CSV",
        df.to_csv(index=False),
        file_name="filtered_reviews.csv"
    )
```
