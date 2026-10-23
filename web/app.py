"""
One Piece OP17 card-price dashboard (Streamlit).
Reads directly from PostgreSQL tables written by the Airflow DAG `op17_card_prices`.
"""
import json
import os

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

st.set_page_config(page_title="OP17 Card Prices", page_icon="🏴‍☠️", layout="wide")

SOURCE_LABEL = {"pricecharting": "PriceCharting", "limitless": "Limitless TCG", "tcgcsv": "TCGplayer (TCGCSV)"}


def conn():
    return psycopg2.connect(
        host=os.environ.get("DATA_DB_HOST", "postgres"),
        port=int(os.environ.get("DATA_DB_PORT", "5432")),
        dbname=os.environ.get("DATA_DB_NAME", "cards"),
        user=os.environ["DATA_DB_USER"],
        password=os.environ["DATA_DB_PASSWORD"],
    )


@st.cache_data(ttl=30)
def q(sql: str) -> pd.DataFrame:
    with conn() as c:
        return pd.read_sql(sql, c)


def safe(sql: str) -> pd.DataFrame | None:
    try:
        return q(sql)
    except Exception as e:  # tables not created yet
        st.info(f"No data yet. Run the Airflow DAG `op17_card_prices` first. ({type(e).__name__})")
        return None


st.title("🏴‍☠️ One Piece Card Game: OP17 The World's Strongest Warriors")
st.caption("Price comparison from PriceCharting · Limitless TCG · TCGplayer (via TCGCSV). Data: PostgreSQL ← Apache Airflow.")

tab_dash, tab_compare, tab_status = st.tabs(["📊 Dashboard", "🔀 Compare sources", "⚙️ Pipeline status"])

prices = safe("SELECT * FROM card_prices")
merged = safe("SELECT * FROM cards_merged")

# ------------------------------------------------------------------ sidebar filters
with st.sidebar:
    st.header("Filters")
    if prices is not None and len(prices):
        src_opts = sorted(prices["source"].unique())
        f_src = st.multiselect("Source", src_opts, default=src_opts, format_func=lambda s: SOURCE_LABEL.get(s, s))
        var_opts = sorted(prices["variant"].unique())
        f_var = st.multiselect("Version", var_opts, default=var_opts)
        maxp = float(prices["price_usd"].max() or 0) if prices["price_usd"].notna().any() else 0.0
        f_price = st.slider("Price (USD)", 0.0, max(maxp, 1.0), (0.0, max(maxp, 1.0)))
        f_text = st.text_input("Search name / card no.")
    else:
        f_src = f_var = []
        f_price = (0.0, 0.0)
        f_text = ""
    if st.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.rerun()

# ------------------------------------------------------------------ dashboard tab
with tab_dash:
    if prices is not None and len(prices):
        df = prices[prices["source"].isin(f_src) & prices["variant"].isin(f_var)].copy()
        df["price_usd"] = df["price_usd"].astype(float)
        df = df[df["price_usd"].isna() | df["price_usd"].between(*f_price)]
        if f_text:
            m = df["card_name"].str.contains(f_text, case=False, na=False) | df["card_number"].str.contains(f_text, case=False, na=False)
            df = df[m]

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Price records", f"{len(df):,}")
        k2.metric("Unique cards (merged)", f"{len(merged):,}" if merged is not None else "–")
        k3.metric("Sources", df["source"].nunique())
        k4.metric("Avg price (USD)", f"${df['price_usd'].mean():,.2f}" if df["price_usd"].notna().any() else "–")
        if df["price_usd"].notna().any():
            top = df.loc[df["price_usd"].idxmax()]
            k5.metric("Most expensive", f"${top['price_usd']:,.0f}", f"{top['card_name']} {top['card_number']}", delta_color="off")

        c1, c2 = st.columns(2)
        with c1:
            top10 = df.dropna(subset=["price_usd"]).sort_values("price_usd", ascending=False).head(10)
            top10["label"] = top10["card_name"] + " " + top10["card_number"] + " (" + top10["variant"] + ")"
            fig = px.bar(top10.iloc[::-1], x="price_usd", y="label", color="source", orientation="h",
                         title="Top 10 most expensive cards (USD)", labels={"price_usd": "USD", "label": ""},
                         color_discrete_map={"pricecharting": "#2a6fdb", "limitless": "#e07a1f", "tcgcsv": "#2e9e6a"})
            st.plotly_chart(fig, width="stretch")
        with c2:
            by_var = df.groupby(["variant", "source"], as_index=False)["price_usd"].median()
            fig2 = px.bar(by_var, x="variant", y="price_usd", color="source", barmode="group",
                          title="Median price by card version", labels={"price_usd": "Median USD", "variant": ""},
                          color_discrete_map={"pricecharting": "#2a6fdb", "limitless": "#e07a1f", "tcgcsv": "#2e9e6a"})
            st.plotly_chart(fig2, width="stretch")

        st.subheader("Price table")
        show = df[["card_number", "card_name", "variant", "rarity", "source", "price_usd", "price_eur",
                   "grade9_usd", "psa10_usd", "updated_at", "url"]].sort_values(["card_number", "variant", "source"])
        st.dataframe(show, width="stretch", hide_index=True,
                     column_config={"url": st.column_config.LinkColumn("link"),
                                    "price_usd": st.column_config.NumberColumn("USD", format="$%.2f"),
                                    "price_eur": st.column_config.NumberColumn("EUR", format="€%.2f")})

# ------------------------------------------------------------------ compare tab
with tab_compare:
    if merged is not None and len(merged):
        m = merged.copy()
        for col in ["pricecharting_usd", "limitless_usd", "tcgplayer_usd", "avg_usd", "min_usd", "max_usd", "psa10_usd"]:
            m[col] = m[col].astype(float)
        m = m[m["variant"].isin(f_var)] if f_var else m
        a, b, c = st.columns(3)
        a.metric("Unique cards after cross-source de-dup", len(m))
        b.metric("Cards matched in 2+ sources", int((m["n_sources"] > 1).sum()))
        c.metric("Cards in all 3 sources", int((m["n_sources"] == 3).sum()))
        m["spread_usd"] = m["max_usd"] - m["min_usd"]
        st.subheader("Same card, different price: biggest gaps between sources")
        st.dataframe(m.sort_values("spread_usd", ascending=False).head(25)[
            ["card_number", "card_name", "variant", "pricecharting_usd", "limitless_usd", "tcgplayer_usd",
             "spread_usd", "psa10_usd", "n_sources"]], width="stretch", hide_index=True)
        cnt = m["n_sources"].value_counts().rename_axis("found in N sources").reset_index(name="cards")
        st.plotly_chart(px.pie(cnt, names="found in N sources", values="cards", title="How many sources list each card"),
                        width="stretch")

# ------------------------------------------------------------------ status tab (bonus)
with tab_status:
    runs = safe("SELECT * FROM pipeline_runs ORDER BY started_at DESC NULLS LAST LIMIT 20")
    if runs is not None and len(runs):
        last = runs.iloc[0]
        st.subheader(f"Latest run: `{last['run_id']}`")
        s1, s2, s3 = st.columns(3)
        s1.metric("Quality check", "✅ passed" if last["quality_passed"] else "❌ failed")
        s2.metric("Branch taken", last["branch"] or "–")
        s3.metric("Rows in card_prices", int(last["rows_loaded"]) if pd.notna(last["rows_loaded"]) else "–")
        states = last["task_states"] if isinstance(last["task_states"], dict) else json.loads(last["task_states"] or "{}")
        icon = {"success": "🟢", "failed": "🔴", "skipped": "⚪", "upstream_failed": "🟠"}
        st.table(pd.DataFrame([{"task": k, "state": f"{icon.get(v, '🔵')} {v}"} for k, v in states.items()]))
        qd = last["quality_details"]
        if qd:
            qd = qd if isinstance(qd, dict) else json.loads(qd)
            st.write("**Quality checks**")
            st.table(pd.DataFrame([{"check": k, "result": "✅" if v else "❌"} for k, v in qd.get("checks", {}).items()]))
        st.subheader("Run history")
        st.dataframe(runs[["run_id", "started_at", "finished_at", "quality_passed", "branch", "rows_loaded"]],
                     width="stretch", hide_index=True)
