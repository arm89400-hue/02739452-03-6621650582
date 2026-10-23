"""
แดชบอร์ดราคาการ์ด One Piece Card Game ชุด OP17 (Streamlit)
อ่านข้อมูลจาก PostgreSQL โดยตรง (ตารางที่ Airflow DAG `op17_card_prices` เขียนไว้)
"""
import json
import os

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

st.set_page_config(page_title="ราคาการ์ด OP17", page_icon="🏴‍☠️", layout="wide")

# ------------------------------------------------------------------ theme
NAVY, RED, GOLD, CREAM = "#0f1d3a", "#d62839", "#f2b630", "#fbf6ec"
SRC_COLOR = {"pricecharting": "#3a6ea5", "limitless": "#f28c28", "tcgcsv": "#2a9d6f"}
SRC_TH = {"pricecharting": "PriceCharting", "limitless": "Limitless TCG", "tcgcsv": "TCGplayer"}
VAR_TH = {"NORMAL": "ปกติ", "ALT": "อัลเทอร์เนทอาร์ต", "SUPER_ALT": "ซูเปอร์อัลเทอร์เนท", "MANGA": "มังงะ",
          "SP": "SP", "TREASURE": "เทรเชอร์", "PROMO": "โปรโม"}

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Prompt:wght@400;600;700&family=Sarabun:wght@400;600&display=swap');
html, body, [class*="css"], .stMarkdown, .stDataFrame, p, label, span, div {{ font-family: 'Sarabun', sans-serif; }}
h1, h2, h3, .hero h1 {{ font-family: 'Prompt', sans-serif !important; }}
.stApp {{ background: {CREAM}; }}
[data-testid="stSidebar"] {{ background: {NAVY}; }}
[data-testid="stSidebar"] * {{ color: #f4efe3 !important; }}
[data-testid="stSidebar"] [data-baseweb="tag"] {{ background: {RED} !important; }}
.hero {{ background: linear-gradient(120deg, {NAVY} 0%, #1d3461 60%, {RED} 140%); border-radius: 18px;
        padding: 26px 30px; color: white; margin-bottom: 18px; box-shadow: 0 8px 24px rgba(15,29,58,.25); }}
.hero h1 {{ margin: 0; font-size: 2.0rem; color: white; }}
.hero p {{ margin: 6px 0 0; color: #dfe6f5; font-size: 1.02rem; }}
.hero .badge {{ display:inline-block; background:{GOLD}; color:{NAVY}; font-weight:700; border-radius:999px;
               padding:2px 12px; margin-right:8px; font-family:'Prompt',sans-serif; }}
.kpi {{ background: white; border-radius: 14px; padding: 16px 18px; border-top: 5px solid {RED};
       box-shadow: 0 3px 12px rgba(0,0,0,.06); height: 100%; }}
.kpi .lbl {{ color: #6b6b6b; font-size: .92rem; }}
.kpi .val {{ font-family: 'Prompt', sans-serif; font-size: 1.7rem; font-weight: 700; color: {NAVY}; line-height:1.2; }}
.kpi .sub {{ color: #8a8a8a; font-size: .85rem; }}
.kpi.gold {{ border-top-color: {GOLD}; }} .kpi.navy {{ border-top-color: {NAVY}; }} .kpi.green {{ border-top-color: #2a9d6f; }}
.card {{ background: white; border-radius: 14px; padding: 8px 14px 4px; box-shadow: 0 3px 12px rgba(0,0,0,.06); }}
.stTabs [data-baseweb="tab"] {{ font-family: 'Prompt', sans-serif; font-size: 1.02rem; }}
.stTabs [aria-selected="true"] {{ color: {RED} !important; }}
.foot {{ color:#8a8a8a; font-size:.85rem; text-align:center; margin-top:22px; }}
</style>
""", unsafe_allow_html=True)


def kpi(col, label, value, sub="", tone=""):
    col.markdown(f'<div class="kpi {tone}"><div class="lbl">{label}</div><div class="val">{value}</div>'
                 f'<div class="sub">{sub}</div></div>', unsafe_allow_html=True)


def style_fig(fig, height=380):
    fig.update_layout(height=height, margin=dict(l=10, r=10, t=50, b=10), paper_bgcolor="white", plot_bgcolor="white",
                      font=dict(family="Sarabun, sans-serif", size=13), title_font=dict(family="Prompt, sans-serif", size=17),
                      legend_title_text="แหล่งข้อมูล")
    fig.for_each_trace(lambda t: t.update(name=SRC_TH.get(t.name, t.name)))
    return fig


# ------------------------------------------------------------------ data
def conn():
    return psycopg2.connect(host=os.environ.get("DATA_DB_HOST", "postgres"), port=int(os.environ.get("DATA_DB_PORT", "5432")),
                            dbname=os.environ.get("DATA_DB_NAME", "cards"), user=os.environ["DATA_DB_USER"],
                            password=os.environ["DATA_DB_PASSWORD"])


@st.cache_data(ttl=30)
def q(sql: str) -> pd.DataFrame:
    with conn() as c:
        return pd.read_sql(sql, c)


def safe(sql: str):
    try:
        return q(sql)
    except Exception:
        return None


prices = safe("SELECT * FROM card_prices")
merged = safe("SELECT * FROM cards_merged")
runs = safe("SELECT * FROM pipeline_runs ORDER BY started_at DESC NULLS LAST LIMIT 20")

st.markdown("""
<div class="hero">
  <span class="badge">OP17</span><span class="badge">3 แหล่งข้อมูล</span>
  <h1>🏴‍☠️ ราคาการ์ด One Piece Card Game</h1>
  <p>ชุด <b>OP17 "The World's Strongest Warriors"</b> · เปรียบเทียบราคาตลาดจาก PriceCharting · Limitless TCG · TCGplayer
  <br>ข้อมูลอัปเดตอัตโนมัติด้วย Apache Airflow → PostgreSQL</p>
</div>""", unsafe_allow_html=True)

if prices is None or prices.empty:
    st.info("ยังไม่มีข้อมูล กรุณารัน DAG `op17_card_prices` ใน Airflow (http://localhost:8080) ก่อน")
    st.stop()

prices["price_usd"] = prices["price_usd"].astype(float)
prices["price_eur"] = prices["price_eur"].astype(float)
prices["แหล่ง"] = prices["source"].map(SRC_TH).fillna(prices["source"])
prices["เวอร์ชัน"] = prices["variant"].map(VAR_TH).fillna(prices["variant"])

# ------------------------------------------------------------------ sidebar
with st.sidebar:
    st.markdown("## 🔎 ตัวกรอง")
    src_opts = sorted(prices["source"].unique())
    f_src = st.multiselect("แหล่งข้อมูล", src_opts, default=src_opts, format_func=lambda s: SRC_TH.get(s, s))
    var_opts = sorted(prices["variant"].unique())
    f_var = st.multiselect("เวอร์ชันการ์ด", var_opts, default=var_opts, format_func=lambda v: VAR_TH.get(v, v))
    pmax = float(prices["price_usd"].max()) if prices["price_usd"].notna().any() else 1.0
    f_price = st.slider("ช่วงราคา (USD)", 0.0, max(pmax, 1.0), (0.0, max(pmax, 1.0)))
    f_text = st.text_input("ค้นหาชื่อการ์ด / เลขการ์ด", placeholder="เช่น Luffy หรือ OP17-001")
    st.markdown("---")
    if st.button("🔄 โหลดข้อมูลใหม่"):
        st.cache_data.clear()
        st.rerun()
    last = prices["updated_at"].max()
    st.caption(f"อัปเดตล่าสุด: {pd.to_datetime(last).tz_convert('Asia/Bangkok'):%d/%m/%Y %H:%M} น.")

df = prices[prices["source"].isin(f_src) & prices["variant"].isin(f_var)]
df = df[df["price_usd"].isna() | df["price_usd"].between(*f_price)]
if f_text:
    df = df[df["card_name"].str.contains(f_text, case=False, na=False) | df["card_number"].str.contains(f_text, case=False, na=False)]

tab1, tab2, tab3 = st.tabs(["📊 ภาพรวม", "⚖️ เปรียบเทียบราคาข้ามแหล่ง", "⚙️ สถานะ Pipeline"])

# ------------------------------------------------------------------ tab 1
with tab1:
    c1, c2, c3, c4, c5 = st.columns(5)
    kpi(c1, "จำนวนรายการราคา", f"{len(df):,}", "แถวในตาราง card_prices")
    kpi(c2, "การ์ดไม่ซ้ำ (หลังรวมข้ามแหล่ง)", f"{len(merged):,}" if merged is not None else "–", "ตาราง cards_merged", "navy")
    kpi(c3, "แหล่งข้อมูลที่มีข้อมูล", f"{df['source'].nunique()} / 3", " · ".join(SRC_TH[s] for s in sorted(df['source'].unique()) if s in SRC_TH), "green")
    kpi(c4, "ราคาเฉลี่ย", f"${df['price_usd'].mean():,.2f}" if df["price_usd"].notna().any() else "–",
        f"มัธยฐาน ${df['price_usd'].median():,.2f}" if df["price_usd"].notna().any() else "", "gold")
    if df["price_usd"].notna().any():
        top = df.loc[df["price_usd"].idxmax()]
        kpi(c5, "การ์ดแพงที่สุด", f"${top['price_usd']:,.0f}", f"{top['card_name']} {top['card_number']}")
    st.write("")

    g1, g2 = st.columns([3, 2])
    with g1:
        top10 = df.dropna(subset=["price_usd"]).sort_values("price_usd", ascending=False).head(10).copy()
        top10["ป้าย"] = top10["card_name"] + " · " + top10["card_number"] + " (" + top10["เวอร์ชัน"] + ")"
        fig = px.bar(top10.iloc[::-1], x="price_usd", y="ป้าย", color="source", orientation="h",
                     title="🏆 10 อันดับการ์ดราคาสูงสุด (USD)", color_discrete_map=SRC_COLOR,
                     labels={"price_usd": "ราคา (USD)", "ป้าย": ""}, text="price_usd")
        fig.update_traces(texttemplate="$%{text:,.0f}", textposition="outside", cliponaxis=False)
        st.plotly_chart(style_fig(fig, 430), config={"displayModeBar": False})
    with g2:
        cnt = df.groupby("เวอร์ชัน", as_index=False).size()
        fig2 = px.pie(cnt, names="เวอร์ชัน", values="size", hole=.55, title="🃏 สัดส่วนรายการตามเวอร์ชันการ์ด",
                      color_discrete_sequence=[RED, NAVY, GOLD, "#2a9d6f", "#3a6ea5", "#9b5de5", "#888"])
        fig2.update_traces(textinfo="percent+label")
        st.plotly_chart(style_fig(fig2, 430), config={"displayModeBar": False})

    g3, g4 = st.columns(2)
    with g3:
        by_var = df.groupby(["เวอร์ชัน", "source"], as_index=False)["price_usd"].median()
        fig3 = px.bar(by_var, x="เวอร์ชัน", y="price_usd", color="source", barmode="group", color_discrete_map=SRC_COLOR,
                      title="💰 ราคามัธยฐานแยกตามเวอร์ชัน", labels={"price_usd": "USD", "เวอร์ชัน": ""})
        st.plotly_chart(style_fig(fig3), config={"displayModeBar": False})
    with g4:
        h = df.dropna(subset=["price_usd"])
        fig4 = px.histogram(h, x="price_usd", color="source", nbins=40, log_y=True, color_discrete_map=SRC_COLOR,
                            title="📈 การกระจายของราคา (แกน Y แบบ log)", labels={"price_usd": "ราคา (USD)"})
        fig4.update_layout(yaxis_title="จำนวนการ์ด", barmode="overlay")
        fig4.update_traces(opacity=.75)
        st.plotly_chart(style_fig(fig4), config={"displayModeBar": False})

    st.markdown("### 📋 ตารางราคาทั้งหมด")
    show = df.sort_values(["card_number", "variant", "source"])[
        ["card_number", "card_name", "เวอร์ชัน", "rarity", "แหล่ง", "price_usd", "price_eur", "psa10_usd", "url"]]
    st.dataframe(show, hide_index=True, width="stretch", height=420, column_config={
        "card_number": "เลขการ์ด", "card_name": "ชื่อการ์ด", "rarity": "ความหายาก",
        "price_usd": st.column_config.NumberColumn("ราคา (USD)", format="$%.2f"),
        "price_eur": st.column_config.NumberColumn("ราคา (EUR)", format="€%.2f"),
        "psa10_usd": st.column_config.NumberColumn("PSA 10 (USD)", format="$%.2f"),
        "url": st.column_config.LinkColumn("ลิงก์", display_text="เปิด ↗")})

# ------------------------------------------------------------------ tab 2
with tab2:
    if merged is None or merged.empty:
        st.info("ยังไม่มีตาราง cards_merged")
    else:
        m = merged.copy()
        for col in ["pricecharting_usd", "limitless_usd", "tcgplayer_usd", "avg_usd", "min_usd", "max_usd", "psa10_usd"]:
            m[col] = m[col].astype(float)
        m = m[m["variant"].isin(f_var)]
        if f_text:
            m = m[m["card_name"].str.contains(f_text, case=False, na=False) | m["card_number"].str.contains(f_text, case=False, na=False)]
        m["ส่วนต่าง"] = m["max_usd"] - m["min_usd"]
        m["เวอร์ชัน"] = m["variant"].map(VAR_TH).fillna(m["variant"])
        a, b, c, d = st.columns(4)
        kpi(a, "การ์ดไม่ซ้ำหลังตัดรายการซ้ำ", f"{len(m):,}", "คีย์: เลขการ์ด + เวอร์ชัน", "navy")
        kpi(b, "พบตั้งแต่ 2 แหล่งขึ้นไป", f"{int((m['n_sources'] > 1).sum()):,}", "จับคู่ข้ามแหล่งได้", "green")
        kpi(c, "พบครบทั้ง 3 แหล่ง", f"{int((m['n_sources'] == 3).sum()):,}", "", "gold")
        kpi(d, "รายการซ้ำที่ถูกตัด", f"{max(len(prices) - len(merged), 0):,}", "card_prices − cards_merged")
        st.write("")
        st.markdown("### ⚖️ การ์ดใบเดียวกัน ราคาต่างกันมากที่สุด")
        multi = m[m["n_sources"] > 1].sort_values("ส่วนต่าง", ascending=False).head(25)
        st.dataframe(multi[["card_number", "card_name", "เวอร์ชัน", "pricecharting_usd", "limitless_usd", "tcgplayer_usd",
                            "ส่วนต่าง", "n_sources"]], hide_index=True, width="stretch", column_config={
            "card_number": "เลขการ์ด", "card_name": "ชื่อการ์ด",
            "pricecharting_usd": st.column_config.NumberColumn("PriceCharting", format="$%.2f"),
            "limitless_usd": st.column_config.NumberColumn("Limitless", format="$%.2f"),
            "tcgplayer_usd": st.column_config.NumberColumn("TCGplayer", format="$%.2f"),
            "ส่วนต่าง": st.column_config.NumberColumn("ส่วนต่างสูงสุด", format="$%.2f"),
            "n_sources": "จำนวนแหล่ง"})
        if len(multi):
            long = multi.head(10).melt(id_vars=["card_number", "card_name"],
                                       value_vars=["pricecharting_usd", "limitless_usd", "tcgplayer_usd"],
                                       var_name="src", value_name="usd").dropna()
            long["source"] = long["src"].map({"pricecharting_usd": "pricecharting", "limitless_usd": "limitless", "tcgplayer_usd": "tcgcsv"})
            long["การ์ด"] = long["card_name"] + " " + long["card_number"]
            fig5 = px.bar(long, x="การ์ด", y="usd", color="source", barmode="group", color_discrete_map=SRC_COLOR,
                          title="📊 เทียบราคา 10 ใบที่ราคาต่างกันมากที่สุด", labels={"usd": "USD", "การ์ด": ""})
            st.plotly_chart(style_fig(fig5, 420), config={"displayModeBar": False})

# ------------------------------------------------------------------ tab 3
with tab3:
    if runs is None or runs.empty:
        st.info("ยังไม่มีประวัติการรัน")
    else:
        last = runs.iloc[0]
        st.markdown(f"### รอบล่าสุด: `{last['run_id']}`")
        s1, s2, s3 = st.columns(3)
        kpi(s1, "ผลตรวจคุณภาพข้อมูล", "✅ ผ่าน" if last["quality_passed"] else "❌ ไม่ผ่าน", "", "green" if last["quality_passed"] else "")
        kpi(s2, "เส้นทางที่ DAG เลือก (branch)", last["branch"] or "–", "build_merged = ผ่าน / quality_alert = ไม่ผ่าน", "navy")
        kpi(s3, "จำนวนแถวใน card_prices", f"{int(last['rows_loaded']):,}" if pd.notna(last["rows_loaded"]) else "–", "", "gold")
        st.write("")
        states = last["task_states"] if isinstance(last["task_states"], dict) else json.loads(last["task_states"] or "{}")
        icon = {"success": "🟢 สำเร็จ", "failed": "🔴 ล้มเหลว", "skipped": "⚪ ข้าม", "upstream_failed": "🟠 ต้นทางล้มเหลว"}
        l, r = st.columns(2)
        with l:
            st.markdown("#### สถานะแต่ละ task")
            st.dataframe(pd.DataFrame([{"task": k, "สถานะ": icon.get(v, f"🔵 {v}")} for k, v in states.items()]),
                         hide_index=True)
        with r:
            qd = last["quality_details"]
            if qd:
                qd = qd if isinstance(qd, dict) else json.loads(qd)
                st.markdown("#### ผลการตรวจคุณภาพข้อมูล")
                st.dataframe(pd.DataFrame([{"เงื่อนไข": k, "ผล": "✅ ผ่าน" if v else "❌ ไม่ผ่าน"} for k, v in qd.get("checks", {}).items()]),
                             hide_index=True)
        st.markdown("#### ประวัติการรัน")
        st.dataframe(runs[["run_id", "started_at", "finished_at", "quality_passed", "branch", "rows_loaded"]].rename(columns={
            "run_id": "รอบการรัน", "started_at": "เริ่ม", "finished_at": "เสร็จ", "quality_passed": "คุณภาพผ่าน",
            "branch": "branch", "rows_loaded": "จำนวนแถว"}), hide_index=True)

st.markdown('<div class="foot">02739452 แพลตฟอร์มข้อมูลขนาดใหญ่และการวิเคราะห์ · 6621650582 · ข้อมูลจากแหล่งสาธารณะ ไม่มีข้อมูลส่วนบุคคล</div>',
            unsafe_allow_html=True)
