"""
เว็บแดชบอร์ดราคาการ์ด One Piece Card Game ชุด OP17 (Streamlit)
- อ่านข้อมูลจาก PostgreSQL โดยตรง (ตารางที่ Airflow DAG `op17_card_prices` เขียนไว้)
- แสดงราคาเป็นเงินบาท (แปลงจาก USD ด้วยอัตราแลกเปลี่ยนล่าสุดจาก open.er-api.com, ถ้าดึงไม่ได้ใช้ค่าสำรอง)
"""
import json
import os

import pandas as pd
import plotly.express as px
import psycopg2
import requests
import streamlit as st

st.set_page_config(page_title="เช็คราคาการ์ด OP17", page_icon="🃏", layout="wide", initial_sidebar_state="collapsed")

ACCENT, INK, MUTED = "#e63946", "#1d2939", "#667085"
SRC_TH = {"pricecharting": "PriceCharting", "limitless": "Limitless TCG", "tcgcsv": "TCGplayer"}
SRC_COLOR = {"PriceCharting": "#4c6ef5", "Limitless TCG": "#f59f00", "TCGplayer": "#12b886"}
VAR_TH = {"NORMAL": "ปกติ", "ALT": "อาร์ตพิเศษ (AA)", "SUPER_ALT": "ซูเปอร์อาร์ต (SAA)", "MANGA": "มังงะ",
          "SP": "SP", "TREASURE": "เทรเชอร์", "PROMO": "โปรโม"}
FALLBACK_THB = float(os.environ.get("USD_THB_FALLBACK", "33.0"))

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Prompt:wght@500;600;700&family=Sarabun:wght@400;500;600&display=swap');
html, body, .stApp, p, label, input, textarea, button, .stMarkdown, [data-testid="stMetricLabel"] {{ font-family: 'Sarabun', sans-serif !important; }}
h1, h2, h3, h4 {{ font-family: 'Prompt', sans-serif !important; color: {INK}; }}
.stApp {{ background: #f6f7f9; }}
[data-testid="stSidebar"], [data-testid="collapsedControl"] {{ display: none; }}
.block-container {{ padding-top: 2rem; max-width: 1250px; }}
.title {{ font-family: 'Prompt', sans-serif; font-size: 2rem; font-weight: 700; color: {INK}; margin: 0; }}
.subtitle {{ color: {MUTED}; font-size: 1.05rem; margin: 2px 0 14px; }}
.pill {{ display:inline-block; background:#fff; border:1px solid #e4e7ec; border-radius:999px; padding:3px 12px;
        margin:0 6px 6px 0; font-size:.9rem; color:{INK}; }}
.pill b {{ color:{ACCENT}; }}
.kpi {{ background:#fff; border:1px solid #eaecf0; border-radius:14px; padding:16px 18px; height:100%; }}
.kpi .icon {{ font-size:1.4rem; }}
.kpi .lbl {{ color:{MUTED}; font-size:.95rem; margin-top:2px; }}
.kpi .val {{ font-family:'Prompt',sans-serif; font-size:1.65rem; font-weight:700; color:{INK}; line-height:1.25; }}
.kpi .sub {{ color:{MUTED}; font-size:.85rem; }}
div[data-testid="stTextInput"] input {{ font-size:1.1rem; padding:14px 16px; border-radius:12px; }}
.stTabs [data-baseweb="tab-list"] {{ gap: 6px; }}
.stTabs [data-baseweb="tab"] {{ font-family:'Prompt',sans-serif; font-size:1.05rem; background:#fff; border-radius:10px 10px 0 0; padding:8px 18px; }}
.stTabs [aria-selected="true"] {{ color:{ACCENT} !important; }}
.hint {{ color:{MUTED}; font-size:.9rem; }}
.foot {{ color:{MUTED}; font-size:.85rem; text-align:center; margin-top:28px; }}
</style>""", unsafe_allow_html=True)


def kpi(col, icon, label, value, sub=""):
    col.markdown(f'<div class="kpi"><div class="icon">{icon}</div><div class="lbl">{label}</div>'
                 f'<div class="val">{value}</div><div class="sub">{sub}</div></div>', unsafe_allow_html=True)


def baht(x):
    return "–" if pd.isna(x) else f"฿{x:,.0f}"


def nice(fig, h=380):
    fig.update_layout(height=h, margin=dict(l=8, r=8, t=48, b=8), paper_bgcolor="white", plot_bgcolor="white",
                      font=dict(family="Sarabun, sans-serif", size=13, color=INK),
                      title_font=dict(family="Prompt, sans-serif", size=16), legend_title_text="")
    fig.update_xaxes(gridcolor="#f0f1f3"); fig.update_yaxes(gridcolor="#f0f1f3")
    return fig


def paginate(d: pd.DataFrame, key: str, size: int = 10) -> pd.DataFrame:
    """Show one page of rows with ◀ ▶ buttons, so tables never need scrolling."""
    pages = max(1, -(-len(d) // size))
    page = min(st.session_state.get(key, 1), pages)
    a, b, c = st.columns([1, 2, 1])
    if a.button("◀ ก่อนหน้า", key=f"{key}_prev", disabled=page <= 1, width="stretch"):
        page -= 1
    if c.button("ถัดไป ▶", key=f"{key}_next", disabled=page >= pages, width="stretch"):
        page += 1
    st.session_state[key] = page
    b.markdown(f"<div style='text-align:center;padding-top:6px'>หน้า <b>{page}</b> / {pages} "
               f"<span class='hint'>(ทั้งหมด {len(d):,} รายการ)</span></div>", unsafe_allow_html=True)
    return d.iloc[(page - 1) * size: page * size]


def fit_height(n_rows: int, row: int = 35) -> int:
    return row * (n_rows + 1) + 3


# ------------------------------------------------------------------ data
@st.cache_data(ttl=3600)
def usd_thb():
    try:
        r = requests.get("https://open.er-api.com/v6/latest/USD", timeout=6)
        r.raise_for_status()
        return float(r.json()["rates"]["THB"]), "open.er-api.com (อัปเดตรายวัน)"
    except Exception:
        return FALLBACK_THB, "ค่าสำรอง (ดึงอัตราสดไม่สำเร็จ)"


def conn():
    return psycopg2.connect(host=os.environ.get("DATA_DB_HOST", "postgres"), port=int(os.environ.get("DATA_DB_PORT", "5432")),
                            dbname=os.environ.get("DATA_DB_NAME", "cards"), user=os.environ["DATA_DB_USER"],
                            password=os.environ["DATA_DB_PASSWORD"])


@st.cache_data(ttl=30)
def q(sql):
    with conn() as c:
        return pd.read_sql(sql, c)


def safe(sql):
    try:
        return q(sql)
    except Exception:
        return None


RATE, RATE_SRC = usd_thb()
prices = safe("SELECT * FROM card_prices")
merged = safe("SELECT * FROM cards_merged")
runs = safe("SELECT * FROM pipeline_runs ORDER BY started_at DESC NULLS LAST LIMIT 20")

# ------------------------------------------------------------------ header
h1, h2 = st.columns([4, 1])
with h1:
    st.markdown('<p class="title">🃏 เช็คราคาการ์ด One Piece ชุด OP17</p>'
                '<p class="subtitle">"The World\'s Strongest Warriors" · รวมราคาจาก 3 เว็บไว้ที่เดียว แสดงเป็นเงินบาท</p>',
                unsafe_allow_html=True)
with h2:
    st.write("")
    if st.button("🔄 โหลดข้อมูลใหม่", width="stretch"):
        st.cache_data.clear()
        st.rerun()

if prices is None or prices.empty:
    st.info("ยังไม่มีข้อมูล — เปิด Airflow ที่ http://localhost:8080 แล้วกดรัน DAG `op17_card_prices` ก่อน")
    st.stop()

updated = pd.to_datetime(prices["updated_at"].max()).tz_convert("Asia/Bangkok")
st.markdown(f'<span class="pill">💱 1 USD = <b>{RATE:,.2f} บาท</b> · {RATE_SRC}</span>'
            f'<span class="pill">🕒 อัปเดตข้อมูล {updated:%d/%m/%Y %H:%M} น.</span>'
            f'<span class="pill">🌐 แหล่ง: ' + " · ".join(SRC_TH[s] for s in sorted(prices["source"].unique()) if s in SRC_TH) + '</span>',
            unsafe_allow_html=True)

# ------------------------------------------------------------------ prepare
prices["price_usd"] = prices["price_usd"].astype(float)
prices["thb"] = prices["price_usd"] * RATE
prices["เว็บ"] = prices["source"].map(SRC_TH).fillna(prices["source"])
prices["แบบการ์ด"] = prices["variant"].map(VAR_TH).fillna(prices["variant"])

m = merged.copy() if merged is not None and not merged.empty else pd.DataFrame()
if not m.empty:
    for c in ["pricecharting_usd", "limitless_usd", "tcgplayer_usd", "avg_usd", "min_usd", "max_usd"]:
        m[c] = m[c].astype(float) * RATE
    m = m.rename(columns={"pricecharting_usd": "PriceCharting", "limitless_usd": "Limitless TCG", "tcgplayer_usd": "TCGplayer"})
    src_cols = ["PriceCharting", "Limitless TCG", "TCGplayer"]
    has = m[src_cols].notna().any(axis=1)
    m["ถูกสุดที่"] = m[src_cols].fillna(float("inf")).idxmin(axis=1).where(has, "–")
    m["แบบการ์ด"] = m["variant"].map(VAR_TH).fillna(m["variant"])
    tcg = prices[prices["source"] == "tcgcsv"].copy()
    tcg["pid"] = tcg["source_key"].str.split("|").str[0]
    img = (tcg.drop_duplicates(["card_number", "variant"])
              .set_index(["card_number", "variant"])["pid"]
              .map(lambda pid: f"https://tcgplayer-cdn.tcgplayer.com/product/{pid}_200w.jpg"))
    m["รูป"] = [img.get((n, v)) for n, v in zip(m["card_number"], m["variant"])]
    # fallback: Limitless card image (base art) when TCGplayer has no matching product
    lim = "https://limitlesstcg.nyc3.cdn.digitaloceanspaces.com/one-piece/{s}/{n}_EN.webp"
    m["รูป"] = m["รูป"].fillna(m["card_number"].map(lambda n: lim.format(s=n.split("-")[0], n=n)))

# ------------------------------------------------------------------ search + filters (main page, not sidebar)
s1, s2, s3 = st.columns([3, 1.4, 1.2])
with s1:
    text = st.text_input("ค้นหา", placeholder="🔍 พิมพ์ชื่อการ์ดหรือเลขการ์ด เช่น Luffy, Zoro, OP17-001", label_visibility="collapsed")
with s2:
    var_opts = ["ทุกแบบ"] + [VAR_TH.get(v, v) for v in sorted(prices["variant"].unique())]
    var_pick = st.selectbox("แบบการ์ด", var_opts, label_visibility="collapsed")
with s3:
    sort_pick = st.selectbox("เรียงตาม", ["ราคาสูง → ต่ำ", "ราคาต่ำ → สูง", "เลขการ์ด"], label_visibility="collapsed")


def apply_filters(d, name_col="card_name"):
    if text:
        d = d[d[name_col].str.contains(text, case=False, na=False) | d["card_number"].str.contains(text, case=False, na=False)]
    if var_pick != "ทุกแบบ":
        d = d[d["แบบการ์ด"] == var_pick]
    return d


tab1, tab2, tab3 = st.tabs(["🔍 ค้นหาราคา", "📊 สรุปภาพรวม", "⚙️ สถานะระบบ"])

# ------------------------------------------------------------------ tab 1: price lookup
with tab1:
    if m.empty:
        st.info("ยังไม่มีตารางรวมราคา (cards_merged)")
    else:
        view = apply_filters(m)
        sort_col, asc = {"ราคาสูง → ต่ำ": ("max_usd", False), "ราคาต่ำ → สูง": ("min_usd", True),
                         "เลขการ์ด": ("card_number", True)}[sort_pick]
        view = view.sort_values(sort_col, ascending=asc, na_position="last")

        k1, k2, k3, k4 = st.columns(4)
        kpi(k1, "🃏", "การ์ดที่พบ", f"{len(view):,} ใบ", "หลังรวมรายการซ้ำจากทุกเว็บ")
        kpi(k2, "💰", "ราคาเฉลี่ย", baht(view["avg_usd"].mean()), f"มัธยฐาน {baht(view['avg_usd'].median())}")
        if view["max_usd"].notna().any():
            t = view.loc[view["max_usd"].idxmax()]
            kpi(k3, "👑", "แพงที่สุด", baht(t["max_usd"]), f"{t['card_name']} · {t['card_number']}")
        else:
            kpi(k3, "👑", "แพงที่สุด", "–")
        kpi(k4, "🔗", "เทียบได้หลายเว็บ", f"{int((view['n_sources'] > 1).sum()):,} ใบ", "มีราคามากกว่า 1 เว็บ")
        st.write("")
        st.markdown("#### 🖼️ การ์ดเด่น (12 ใบแรกตามการเรียง)")
        g = view.head(12)
        cols = st.columns(6)
        for i, (_, r) in enumerate(g.iterrows()):
            with cols[i % 6]:
                st.image(r["รูป"], width="stretch")
                st.markdown(f"<div style='text-align:center;line-height:1.3;margin-bottom:12px'><b>{r['card_name']}</b><br>"
                            f"<span class='hint'>{r['card_number']} · {r['แบบการ์ด']}</span><br>"
                            f"<span style='color:{ACCENT};font-weight:700;font-family:Prompt'>{baht(r['avg_usd'])}</span></div>",
                            unsafe_allow_html=True)
        st.markdown("#### ราคาการ์ดแต่ละใบ (บาท)")
        st.markdown('<p class="hint">ช่องว่าง = เว็บนั้นไม่มีราคาการ์ดใบนี้ · คอลัมน์ "ถูกสุดที่" บอกว่าควรซื้อที่เว็บไหน</p>', unsafe_allow_html=True)
        cfg = {c: st.column_config.NumberColumn(c, format="฿%.0f") for c in ["PriceCharting", "Limitless TCG", "TCGplayer"]}
        cfg.update({"card_number": "เลขการ์ด", "card_name": "ชื่อการ์ด", "rarity": "ความหายาก", "n_sources": "จำนวนเว็บ",
                    "avg_usd": st.column_config.NumberColumn("ราคาเฉลี่ย", format="฿%.0f")})
        cfg["รูป"] = st.column_config.ImageColumn("รูปการ์ด", width="small", help="คลิกเพื่อขยาย")
        src_show = [c for c in ["PriceCharting", "Limitless TCG", "TCGplayer"] if view[c].notna().any()]
        page_df = paginate(view, "price_page", 10)
        st.dataframe(page_df[["รูป", "card_number", "card_name", "แบบการ์ด", "avg_usd", "ถูกสุดที่"] + src_show + ["n_sources"]],
                     hide_index=True, width="stretch", row_height=70, height=fit_height(len(page_df), 70), column_config=cfg)

# ------------------------------------------------------------------ tab 2: overview charts
with tab2:
    d = apply_filters(prices).dropna(subset=["thb"])
    c1, c2 = st.columns([3, 2])
    with c1:
        top = d.sort_values("thb", ascending=False).drop_duplicates(["card_number", "variant"]).head(10).copy()
        top["การ์ด"] = top["card_name"] + " (" + top["แบบการ์ด"] + ")"
        fig = px.bar(top.iloc[::-1], x="thb", y="การ์ด", orientation="h", text="thb", color_discrete_sequence=[ACCENT],
                     title="👑 10 การ์ดราคาสูงสุด", labels={"thb": "ราคา (บาท)", "การ์ด": ""})
        fig.update_traces(texttemplate="฿%{text:,.0f}", textposition="outside", cliponaxis=False)
        st.plotly_chart(nice(fig, 440), config={"displayModeBar": False})
    with c2:
        bins = pd.cut(d["thb"], [0, 100, 500, 1000, 5000, 20000, float("inf")],
                      labels=["< ฿100", "฿100–500", "฿500–1,000", "฿1,000–5,000", "฿5,000–20,000", "> ฿20,000"])
        bc = bins.value_counts().sort_index().reset_index()
        bc.columns = ["ช่วงราคา", "จำนวน"]
        fig2 = px.bar(bc, x="ช่วงราคา", y="จำนวน", text="จำนวน", color_discrete_sequence=["#1d2939"],
                      title="💵 การ์ดส่วนใหญ่อยู่ในช่วงราคาไหน")
        fig2.update_traces(textposition="outside", cliponaxis=False)
        st.plotly_chart(nice(fig2, 440), config={"displayModeBar": False})
    c3, c4 = st.columns(2)
    with c3:
        med = d.groupby("แบบการ์ด", as_index=False)["thb"].median().sort_values("thb")
        fig3 = px.bar(med, x="thb", y="แบบการ์ด", orientation="h", text="thb", color_discrete_sequence=["#f59f00"],
                      title="🎨 ราคากลางตามแบบการ์ด", labels={"thb": "ราคามัธยฐาน (บาท)", "แบบการ์ด": ""})
        fig3.update_traces(texttemplate="฿%{text:,.0f}", textposition="outside", cliponaxis=False)
        st.plotly_chart(nice(fig3), config={"displayModeBar": False})
    with c4:
        per = d.groupby("เว็บ", as_index=False).size().rename(columns={"size": "จำนวน"})
        fig4 = px.pie(per, names="เว็บ", values="จำนวน", hole=.55, color="เว็บ", color_discrete_map=SRC_COLOR,
                      title="🌐 จำนวนรายการราคาจากแต่ละเว็บ")
        fig4.update_traces(textinfo="label+value")
        st.plotly_chart(nice(fig4), config={"displayModeBar": False})

# ------------------------------------------------------------------ tab 3: pipeline status
with tab3:
    if runs is None or runs.empty:
        st.info("ยังไม่มีประวัติการรัน")
    else:
        last = runs.iloc[0]
        states = last["task_states"] if isinstance(last["task_states"], dict) else json.loads(last["task_states"] or "{}")
        ok = sum(v == "success" for v in states.values())
        a, b, c = st.columns(3)
        kpi(a, "✅" if last["quality_passed"] else "❌", "ตรวจคุณภาพข้อมูล", "ผ่าน" if last["quality_passed"] else "ไม่ผ่าน",
            f"เส้นทาง: {last['branch'] or '–'}")
        kpi(b, "🧩", "task ที่สำเร็จ (รอบล่าสุด)", f"{ok} / {len(states)}", str(pd.to_datetime(last['started_at']).tz_convert('Asia/Bangkok'))[:16] if pd.notna(last['started_at']) else "")
        kpi(c, "🗄️", "แถวในฐานข้อมูล", f"{int(last['rows_loaded']):,}" if pd.notna(last["rows_loaded"]) else "–", "ตาราง card_prices")
        st.write("")
        icon = {"success": "🟢 สำเร็จ", "failed": "🔴 ล้มเหลว", "skipped": "⚪ ข้าม", "upstream_failed": "🟠 ต้นทางล้มเหลว"}
        l, r = st.columns(2)
        with l:
            st.markdown("#### ขั้นตอนใน DAG")
            st.dataframe(pd.DataFrame([{"ขั้นตอน": k, "สถานะ": icon.get(v, f"🔵 {v}")} for k, v in states.items()]),
                         hide_index=True, width="stretch", height=fit_height(len(states)))
        with r:
            qd = last["quality_details"]
            if qd:
                qd = qd if isinstance(qd, dict) else json.loads(qd)
                st.markdown("#### เงื่อนไขตรวจคุณภาพ")
                st.dataframe(pd.DataFrame([{"เงื่อนไข": k, "ผล": "✅ ผ่าน" if v else "❌ ไม่ผ่าน"} for k, v in qd.get("checks", {}).items()]),
                             hide_index=True, width="stretch", height=fit_height(len(qd.get("checks", {}))))
        with st.expander("ประวัติการรันทั้งหมด"):
            hist = paginate(runs, "run_page", 5)
            st.dataframe(hist[["run_id", "started_at", "quality_passed", "branch", "rows_loaded"]].rename(columns={
                "run_id": "รอบการรัน", "started_at": "เริ่ม", "quality_passed": "คุณภาพผ่าน", "rows_loaded": "จำนวนแถว"}),
                hide_index=True, width="stretch", height=fit_height(len(hist)))

st.markdown('<div class="foot">02739452 แพลตฟอร์มข้อมูลขนาดใหญ่และการวิเคราะห์ · 6621650582 · '
            'ข้อมูลราคาจากแหล่งสาธารณะ ไม่มีข้อมูลส่วนบุคคล · ราคาเป็นบาทคำนวณจาก USD</div>', unsafe_allow_html=True)
