"""
Helper functions for the One Piece OP17 card-price pipeline.

Sources (all public, no personal data, robots.txt respected):
  1. PriceCharting  - web scraping (requests + BeautifulSoup)
  2. Limitless TCG  - web scraping (requests + BeautifulSoup)
  3. TCGCSV         - JSON API (daily mirror of TCGplayer prices)
"""
from __future__ import annotations

import glob
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# ---------------------------------------------------------------- config
SET_CODE = "OP17"
SET_NAME = "The World's Strongest Warriors"

PRICECHARTING_URL = "https://www.pricecharting.com/console/one-piece-world%27s-strongest-warriors"
LIMITLESS_SET_URL = "https://onepiece.limitlesstcg.com/cards/op17-the-worlds-strongest-warriors"
LIMITLESS_BASE = "https://onepiece.limitlesstcg.com"
TCGCSV_CATEGORY = 68      # One Piece Card Game
TCGCSV_GROUP = 24736      # OP17 The World's Strongest Warriors
TCGCSV_BASE = f"https://tcgcsv.com/tcgplayer/{TCGCSV_CATEGORY}/{TCGCSV_GROUP}"

SOURCES = ["pricecharting", "limitless", "tcgcsv"]
DATA_DIR = os.environ.get("PIPELINE_DATA_DIR", "/opt/airflow/data")
MIN_ROWS = int(os.environ.get("QUALITY_MIN_ROWS", "100"))
REQUEST_DELAY = float(os.environ.get("REQUEST_DELAY_SEC", "0.7"))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (KU-02739452 student project; educational price comparison)",
    "Accept-Language": "en-US,en;q=0.9",
}

CARD_NO_RE = re.compile(r"\b((?:OP|ST|EB|PRB)\d{2}-\d{3}|P-\d{3})\b", re.I)
MONEY_USD_RE = re.compile(r"\$\s*([\d,]+(?:\.\d+)?)")
MONEY_EUR_RE = re.compile(r"([\d.,]+)\s*€")


# ---------------------------------------------------------------- helpers
def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def to_float(text) -> float | None:
    """'$1,325.00' -> 1325.0 ; '2.28€' -> 2.28 ; '-'/'' -> None"""
    if text is None:
        return None
    if isinstance(text, (int, float)):
        return float(text)
    t = str(text).strip().replace("$", "").replace("€", "").replace(",", "").strip()
    if not t or t in {"-", "—", "N/A"}:
        return None
    try:
        return float(t)
    except ValueError:
        m = re.search(r"\d+(?:\.\d+)?", t)
        return float(m.group()) if m else None


def norm_variant(text: str | None) -> str:
    """Map each site's wording of the card version to one common code."""
    t = (text or "").lower()
    if "super" in t and ("alt" in t or "parallel" in t):
        return "SUPER_ALT"
    if "manga" in t:
        return "MANGA"
    if "treasure" in t or re.search(r"\btr\b", t):
        return "TREASURE"
    if re.search(r"\bsp\b", t) or "special card" in t:
        return "SP"
    if "alternate" in t or "alt art" in t or "parallel" in t or re.search(r"\baa\d*\b", t):
        return "ALT"
    if "jolly" in t or "pre-release" in t or "prerelease" in t or "promo" in t:
        return "PROMO"
    return "NORMAL"


def find_card_number(*texts) -> str | None:
    for t in texts:
        if not t:
            continue
        m = CARD_NO_RE.search(str(t))
        if m:
            return m.group(1).upper()
    return None


def clean_name(name: str) -> str:
    n = re.sub(r"\[.*?\]|\(.*?\)", "", name or "")
    n = CARD_NO_RE.sub("", n)
    n = re.sub(r"\s*-\s*$", "", n)
    return re.sub(r"\s+", " ", n).strip()


def run_dir(kind: str) -> str:
    d = os.path.join(DATA_DIR, kind)
    os.makedirs(d, exist_ok=True)
    return d


def raw_path(source: str, tag: str) -> str:
    return os.path.join(run_dir("raw"), f"{source}_{tag}.json")


def clean_path(source: str, tag: str) -> str:
    return os.path.join(run_dir("clean"), f"{source}_{tag}.json")


# ---------------------------------------------------------------- EXTRACT
def extract_pricecharting() -> list[dict]:
    """Scrape the set price table. Paginates with ?cursor=N until no new rows."""
    s = _session()
    rows, seen = [], set()
    for cursor in range(0, 1000, 50):
        params = {"cursor": cursor} if cursor else None
        r = s.get(PRICECHARTING_URL, params=params, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        trs = soup.select("table#games_table tr[id^=product-]") or soup.select("tr[id^=product-]")
        new = 0
        for tr in trs:
            a = tr.select_one("td.title a") or tr.select_one("a[href*='/game/']")
            if not a:
                continue
            key = tr.get("data-product") or tr.get("id", "").replace("product-", "") or a.get("href")
            if key in seen:
                continue
            seen.add(key)
            new += 1

            def cell(cls):
                td = tr.select_one(f"td.{cls}")
                return td.get_text(" ", strip=True) if td else None

            rows.append({
                "source_key": str(key),
                "name": a.get_text(" ", strip=True),
                "url": urljoin("https://www.pricecharting.com", a.get("href", "")),
                "ungraded": cell("used_price"),
                "grade9": cell("cib_price"),
                "psa10": cell("new_price"),
            })
        log.info("pricecharting cursor=%s rows=%s new=%s", cursor, len(trs), new)
        if new == 0:
            break
        time.sleep(REQUEST_DELAY)
    if not rows:
        raise RuntimeError("PriceCharting: no rows parsed (layout changed or blocked)")
    return rows


def extract_limitless() -> list[dict]:
    """Scrape the set page for card links, then each card page for USD/EUR prices."""
    s = _session()
    r = s.get(LIMITLESS_SET_URL, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    card_ids = []
    for a in soup.select("a[href*='/cards/']"):
        m = re.search(r"/cards/([A-Za-z]+\d*-\d{3})(?:$|[?#/])", a.get("href", "") + "/")
        if m and m.group(1).upper() not in card_ids:
            card_ids.append(m.group(1).upper())
    log.info("limitless: %s card pages to fetch", len(card_ids))
    if not card_ids:
        raise RuntimeError("Limitless: no card links found")

    max_cards = int(os.environ.get("LIMITLESS_MAX_CARDS", "400"))
    rows = []
    for cid in card_ids[:max_cards]:
        url = f"{LIMITLESS_BASE}/cards/{cid}"
        try:
            cr = s.get(url, timeout=30)
            cr.raise_for_status()
        except requests.RequestException as e:  # one bad page should not kill the source
            log.warning("limitless %s failed: %s", cid, e)
            continue
        cs = BeautifulSoup(cr.text, "html.parser")
        name_el = cs.select_one(".card-text-name") or cs.select_one("h1")
        name = name_el.get_text(" ", strip=True) if name_el else ""
        if not name and cs.title:
            name = cs.title.get_text().split("•")[0].split("|")[0]
        rarity_el = cs.select_one(".card-text-type, .card-text-rarity")
        rarity = rarity_el.get_text(" ", strip=True) if rarity_el else None

        # Price rows: every <tr> that mentions a $ or € amount.
        found = []
        for i, tr in enumerate(cs.select("tr")):
            txt = tr.get_text(" ", strip=True)
            usd = MONEY_USD_RE.search(txt)
            eur = MONEY_EUR_RE.search(txt)
            if not (usd or eur):
                continue
            label = MONEY_USD_RE.sub("", MONEY_EUR_RE.sub("", txt)).strip()
            found.append({
                "label": label,
                "usd": usd.group(1) if usd else None,
                "eur": eur.group(1) if eur else None,
                "row": i,
            })
        # keep prints of this set only (cards are also reprinted in promo sets)
        same_set = [f for f in found if "strongest" in f["label"].lower() or SET_CODE.lower() in f["label"].lower()]
        for f in (same_set or found[:1]):
            rows.append({
                "source_key": f"{cid}|{f['label']}",
                "card_id": cid,
                "name": name,
                "label": f["label"],
                "rarity": rarity,
                "usd": f["usd"],
                "eur": f["eur"],
                "url": url,
            })
        time.sleep(REQUEST_DELAY)
    if not rows:
        raise RuntimeError("Limitless: no prices parsed")
    return rows


def extract_tcgcsv() -> list[dict]:
    """TCGCSV JSON API: products + prices for the OP17 group, joined on productId."""
    s = _session()
    products = s.get(f"{TCGCSV_BASE}/products", timeout=30)
    products.raise_for_status()
    prices = s.get(f"{TCGCSV_BASE}/prices", timeout=30)
    prices.raise_for_status()
    price_by_pid: dict[int, list[dict]] = {}
    for p in prices.json().get("results", []):
        price_by_pid.setdefault(p["productId"], []).append(p)

    rows = []
    for prod in products.json().get("results", []):
        ext = {e.get("name"): e.get("value") for e in prod.get("extendedData", [])}
        for p in price_by_pid.get(prod["productId"], [{}]):
            rows.append({
                "source_key": f"{prod['productId']}|{p.get('subTypeName', '')}",
                "product_id": prod["productId"],
                "name": prod.get("name"),
                "number": ext.get("Number"),
                "rarity": ext.get("Rarity"),
                "sub_type": p.get("subTypeName"),
                "market": p.get("marketPrice"),
                "mid": p.get("midPrice"),
                "low": p.get("lowPrice"),
                "url": prod.get("url"),
            })
    if not rows:
        raise RuntimeError("TCGCSV: empty response")
    return rows


EXTRACTORS = {
    "pricecharting": extract_pricecharting,
    "limitless": extract_limitless,
    "tcgcsv": extract_tcgcsv,
}


def extract(source: str, tag: str, simulate_down: str = "") -> str:
    down = [x.strip().lower() for x in (simulate_down or "").split(",") if x.strip()]
    if source in down:
        raise RuntimeError(f"[simulated outage] source '{source}' is down")
    rows = EXTRACTORS[source]()
    path = raw_path(source, tag)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"source": source, "fetched_at": datetime.now(timezone.utc).isoformat(), "rows": rows}, f, ensure_ascii=False)
    log.info("%s: %s raw rows -> %s", source, len(rows), path)
    return path


# ---------------------------------------------------------------- TRANSFORM
def _t_pricecharting(r: dict) -> dict | None:
    name = r.get("name", "")
    bracket = " ".join(re.findall(r"\[(.*?)\]", name))
    num = find_card_number(name)
    if not num:  # sealed product (booster box, etc.)
        return None
    return {
        "card_number": num, "card_name": clean_name(name), "variant": norm_variant(bracket),
        "rarity": None, "price_usd": to_float(r.get("ungraded")), "price_eur": None,
        "grade9_usd": to_float(r.get("grade9")), "psa10_usd": to_float(r.get("psa10")),
        "url": r.get("url"),
    }


def _t_limitless(r: dict) -> dict | None:
    num = find_card_number(r.get("card_id"), r.get("name"))
    if not num:
        return None
    label = r.get("label", "")
    # Limitless marks versions with a short suffix after the set name (e.g. "... aa", "... sp")
    suffix = re.sub(r".*warriors", "", label, flags=re.I).strip()
    return {
        "card_number": num, "card_name": clean_name(r.get("name")), "variant": norm_variant(suffix),
        "rarity": r.get("rarity"), "price_usd": to_float(r.get("usd")), "price_eur": to_float(r.get("eur")),
        "grade9_usd": None, "psa10_usd": None, "url": r.get("url"),
    }


def _t_tcgcsv(r: dict) -> dict | None:
    num = find_card_number(r.get("number"), r.get("name"))
    if not num:
        return None
    name = r.get("name", "")
    paren = " ".join(x for x in re.findall(r"\((.*?)\)", name) if not x.strip().isdigit())
    price = r.get("market") if r.get("market") is not None else r.get("mid")
    return {
        "card_number": num, "card_name": clean_name(name), "variant": norm_variant(paren),
        "rarity": r.get("rarity"), "price_usd": to_float(price), "price_eur": None,
        "grade9_usd": None, "psa10_usd": None, "url": r.get("url"),
    }


TRANSFORMERS = {"pricecharting": _t_pricecharting, "limitless": _t_limitless, "tcgcsv": _t_tcgcsv}


def transform(source: str, tag: str) -> dict:
    """Raw file -> one common schema. Missing raw file (source down) is not an error."""
    path = raw_path(source, tag)
    if not os.path.exists(path):
        log.warning("%s: no raw file for this run (source failed) - skipping", source)
        return {"source": source, "status": "missing", "rows": 0}
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    out, seen, dropped = [], set(), 0
    for r in raw["rows"]:
        rec = TRANSFORMERS[source](r)
        if rec is None:
            dropped += 1
            continue
        key = r["source_key"]
        if key in seen:  # duplicate inside the same source
            dropped += 1
            continue
        seen.add(key)
        rec.update({"source": source, "source_key": key, "fetched_at": raw["fetched_at"]})
        out.append(rec)
    with open(clean_path(source, tag), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)
    log.info("%s: %s clean rows (%s dropped: sealed/no card number/duplicate)", source, len(out), dropped)
    return {"source": source, "status": "ok", "rows": len(out), "dropped": dropped}


# ---------------------------------------------------------------- DATABASE
DDL = """
CREATE TABLE IF NOT EXISTS card_prices (
    id           SERIAL PRIMARY KEY,
    source       TEXT NOT NULL,
    source_key   TEXT NOT NULL,
    card_number  TEXT NOT NULL,
    card_name    TEXT NOT NULL,
    variant      TEXT NOT NULL,
    rarity       TEXT,
    price_usd    NUMERIC(12,2),
    price_eur    NUMERIC(12,2),
    grade9_usd   NUMERIC(12,2),
    psa10_usd    NUMERIC(12,2),
    url          TEXT,
    fetched_at   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, source_key)
);
CREATE TABLE IF NOT EXISTS cards_merged (
    card_number        TEXT NOT NULL,
    variant            TEXT NOT NULL,
    card_name          TEXT NOT NULL,
    rarity             TEXT,
    pricecharting_usd  NUMERIC(12,2),
    limitless_usd      NUMERIC(12,2),
    tcgplayer_usd      NUMERIC(12,2),
    psa10_usd          NUMERIC(12,2),
    avg_usd            NUMERIC(12,2),
    min_usd            NUMERIC(12,2),
    max_usd            NUMERIC(12,2),
    n_sources          INT NOT NULL,
    sources            TEXT NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (card_number, variant)
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          TEXT PRIMARY KEY,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    task_states     JSONB,
    rows_loaded     INT,
    quality_passed  BOOLEAN,
    quality_details JSONB,
    branch          TEXT
);
"""


def get_conn():
    return psycopg2.connect(
        host=os.environ.get("DATA_DB_HOST", "postgres"),
        port=int(os.environ.get("DATA_DB_PORT", "5432")),
        dbname=os.environ.get("DATA_DB_NAME", "cards"),
        user=os.environ["DATA_DB_USER"],
        password=os.environ["DATA_DB_PASSWORD"],
    )


def create_tables():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(DDL)


COLS = ["source", "source_key", "card_number", "card_name", "variant", "rarity",
        "price_usd", "price_eur", "grade9_usd", "psa10_usd", "url", "fetched_at"]

UPSERT_SQL = f"""
INSERT INTO card_prices ({", ".join(COLS)}) VALUES %s
ON CONFLICT (source, source_key) DO UPDATE SET
    card_number = EXCLUDED.card_number, card_name = EXCLUDED.card_name,
    variant = EXCLUDED.variant, rarity = EXCLUDED.rarity,
    price_usd = EXCLUDED.price_usd, price_eur = EXCLUDED.price_eur,
    grade9_usd = EXCLUDED.grade9_usd, psa10_usd = EXCLUDED.psa10_usd,
    url = EXCLUDED.url, fetched_at = EXCLUDED.fetched_at, updated_at = now()
"""


def load(tag: str) -> dict:
    """UPSERT every clean file of this run. Re-running does not add rows."""
    files = sorted(glob.glob(os.path.join(run_dir("clean"), f"*_{tag}.json")))
    loaded = {}
    with get_conn() as conn, conn.cursor() as cur:
        for fp in files:
            with open(fp, encoding="utf-8") as f:
                recs = json.load(f)
            if not recs:
                continue
            values = [tuple(r.get(c) for c in COLS) for r in recs]
            psycopg2.extras.execute_values(cur, UPSERT_SQL, values, page_size=500)
            loaded[recs[0]["source"]] = len(recs)
        cur.execute("SELECT count(*) FROM card_prices")
        total = cur.fetchone()[0]
    log.info("loaded this run: %s | rows in card_prices: %s", loaded, total)
    return {"loaded": loaded, "table_rows": total}


def quality_check(tag: str) -> tuple[bool, dict]:
    """Checks on the data loaded by THIS run (sources that were down are excluded)."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*), count(DISTINCT source) FROM card_prices WHERE fetched_at >= now() - interval '6 hours'")
        recent_rows, recent_sources = cur.fetchone()
        cur.execute("SELECT count(*) FROM card_prices")
        total = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM card_prices WHERE card_number !~ '^[A-Z]+[0-9]*-[0-9]{3}$'")
        bad_numbers = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM card_prices WHERE price_usd < 0 OR price_eur < 0")
        negative = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM card_prices WHERE price_usd IS NULL AND price_eur IS NULL")
        no_price = cur.fetchone()[0]
    this_run = {}
    for fp in glob.glob(os.path.join(run_dir("clean"), f"*_{tag}.json")):
        with open(fp, encoding="utf-8") as f:
            n = len(json.load(f))
        if n:
            this_run[os.path.basename(fp).split("_")[0]] = n
    checks = {
        f"total_rows >= {MIN_ROWS}": total >= MIN_ROWS,
        "at least 1 source loaded in this run": len(this_run) >= 1,
        "card_number format valid": bad_numbers == 0,
        "no negative prices": negative == 0,
        "rows without any price < 20%": (no_price / total) < 0.2 if total else False,
    }
    details = {
        "checks": checks,
        "metrics": {"total_rows": total, "rows_this_run": this_run, "recent_rows": recent_rows, "recent_sources": recent_sources,
                    "bad_card_numbers": bad_numbers, "negative_prices": negative, "rows_without_price": no_price},
    }
    passed = all(checks.values())
    log.info("quality passed=%s details=%s", passed, details)
    return passed, details


MERGE_SQL = """
INSERT INTO cards_merged (card_number, variant, card_name, rarity, pricecharting_usd, limitless_usd,
                          tcgplayer_usd, psa10_usd, avg_usd, min_usd, max_usd, n_sources, sources, updated_at)
SELECT card_number, variant,
       (array_agg(card_name ORDER BY (source = 'tcgcsv') DESC, (source = 'limitless') DESC, length(card_name) DESC))[1],
       max(rarity),
       round(avg(price_usd) FILTER (WHERE source = 'pricecharting'), 2),
       round(avg(price_usd) FILTER (WHERE source = 'limitless'), 2),
       round(avg(price_usd) FILTER (WHERE source = 'tcgcsv'), 2),
       max(psa10_usd),
       round(avg(price_usd), 2), min(price_usd), max(price_usd),
       count(DISTINCT source),
       string_agg(DISTINCT source, ','),
       now()
FROM card_prices
GROUP BY card_number, variant
ON CONFLICT (card_number, variant) DO UPDATE SET
    card_name = EXCLUDED.card_name, rarity = EXCLUDED.rarity,
    pricecharting_usd = EXCLUDED.pricecharting_usd, limitless_usd = EXCLUDED.limitless_usd,
    tcgplayer_usd = EXCLUDED.tcgplayer_usd, psa10_usd = EXCLUDED.psa10_usd,
    avg_usd = EXCLUDED.avg_usd, min_usd = EXCLUDED.min_usd, max_usd = EXCLUDED.max_usd,
    n_sources = EXCLUDED.n_sources, sources = EXCLUDED.sources, updated_at = now()
"""


def build_merged() -> dict:
    """Cross-source de-duplication: one row per (card_number, variant)."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(MERGE_SQL)
        cur.execute("SELECT count(*), count(*) FILTER (WHERE n_sources > 1) FROM cards_merged")
        merged, multi = cur.fetchone()
        cur.execute("SELECT count(*) FROM card_prices")
        raw_total = cur.fetchone()[0]
    res = {"source_rows": raw_total, "unique_cards": merged, "matched_in_2plus_sources": multi,
           "duplicates_removed": raw_total - merged}
    log.info("merge: %s", res)
    return res


def record_run(run_id: str, started_at, task_states: dict, rows_loaded: int | None,
               quality: dict | None, branch: str | None):
    passed = None if quality is None else quality.get("passed")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute(
            """INSERT INTO pipeline_runs (run_id, started_at, finished_at, task_states, rows_loaded,
                                          quality_passed, quality_details, branch)
               VALUES (%s, %s, now(), %s, %s, %s, %s, %s)
               ON CONFLICT (run_id) DO UPDATE SET finished_at = now(), task_states = EXCLUDED.task_states,
                   rows_loaded = EXCLUDED.rows_loaded, quality_passed = EXCLUDED.quality_passed,
                   quality_details = EXCLUDED.quality_details, branch = EXCLUDED.branch""",
            (run_id, started_at, json.dumps(task_states), rows_loaded, passed,
             json.dumps(quality) if quality else None, branch),
        )
