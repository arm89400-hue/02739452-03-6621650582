# One Piece Card Game OP17 Price Aggregator

**Repo:** `02739452-03-6621650582` · Student ID 6621650582 · No. 03

Final exam project, **02739452 Big Data Platform and Analytics** (Kasetsart University, Kamphaeng Saen).

**Topic:** card prices for the One Piece Card Game set **OP17 "The World's Strongest Warriors"**, combined from 3 sources into one dataset.
**Stack:** Apache Airflow 2.10 (LocalExecutor) → PostgreSQL 16 → Streamlit dashboard, all in Docker Compose.

## Data sources

| # | Source | Method | What we take | robots.txt |
|---|--------|--------|--------------|------------|
| 1 | [PriceCharting](https://www.pricecharting.com/console/one-piece-world%27s-strongest-warriors) | **Web scraping** (requests + BeautifulSoup, no Selenium) | Ungraded / Grade 9 / PSA 10 price (USD) | `/console/*` allowed (only `/buy`, `/publish-offer` and `/stripe-connect` are disallowed) |
| 2 | [Limitless TCG](https://onepiece.limitlesstcg.com/cards/op17-the-worlds-strongest-warriors) | **Web scraping**: the set page, then each card page | USD + EUR price for each print | `Disallow:` is empty (everything allowed) |
| 3 | [TCGCSV](https://tcgcsv.com/) (daily copy of TCGplayer prices) | **JSON API** `/tcgplayer/68/24736/products` + `/prices` | TCGplayer market price (USD), rarity, card number | Public API |

No personal data is collected (PDPA): only card names, card numbers and prices.
Requests use a 0.7 s delay and an identifying User-Agent.

## Pipeline (DAG `op17_card_prices`)

```
create_tables
   └─► TaskGroup "extract"   ┌ pricecharting ┐
                             ├ limitless     ┤   (run in parallel)
                             └ tcgcsv        ┘
   └─► TaskGroup "transform" transform.expand(source=[...])   ← Dynamic Task Mapping, trigger_rule=all_done
   └─► load_upsert           INSERT ... ON CONFLICT (source, source_key) DO UPDATE   ← re-runs do not add rows
   └─► quality_check         @task.branch
          ├─► build_merged   cross-source de-dup → cards_merged (1 row per card_number + version)
          └─► quality_alert  (if a check fails, the merged table is not updated)
   └─► record_run            writes task states and quality results → pipeline_runs (shown on the web page)
```

* **Source down:** the transform, load and later tasks use `trigger_rule=all_done`, so if one source fails the DAG still merges the others.
  To demo an outage, open **Trigger DAG w/ config** and enter `{"simulate_down": "limitless"}`.
* **Common schema:** `source, source_key, card_number, card_name, variant, rarity, price_usd, price_eur, grade9_usd, psa10_usd, url, fetched_at`.
  The `variant` column puts each site's wording into one code: `NORMAL / ALT / SUPER_ALT / MANGA / SP / TREASURE / PROMO`.
* **Removing duplicates across sources:** the key is `(card_number, variant)`, e.g. `OP17-079 + SUPER_ALT`. The merged table stores each source's price side by side, plus min/max/avg.
* **Quality checks:** at least 100 rows in total; at least 1 source loaded in this run; card number matches `XX00-000`; no negative prices; fewer than 20% of rows without a price.

### Tables (PostgreSQL database `cards`)
| Table | Contents |
|-------|----------|
| `card_prices` | One row per source and product (UPSERT on `source, source_key`) |
| `cards_merged` | One row per unique card after removing cross-source duplicates |
| `pipeline_runs` | Run history for the status page |

## Web dashboard (http://localhost:8501)
Streamlit, Thai interface, reads **directly from PostgreSQL** (psycopg2). Prices are shown in **Thai baht**: USD prices are converted with the latest rate from open.er-api.com, falling back to 33 THB/USD if the rate can't be fetched.
* **🔍 ค้นหาราคา (price search):** search by card name or number, filter by card type, sort; KPI cards; a gallery of card pictures with names; a price table with each site's price and the cheapest site (10 rows per page).
* **📊 สรุปภาพรวม (overview):** top-10 most expensive cards, cards by price range, median price by card type, and price records per site.
* **⚙️ สถานะระบบ (system status, bonus):** the latest run's task states, quality-check results and run history.

Card pictures come from TCGplayer's image server, using the product ID stored by the pipeline.

## How to run

Requirements: Docker Desktop.

```bash
git clone <this repo> && cd <repo>
cp .env.example .env          # Windows PowerShell: copy .env.example .env   (then change the passwords)
docker compose up -d --build  # first start takes a few minutes
```

1. Airflow UI: http://localhost:8080 (user and password from `.env`). The DAG `op17_card_prices` is unpaused and runs every 6 hours. Press ▶ **Trigger DAG** to run it now (about 3–4 minutes, mostly the Limitless card pages).
2. Dashboard: http://localhost:8501
3. PostgreSQL from the host: `localhost:5433`, database `cards`.

Stop: `docker compose down` (add `-v` to delete the data).

## Project layout
```
dags/op17_card_prices_dag.py   DAG definition
dags/op17_pipeline.py          extract / transform / load / quality / merge functions
web/app.py, web/Dockerfile     Streamlit dashboard
postgres/init/                 creates the Airflow metadata database
docker-compose.yml, requirements.txt, .env.example
```
