"""
DAG: op17_card_prices
One Piece Card Game OP17 "The World's Strongest Warriors" - price comparison from 3 sources.

create_tables
   -> [TaskGroup extract]   pricecharting | limitless | tcgcsv      (run in parallel)
   -> [TaskGroup transform] transform.expand(source=...)            (Dynamic Task Mapping)
   -> load_upsert                  (trigger_rule=all_done -> continues even if a source is down)
   -> quality_check (branch) -> build_merged   (pass: cross-source de-dup into cards_merged)
                             -> quality_alert  (fail)
   -> record_run                   (writes run status for the web "Pipeline status" page)

Trigger with config {"simulate_down": "limitless"} to demo a source outage.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

import op17_pipeline as P


def _tag() -> str:
    ctx = get_current_context()
    return ctx["dag_run"].run_id.replace(":", "").replace("+", "").replace(".", "_")


@dag(
    dag_id="op17_card_prices",
    description="One Piece OP17 card prices: PriceCharting + Limitless TCG + TCGplayer(TCGCSV) -> PostgreSQL",
    schedule="0 */6 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Bangkok"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=15), "execution_timeout": timedelta(minutes=20)},
    params={"simulate_down": Param("", type="string", description="comma list of sources to fail on purpose, e.g. limitless")},
    tags=["02739452", "onepiece", "final-exam"],
    doc_md=__doc__,
)
def op17_card_prices():

    @task
    def create_tables():
        P.create_tables()

    @task_group(group_id="extract", tooltip="Fetch the 3 sources in parallel")
    def extract_group():
        tasks = []
        for source in P.SOURCES:
            @task(task_id=source)
            def _extract(src: str = source) -> str:
                ctx = get_current_context()
                return P.extract(src, _tag(), ctx["params"].get("simulate_down", ""))
            tasks.append(_extract())
        return tasks

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def transform(source: str) -> dict:
        return P.transform(source, _tag())

    @task_group(group_id="transform", tooltip="Normalise each source to one schema (dynamic task mapping)")
    def transform_group():
        return transform.expand(source=P.SOURCES)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def load_upsert() -> dict:
        return P.load(_tag())

    @task.branch(trigger_rule=TriggerRule.ALL_DONE)
    def quality_check() -> str:
        passed, details = P.quality_check(_tag())
        details["passed"] = passed
        get_current_context()["ti"].xcom_push(key="quality", value=details)
        return "build_merged" if passed else "quality_alert"

    @task
    def build_merged() -> dict:
        return P.build_merged()

    @task
    def quality_alert():
        ti = get_current_context()["ti"]
        q = ti.xcom_pull(task_ids="quality_check", key="quality")
        failed = [k for k, v in (q or {}).get("checks", {}).items() if not v]
        # In production this would send LINE/Email; here we log it and keep the old merged table.
        print(f"DATA QUALITY FAILED -> merged table NOT updated. Failed checks: {failed}")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def record_run():
        ctx = get_current_context()
        dr, ti = ctx["dag_run"], ctx["ti"]
        states = {}
        for t in dr.get_task_instances():
            if t.task_id == "record_run":
                continue
            name = t.task_id if t.map_index < 0 else f"{t.task_id}[{P.SOURCES[t.map_index]}]"
            states[name] = t.state
        q = ti.xcom_pull(task_ids="quality_check", key="quality")
        load_res = ti.xcom_pull(task_ids="load_upsert") or {}
        branch = None
        if states.get("build_merged") == "success":
            branch = "build_merged"
        elif states.get("quality_alert") == "success":
            branch = "quality_alert"
        P.record_run(dr.run_id, dr.start_date, states, load_res.get("table_rows"), q, branch)

    t_create = create_tables()
    t_extract = extract_group()
    t_transform = transform_group()
    t_load = load_upsert()
    t_qc = quality_check()
    t_merge = build_merged()
    t_alert = quality_alert()
    t_record = record_run()

    t_create >> t_extract >> t_transform >> t_load >> t_qc >> [t_merge, t_alert] >> t_record


op17_card_prices()
