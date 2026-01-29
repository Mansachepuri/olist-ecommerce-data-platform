from __future__ import annotations

import json
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


MANIFEST_PATH = "/opt/airflow/config/olist_tables.json"
BRONZE_BASE = "/opt/data/bronze/olist"


def load_manifest() -> dict:
    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def validate_raw_files():
    manifest = load_manifest()
    raw_base = manifest["raw_base_path"]

    missing = []
    for t in manifest["tables"]:
        p = os.path.join(raw_base, t["file"])
        if not os.path.exists(p):
            missing.append(p)

    if missing:
        raise FileNotFoundError("Missing raw files:\n" + "\n".join(missing))

    print(f"All raw files present under {raw_base} ✅")


def bronze_smoke_check():
    # Just confirm bronze base exists and has at least some parquet files
    if not os.path.exists(BRONZE_BASE):
        raise FileNotFoundError(f"Bronze base path not found: {BRONZE_BASE}")

    parquet_count = 0
    for root, _, files in os.walk(BRONZE_BASE):
        parquet_count += sum(1 for f in files if f.endswith(".parquet"))

    if parquet_count == 0:
        raise RuntimeError("No parquet files found in bronze layer.")

    print(f"Bronze smoke check passed ✅ parquet files found: {parquet_count}")


with DAG(
    dag_id="01_olist_bronze_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # run manually for now
    catchup=False,
    tags=["olist", "bronze"],
    default_args={"retries": 2},
) as dag:

    with TaskGroup("tg_validate_raw") as tg_validate_raw:
        t_validate_raw = PythonOperator(
            task_id="validate_raw_files",
            python_callable=validate_raw_files,
        )

    with TaskGroup("tg_bronze_ingest") as tg_bronze_ingest:
        t_run_bronze = BashOperator(
            task_id="run_bronze_ingestion_script",
            bash_command="python /opt/airflow/jobs/bronze/ingest_olist_to_bronze.py",
        )

    with TaskGroup("tg_bronze_smoke") as tg_bronze_smoke:
        t_smoke = PythonOperator(
            task_id="bronze_smoke_check",
            python_callable=bronze_smoke_check,
        )

    tg_validate_raw >> tg_bronze_ingest >> tg_bronze_smoke
