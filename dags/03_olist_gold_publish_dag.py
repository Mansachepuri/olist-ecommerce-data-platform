from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

SILVER_ORDERS_BASE = "/opt/data/silver/olist/orders"
GOLD_BASE = "/opt/data/gold/warehouse"


def assert_silver_partition_exists(execution_date_str: str):
    part = os.path.join(SILVER_ORDERS_BASE, f"ingestion_date={execution_date_str}")
    if not os.path.exists(part):
        raise FileNotFoundError(f"Silver partition not found: {part}")
    print(f"Silver partition exists ✅ {part}")


def gold_smoke_check(execution_date_str: str):
    # Confirm Gold parquet exists for that date
    if not os.path.exists(GOLD_BASE):
        raise FileNotFoundError(f"Gold base path missing: {GOLD_BASE}")

    parquet_count = 0
    for root, _, files in os.walk(GOLD_BASE):
        if f"ingestion_date={execution_date_str}" in root:
            parquet_count += sum(1 for f in files if f.endswith(".parquet"))

    if parquet_count == 0:
        raise RuntimeError(f"No gold parquet files found for ingestion_date={execution_date_str}")

    print(f"Gold smoke check passed ✅ parquet_files={parquet_count}")


with DAG(
    dag_id="03_olist_gold_build_and_publish",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["olist", "gold", "publish"],
    default_args={"retries": 2},
) as dag:

    ingestion_date = "{{ ds }}"

    with TaskGroup("tg_validate_silver") as tg_validate_silver:
        t_validate = PythonOperator(
            task_id="validate_silver_partition",
            python_callable=assert_silver_partition_exists,
            op_args=[ingestion_date],
        )

    with TaskGroup("tg_build_gold") as tg_build_gold:
        t_build = BashOperator(
            task_id="build_gold_parquet",
            bash_command=f"python /opt/airflow/jobs/gold/silver_to_gold.py --ingestion_date {ingestion_date}",
        )

    with TaskGroup("tg_gold_smoke") as tg_gold_smoke:
        t_smoke = PythonOperator(
            task_id="gold_smoke_check",
            python_callable=gold_smoke_check,
            op_args=[ingestion_date],
        )

    with TaskGroup("tg_publish_postgres") as tg_publish_postgres:
        t_publish = BashOperator(
            task_id="publish_gold_to_postgres",
            bash_command=f"python /opt/airflow/jobs/publish/gold_to_postgres.py --ingestion_date {ingestion_date}",
        )

    t_validate >> t_build >> t_smoke >> t_publish
