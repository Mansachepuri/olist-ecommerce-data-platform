from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

BRONZE_ORDERS_BASE = "/opt/data/bronze/olist/orders"
SILVER_BASE = "/opt/data/silver/olist"


def assert_bronze_partition_exists(execution_date_str: str):
    # execution_date_str will be passed like "YYYY-MM-DD"
    part = os.path.join(BRONZE_ORDERS_BASE, f"ingestion_date={execution_date_str}")
    if not os.path.exists(part):
        raise FileNotFoundError(
            f"Bronze partition not found for orders: {part}\n"
            f"Tip: run Bronze for that date first, or trigger Silver with a date that exists."
        )
    print(f"Bronze partition exists ✅ {part}")


def silver_smoke_check(execution_date_str: str):
    # ensure at least one parquet exists under silver for that date
    base = os.path.join(SILVER_BASE)
    if not os.path.exists(base):
        raise FileNotFoundError(f"Silver base path missing: {base}")

    count = 0
    for root, _, files in os.walk(base):
        if f"ingestion_date={execution_date_str}" in root:
            count += sum(1 for f in files if f.endswith(".parquet"))

    if count == 0:
        raise RuntimeError(f"No silver parquet files found for ingestion_date={execution_date_str}")
    print(f"Silver smoke check passed ✅ parquet_files={count}")


with DAG(
    dag_id="02_olist_silver_transform",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # manual for now
    catchup=False,
    tags=["olist", "silver"],
    default_args={"retries": 2},
) as dag:

    # We'll use Airflow's {{ ds }} as ingestion_date (YYYY-MM-DD)
    ingestion_date = "{{ ds }}"

    with TaskGroup("tg_validate_bronze") as tg_validate_bronze:
        t_validate = PythonOperator(
            task_id="validate_bronze_partition",
            python_callable=assert_bronze_partition_exists,
            op_args=[ingestion_date],
        )

    with TaskGroup("tg_silver_transform") as tg_silver_transform:
        t_run_silver = BashOperator(
            task_id="run_bronze_to_silver",
            bash_command=(
                "python /opt/airflow/jobs/silver/bronze_to_silver.py "
                f"--ingestion_date {ingestion_date}"
            ),
        )

    with TaskGroup("tg_silver_smoke") as tg_silver_smoke:
        t_smoke = PythonOperator(
            task_id="silver_smoke_check",
            python_callable=silver_smoke_check,
            op_args=[ingestion_date],
        )

    tg_validate_bronze >> tg_silver_transform >> tg_silver_smoke
