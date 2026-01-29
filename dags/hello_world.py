from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def print_env_and_paths():
    # Key env vars
    print("AIRFLOW_HOME:", os.environ.get("AIRFLOW_HOME"))
    print("DATA_DIR:", os.environ.get("DATA_DIR"))

    # Validate mounted folders exist inside the container
    for p in ["/opt/airflow/dags", "/opt/airflow/logs", "/opt/data"]:
        print(f"Checking path exists: {p} -> {os.path.exists(p)}")

    # Show what's inside /opt/data
    try:
        print("Listing /opt/data:")
        for name in os.listdir("/opt/data"):
            print(" -", name)
    except Exception as e:
        print("Could not list /opt/data:", e)


with DAG(
    dag_id="00_hello_world_local_stack",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # manual trigger
    catchup=False,
    tags=["phase0"],
) as dag:
    t1 = PythonOperator(
        task_id="print_env_and_paths",
        python_callable=print_env_and_paths,
    )

    t1


# What this proves:
# DAGs folder is mounted correctly
# /opt/data mount works (your lake folders are accessible)
# Airflow can execute Python tasks