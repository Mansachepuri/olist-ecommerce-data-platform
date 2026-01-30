from __future__ import annotations
import argparse
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


GOLD_BASE = "/opt/data/gold/warehouse"
TABLES = [
    "dim_customers",
    "dim_products",
    "fact_orders",
    "fact_order_items",
    "agg_daily_revenue",
]


def read_gold_table(table: str, ingestion_date: str) -> pd.DataFrame:
    part_dir = os.path.join(GOLD_BASE, table, f"ingestion_date={ingestion_date}")
    if not os.path.exists(part_dir):
        raise FileNotFoundError(f"Gold partition not found: {part_dir}")

    files = [os.path.join(part_dir, f) for f in os.listdir(part_dir) if f.endswith(".parquet")]
    if not files:
        raise FileNotFoundError(f"No parquet files found in: {part_dir}")

    dfs = [pd.read_parquet(f) for f in files]
    return pd.concat(dfs, ignore_index=True)


def get_engine():
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ["POSTGRES_DB"]
    user = os.environ["POSTGRES_USER"]
    pwd = os.environ["POSTGRES_PASSWORD"]

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url)


def ensure_schema(engine, schema: str = "gold"):
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))


def publish_table(engine, df: pd.DataFrame, table: str, schema: str = "gold"):
    full_name = f"{schema}.{table}"

    # Full refresh: drop & recreate via pandas to_sql
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {full_name};"))

    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
        chunksize=5000,
        method="multi",
    )
    print(f"[OK] loaded {full_name} rows={len(df)}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingestion_date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    ingestion_date = args.ingestion_date

    engine = get_engine()
    ensure_schema(engine, "gold")

    for t in TABLES:
        df = read_gold_table(t, ingestion_date)
        publish_table(engine, df, t, "gold")

    print("Done ✅ Gold published to Postgres.")


if __name__ == "__main__":
    main()
