from __future__ import annotations
import argparse
import os
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd


BRONZE_BASE = "/opt/data/bronze/olist"
SILVER_BASE = "/opt/data/silver/olist"

CORE_TABLES = ["orders", "order_items", "customers", "products"]


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def list_ingestion_dates(table: str) -> list[str]:
    table_dir = os.path.join(BRONZE_BASE, table)
    if not os.path.exists(table_dir):
        return []
    parts = [p for p in os.listdir(table_dir) if p.startswith("ingestion_date=")]
    dates = [p.split("=", 1)[1] for p in parts]
    return sorted(dates)


def latest_ingestion_date(table: str) -> str:
    dates = list_ingestion_dates(table)
    if not dates:
        raise FileNotFoundError(f"No ingestion partitions found for bronze table: {table}")
    return dates[-1]


def read_bronze_table(table: str, ingestion_date: str) -> pd.DataFrame:
    part_dir = os.path.join(BRONZE_BASE, table, f"ingestion_date={ingestion_date}")
    if not os.path.exists(part_dir):
        raise FileNotFoundError(f"Bronze partition not found: {part_dir}")

    files = [os.path.join(part_dir, f) for f in os.listdir(part_dir) if f.endswith(".parquet")]
    if not files:
        raise FileNotFoundError(f"No parquet files in: {part_dir}")

    # read all files in the partition
    dfs = [pd.read_parquet(f) for f in files]
    return pd.concat(dfs, ignore_index=True)


def write_silver_table(df: pd.DataFrame, table: str, ingestion_date: str) -> str:
    out_dir = os.path.join(SILVER_BASE, table, f"ingestion_date={ingestion_date}")
    ensure_dir(out_dir)
    out_file = os.path.join(out_dir, f"{table}_{ingestion_date}.parquet")
    df.to_parquet(out_file, index=False)
    return out_file


# ----------------- Table transforms -----------------

def to_datetime_safe(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    # Parse timestamps (coerce invalid to NaT)
    for col in [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]:
        if col in df.columns:
            df[col] = to_datetime_safe(df[col])

    # Validity filters
    df = df[df["order_id"].notna()]
    df = df[df["customer_id"].notna()]

    # Deduplicate
    df = df.drop_duplicates(subset=["order_id"], keep="last")

    return df


def transform_order_items(df: pd.DataFrame) -> pd.DataFrame:
    # Type casts
    if "order_item_id" in df.columns:
        df["order_item_id"] = pd.to_numeric(df["order_item_id"], errors="coerce").astype("Int64")

    for col in ["price", "freight_value"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Validity filters
    df = df[df["order_id"].notna()]
    df = df[df["product_id"].notna()]
    df = df[df["seller_id"].notna()]
    if "price" in df.columns:
        df = df[df["price"].fillna(0) >= 0]

    # Deduplicate composite key
    if "order_item_id" in df.columns:
        df = df.drop_duplicates(subset=["order_id", "order_item_id"], keep="last")

    return df


def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["customer_id"].notna()]
    df = df[df["customer_unique_id"].notna()]
    df = df.drop_duplicates(subset=["customer_id"], keep="last")
    return df


def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["product_id"].notna()]
    df = df.drop_duplicates(subset=["product_id"], keep="last")

    for col in [
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


TRANSFORMS = {
    "orders": transform_orders,
    "order_items": transform_order_items,
    "customers": transform_customers,
    "products": transform_products,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingestion_date", default=None, help="YYYY-MM-DD; defaults to latest per table")
    args = parser.parse_args()

    for table in CORE_TABLES:
        ingestion_date = args.ingestion_date or latest_ingestion_date(table)

        print(f"\n[INFO] Processing {table} for ingestion_date={ingestion_date}")
        df_bronze = read_bronze_table(table, ingestion_date)

        transform_fn = TRANSFORMS.get(table, lambda x: x)
        df_silver = transform_fn(df_bronze)

        out_file = write_silver_table(df_silver, table, ingestion_date)
        print(f"[OK] silver.{table} -> {out_file} (rows={len(df_silver)})")

    print("\nDone ✅ Silver layer written.")


if __name__ == "__main__":
    main()
