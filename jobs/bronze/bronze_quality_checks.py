from __future__ import annotations
import os
from datetime import datetime, timezone
import pandas as pd

BRONZE_BASE = "/opt/data/bronze/olist"

# Define minimal, realistic DQ rules per table
RULES = {
    "orders": {
        "not_null": ["order_id", "customer_id", "order_status"],
        "unique": ["order_id"],
    },
    "order_items": {
        "not_null": ["order_id", "order_item_id", "product_id", "seller_id"],
        "unique": [],  # composite uniqueness handled later in Silver
    },
    "customers": {
        "not_null": ["customer_id", "customer_unique_id"],
        "unique": ["customer_id"],
    },
}


def get_today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def find_latest_parquet(table: str) -> str:
    # We partition by ingestion_date=YYYY-MM-DD, so check today's partition
    today = get_today_utc()
    part_dir = os.path.join(BRONZE_BASE, table, f"ingestion_date={today}")
    if not os.path.exists(part_dir):
        raise FileNotFoundError(f"Expected partition not found: {part_dir}")

    files = [os.path.join(part_dir, f) for f in os.listdir(part_dir) if f.endswith(".parquet")]
    if not files:
        raise FileNotFoundError(f"No parquet files found in: {part_dir}")

    # If multiple, just pick first (for Bronze smoke). Later we can validate all.
    return files[0]


def check_rowcount(df: pd.DataFrame, table: str):
    if len(df) == 0:
        raise AssertionError(f"[{table}] Row count is 0")
    print(f"[{table}] Row count OK: {len(df)}")


def check_not_null(df: pd.DataFrame, table: str, cols: list[str]):
    for c in cols:
        if c not in df.columns:
            raise AssertionError(f"[{table}] Missing column: {c}")
        nulls = df[c].isna().sum()
        if nulls > 0:
            raise AssertionError(f"[{table}] Nulls in {c}: {nulls}")
    print(f"[{table}] Not-null checks OK: {cols}")


def check_unique(df: pd.DataFrame, table: str, cols: list[str]):
    for c in cols:
        if c not in df.columns:
            raise AssertionError(f"[{table}] Missing column: {c}")
        dupes = df[c].duplicated().sum()
        if dupes > 0:
            raise AssertionError(f"[{table}] Duplicates in {c}: {dupes}")
    if cols:
        print(f"[{table}] Uniqueness checks OK: {cols}")


def main():
    # Only run rules for tables we defined (starter set)
    for table, rules in RULES.items():
        parquet_file = find_latest_parquet(table)
        print(f"\nValidating {table} using {parquet_file}")

        df = pd.read_parquet(parquet_file)

        check_rowcount(df, table)
        check_not_null(df, table, rules.get("not_null", []))
        check_unique(df, table, rules.get("unique", []))

    print("\nAll Bronze quality checks passed ✅")


if __name__ == "__main__":
    main()
