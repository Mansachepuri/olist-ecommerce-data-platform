from __future__ import annotations 
import argparse
import os
from pathlib import Path

import pandas as pd


SILVER_BASE = "/opt/data/silver/olist"
GOLD_BASE = "/opt/data/gold/warehouse"


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def read_silver(table: str, ingestion_date: str) -> pd.DataFrame:
    part_dir = os.path.join(SILVER_BASE, table, f"ingestion_date={ingestion_date}")
    if not os.path.exists(part_dir):
        raise FileNotFoundError(f"Silver partition not found: {part_dir}")

    files = [os.path.join(part_dir, f) for f in os.listdir(part_dir) if f.endswith(".parquet")]
    if not files:
        raise FileNotFoundError(f"No parquet files found in: {part_dir}")

    dfs = [pd.read_parquet(f) for f in files]
    return pd.concat(dfs, ignore_index=True)


def write_gold(df: pd.DataFrame, table: str, ingestion_date: str) -> str:
    out_dir = os.path.join(GOLD_BASE, table, f"ingestion_date={ingestion_date}")
    ensure_dir(out_dir)
    out_file = os.path.join(out_dir, f"{table}_{ingestion_date}.parquet")
    df.to_parquet(out_file, index=False)
    return out_file


def build_dim_customers(customers: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ]
    cols = [c for c in cols if c in customers.columns]
    dim = customers[cols].drop_duplicates(subset=["customer_id"], keep="last")

    dim = dim.rename(
        columns={
            "customer_zip_code_prefix": "zip_prefix",
            "customer_city": "city",
            "customer_state": "state",
        }
    )
    return dim


def build_dim_products(products: pd.DataFrame, translation: pd.DataFrame | None = None) -> pd.DataFrame:
    cols = [
        "product_id",
        "product_category_name",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]
    cols = [c for c in cols if c in products.columns]
    dim = products[cols].drop_duplicates(subset=["product_id"], keep="last").copy()

    # Optional: join category translation if it exists in Silver later
    if translation is not None and "product_category_name" in dim.columns:
        if "product_category_name" in translation.columns and "product_category_name_english" in translation.columns:
            dim = dim.merge(translation, on="product_category_name", how="left")

    dim = dim.rename(columns={"product_category_name": "category_name"})
    return dim


def build_fact_orders(orders: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    cols = [c for c in cols if c in orders.columns]
    fact = orders[cols].drop_duplicates(subset=["order_id"], keep="last").copy()

    fact = fact.rename(
        columns={
            "order_purchase_timestamp": "purchase_ts",
            "order_approved_at": "approved_ts",
            "order_delivered_carrier_date": "delivered_carrier_ts",
            "order_delivered_customer_date": "delivered_customer_ts",
            "order_estimated_delivery_date": "estimated_delivery_ts",
        }
    )
    return fact


def build_fact_order_items(order_items: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
    ]
    cols = [c for c in cols if c in order_items.columns]
    fact = order_items[cols].copy()

    # Composite key dedupe (order_id, order_item_id)
    if "order_item_id" in fact.columns:
        fact = fact.drop_duplicates(subset=["order_id", "order_item_id"], keep="last")
    else:
        fact = fact.drop_duplicates(keep="last")

    if "shipping_limit_date" in fact.columns:
        fact = fact.rename(columns={"shipping_limit_date": "shipping_limit_ts"})

    return fact


def build_agg_daily_revenue(fact_orders: pd.DataFrame, fact_items: pd.DataFrame) -> pd.DataFrame:
    # Define "order_date" from purchase timestamp
    if "purchase_ts" not in fact_orders.columns:
        raise ValueError("fact_orders is missing purchase_ts (from order_purchase_timestamp)")

    # Convert to date
    orders = fact_orders[["order_id", "order_status", "purchase_ts"]].copy()
    orders["order_date"] = pd.to_datetime(orders["purchase_ts"], errors="coerce").dt.date

    # Revenue per order = sum(price + freight_value)
    items = fact_items[["order_id", "price", "freight_value"]].copy()
    items["price"] = pd.to_numeric(items["price"], errors="coerce").fillna(0.0)
    items["freight_value"] = pd.to_numeric(items["freight_value"], errors="coerce").fillna(0.0)
    items["line_revenue"] = items["price"] + items["freight_value"]

    revenue_by_order = items.groupby("order_id", as_index=False)["line_revenue"].sum()

    merged = orders.merge(revenue_by_order, on="order_id", how="left")
    merged["line_revenue"] = merged["line_revenue"].fillna(0.0)

    daily = merged.groupby("order_date", as_index=False).agg(
        total_orders=("order_id", "nunique"),
        total_revenue=("line_revenue", "sum"),
        avg_order_value=("line_revenue", "mean"),
        delivered_orders=("order_status", lambda s: (s == "delivered").sum()),
    )

    # Round for readability
    daily["total_revenue"] = daily["total_revenue"].round(2)
    daily["avg_order_value"] = daily["avg_order_value"].round(2)

    return daily.sort_values("order_date")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingestion_date", required=True, help="YYYY-MM-DD (must exist in Silver)")
    args = parser.parse_args()

    ingestion_date = args.ingestion_date

    # Read Silver core tables
    customers = read_silver("customers", ingestion_date)
    products = read_silver("products", ingestion_date)
    orders = read_silver("orders", ingestion_date)
    order_items = read_silver("order_items", ingestion_date)

    # Build Gold tables
    dim_customers = build_dim_customers(customers)
    dim_products = build_dim_products(products)
    fact_orders = build_fact_orders(orders)
    fact_order_items = build_fact_order_items(order_items)
    agg_daily_revenue = build_agg_daily_revenue(fact_orders, fact_order_items)

    # Write Gold
    outputs = []
    outputs.append(write_gold(dim_customers, "dim_customers", ingestion_date))
    outputs.append(write_gold(dim_products, "dim_products", ingestion_date))
    outputs.append(write_gold(fact_orders, "fact_orders", ingestion_date))
    outputs.append(write_gold(fact_order_items, "fact_order_items", ingestion_date))
    outputs.append(write_gold(agg_daily_revenue, "agg_daily_revenue", ingestion_date))

    for o in outputs:
        print(f"[OK] wrote {o}")

    print("Done ✅ Gold layer written.")


if __name__ == "__main__":
    main()
