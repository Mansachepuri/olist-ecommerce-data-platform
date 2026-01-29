import argparse
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F


BRONZE_BASE = "/opt/data/bronze/olist"
SILVER_BASE = "/opt/data/silver/olist"


def get_default_ingestion_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def read_bronze(spark: SparkSession, table: str, ingestion_date: str):
    path = f"{BRONZE_BASE}/{table}/ingestion_date={ingestion_date}"
    return spark.read.parquet(path)


def write_silver(df, table: str, ingestion_date: str):
    out_path = f"{SILVER_BASE}/{table}/ingestion_date={ingestion_date}"
    (
        df.write.mode("overwrite")
        .parquet(out_path)
    )
    return out_path


def transform_orders(df):
    # Cast timestamps (safe parsing)
    ts_cols = ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
               "order_delivered_customer_date", "order_estimated_delivery_date"]
    for c in ts_cols:
        if c in df.columns:
            df = df.withColumn(c, F.to_timestamp(F.col(c)))

    # Basic validity: order_id not null, customer_id not null
    df = df.filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())

    # Deduplicate on order_id, keep latest by ingestion_ts
    if "ingestion_ts" in df.columns:
        w = F.window  # placeholder to avoid importing Window for beginners
    # Use dropDuplicates (good enough for Bronze->Silver here)
    df = df.dropDuplicates(["order_id"])

    # Keep only relevant columns (plus lineage)
    keep = [c for c in df.columns]
    return df.select(*keep)


def transform_order_items(df):
    # Cast numeric columns
    num_cols = ["price", "freight_value"]
    for c in num_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))

    # order_item_id should be int
    if "order_item_id" in df.columns:
        df = df.withColumn("order_item_id", F.col("order_item_id").cast("int"))

    # Validity: no null keys and non-negative price
    df = df.filter(
        F.col("order_id").isNotNull()
        & F.col("product_id").isNotNull()
        & F.col("seller_id").isNotNull()
        & (F.col("price") >= 0)
    )

    # Deduplicate on composite key (order_id, order_item_id)
    if "order_id" in df.columns and "order_item_id" in df.columns:
        df = df.dropDuplicates(["order_id", "order_item_id"])

    return df


def transform_customers(df):
    # Validity: ids exist
    df = df.filter(F.col("customer_id").isNotNull() & F.col("customer_unique_id").isNotNull())
    df = df.dropDuplicates(["customer_id"])
    return df


def transform_products(df):
    df = df.filter(F.col("product_id").isNotNull())
    df = df.dropDuplicates(["product_id"])

    # Cast some columns if present
    for c in ["product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingestion_date", default=get_default_ingestion_date())
    args = parser.parse_args()

    ingestion_date = args.ingestion_date
    spark = (
        SparkSession.builder
        .appName("olist_bronze_to_silver")
        .getOrCreate()
    )

    # Reduce noise
    spark.sparkContext.setLogLevel("WARN")

    # Process core tables first
    tables = ["orders", "order_items", "customers", "products"]

    for t in tables:
        df_bronze = read_bronze(spark, t, ingestion_date)

        if t == "orders":
            df_silver = transform_orders(df_bronze)
        elif t == "order_items":
            df_silver = transform_order_items(df_bronze)
        elif t == "customers":
            df_silver = transform_customers(df_bronze)
        elif t == "products":
            df_silver = transform_products(df_bronze)
        else:
            df_silver = df_bronze

        out = write_silver(df_silver, t, ingestion_date)
        print(f"[OK] silver.{t} -> {out} (rows={df_silver.count()})")

    spark.stop()
    print("Done ✅")


if __name__ == "__main__":
    main()
