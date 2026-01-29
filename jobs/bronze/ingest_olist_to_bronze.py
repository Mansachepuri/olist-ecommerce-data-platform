import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ingestion_date_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def read_manifest(manifest_path: str) -> dict:
    with open(manifest_path, "r", encoding="utf-8") as f:
        return json.load(f)


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def ingest_table(raw_base: str, bronze_base: str, table_name: str, filename: str) -> str:
    raw_path = os.path.join(raw_base, filename)
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw file not found: {raw_path}")

    ingest_ts = utc_now_iso()
    ingest_date = ingestion_date_utc()

    df = pd.read_csv(raw_path)

    # Bronze metadata (minimal, production-ish)
    df["ingestion_ts"] = ingest_ts
    df["ingestion_date"] = ingest_date
    df["source_file"] = filename

    out_dir = os.path.join(bronze_base, table_name, f"ingestion_date={ingest_date}")
    ensure_dir(out_dir)

    out_file = os.path.join(out_dir, f"{table_name}_{ingest_date}.parquet")
    df.to_parquet(out_file, index=False)

    return out_file


def main():
    manifest_path = os.environ.get("OLIST_MANIFEST", "/opt/airflow/config/olist_tables.json")

    if not os.path.exists(manifest_path):
        raise FileNotFoundError(
            f"Manifest not found at {manifest_path}. "
            f"Make sure config is mounted into Airflow container."
        )

    manifest = read_manifest(manifest_path)
    raw_base = manifest["raw_base_path"]
    bronze_base = manifest["bronze_base_path"]

    print(f"Using manifest: {manifest_path}")
    print(f"Raw base: {raw_base}")
    print(f"Bronze base: {bronze_base}")

    results = []
    for t in manifest["tables"]:
        out = ingest_table(raw_base, bronze_base, t["name"], t["file"])
        print(f"[OK] {t['name']} -> {out}")
        results.append(out)

    print(f"Done. Wrote {len(results)} parquet files.")


if __name__ == "__main__":
    main()
