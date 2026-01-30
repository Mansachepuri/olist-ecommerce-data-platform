## Olist E-commerce Analytics Platform
A Local End-to-End Data Engineering Project
#### Project overview
This project is a end-to-end data ingestion and analytics platform built entirely on a local machine using Docker.

It demonstrates how raw business data can be transformed into analytics-ready insights using a Bronze → Silver → Gold architecture, orchestrated by Apache Airflow and visualized through Metabase.

The dataset used is the Olist Brazilian E-commerce dataset, simulating a real-world e-commerce analytics workflow

#### Architecture Overview
``` text 
  Raw CSV (Kaggle Olist Dataset)
        ↓
  Bronze Layer (Raw Ingestion)
        ↓
  Silver Layer (Cleaned & Standardized)
        ↓
  Gold Layer (Fact & Aggregate Tables)
        ↓
  PostgreSQL Warehouse
        ↓
  Metabase Executive Dashboard
```
#### Core principles:
- Clear data layer separation
- Idempotent, restart-safe pipelines
- Analytics-first data modeling
- Local-first, cloud-agnostic design

####  Tech Stack

| Component | Technology |
|------|------------|
| Orchestration | Apache Airflow |
| Transformations | Python |
| Storage | Bronze/Silver)	Parquet |
| Analytics Warehouse | PostgreSQL |
| Frontend | Streamlit |
| BI & Dashboards | Metabase |
| Infrastructure | Docker & Docker Compose |

	
#### Dataset

Source: [Olist Brazilian E-commerce Dataset (Kaggle)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

#### Bronze Layer – Raw Ingestion

Purpose:
   - Preserve source data exactly as received
   - Enable reprocessing and backfills
Characteristics:
  - Raw CSV ingestion
  - inimal transformations
  - Partitioned by ingestion_date
  - Stored as Parquet
    > Output Example: data/bronze/olist/orders/ingestion_date=YYYY-MM-DD/

#### Silver Layer – Clean & Standardized

Purpose: 
  - Prepare data for reliable analytics
  - Enforce consistency and data quality
Transformations
  - Data type normalization
  - Null handling
  - Deduplication
  - Standardized date fields
  - Referential integrity checks

> Result : Clean, analytics-safe datasets suitable for aggregation.

#### Gold Layer – Analytics & Business Logic

Purpose: 
  - Serve BI tools and business users
  - Optimize for query performance and clarity
Tables Created: fact_orders, fact_revenue, agg_daily_revenue, agg_monthly_revenue, top_products_by_revenue(These tables represent business-ready facts and aggregates, not raw operational data).

#### Orchestration with Airflow

Pipelines are organized into layered DAGs:

1. olist_bronze_dag: Ingest raw CSVs || Basic schema and null checks

2. olist_silver_dag :Transform Bronze → Silver

3. olist_gold_dag: Build fact and aggregate tables

4.publish_gold_to_postgres: Load Gold tables into PostgreSQL

Key Features
  - Modular DAG design
  - Restart-safe execution
  - Clear dependency management
  - LocalExecutor setup

#### Data Quality Strategy

Lightweight but production-realistic checks:
  - Schema validation
  - Null checks on primary keys
  - Row count sanity checks
  - Partition-level validation
  - Failures surface immediately in Airflow.

#### Analytics & Dashboard

Tool: Metabase

#### Access

Airflow UI: http://localhost:8080
Metabase UI: http://localhost:3000
Postgres: localhost:5432
  	
