# Databricks notebook source
# COMMAND ----------
PIPELINES = {
    "retail_orders": {
        "description": "Retail orders ingestion and enrichment",
        "raw_table": "retail_orders_raw",
        "silver_table": "retail_orders_silver",
        "gold_table": "retail_orders_gold",
        "fail_mode": "schema_drift_order_ts",
    },
    "payments_recon": {
        "description": "Payments reconciliation and fraud flags",
        "raw_table": "payments_raw",
        "silver_table": "payments_silver",
        "gold_table": "payments_gold",
        "fail_mode": "duplicate_txn_id",
    },
    "inventory_snapshot": {
        "description": "Inventory snapshots and low-stock alerts",
        "raw_table": "inventory_raw",
        "silver_table": "inventory_silver",
        "gold_table": "inventory_gold",
        "fail_mode": "negative_on_hand",
    },
    "customer_360": {
        "description": "Customer 360 profile assembly",
        "raw_table": "customer_raw",
        "silver_table": "customer_silver",
        "gold_table": "customer_gold",
        "fail_mode": "missing_email",
    },
    "clickstream_sessions": {
        "description": "Clickstream sessionization",
        "raw_table": "clickstream_raw",
        "silver_table": "clickstream_silver",
        "gold_table": "clickstream_gold",
        "fail_mode": "invalid_event_ts",
    },
    "finance_close": {
        "description": "Finance close and FX normalization",
        "raw_table": "finance_raw",
        "silver_table": "finance_silver",
        "gold_table": "finance_gold",
        "fail_mode": "missing_fx_rate",
    },
    "patchit_airflow_issue_001": {
        "description": "AF001: Primary key validation and deduplication",
        "raw_table": "af001_raw",
        "silver_table": "af001_silver",
        "gold_table": "af001_gold",
        "fail_mode": "missing_pk",
    },
}


def get_pipeline_config(pipeline_id: str) -> dict:
    if pipeline_id not in PIPELINES:
        raise ValueError(
            f"Unknown pipeline_id '{pipeline_id}'. Valid values: {list(PIPELINES.keys())}"
        )
    return PIPELINES[pipeline_id]


def table_name(catalog: str, schema: str, base_name: str) -> str:
    return f"{catalog}.{schema}.{base_name}"


def ensure_schema(catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
