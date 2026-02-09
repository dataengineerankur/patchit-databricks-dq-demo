# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./_config

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------
# Parameters
try:
    dbutils.widgets.text("pipeline_id", "retail_orders")
    dbutils.widgets.text("catalog", "patchit")
    dbutils.widgets.text("schema", "dq")
    dbutils.widgets.text("row_count", "5000")
    dbutils.widgets.text("fail_mode", "")
except Exception:
    pass

pipeline_id = dbutils.widgets.get("pipeline_id") if "dbutils" in globals() else "retail_orders"
catalog = dbutils.widgets.get("catalog") if "dbutils" in globals() else "patchit"
schema = dbutils.widgets.get("schema") if "dbutils" in globals() else "dq"
row_count = int(dbutils.widgets.get("row_count")) if "dbutils" in globals() else 5000
fail_mode = dbutils.widgets.get("fail_mode") if "dbutils" in globals() else ""

config = get_pipeline_config(pipeline_id)
ensure_schema(catalog, schema)
raw_table = table_name(catalog, schema, config["raw_table"])

print(f"[INGEST] pipeline_id={pipeline_id} fail_mode={fail_mode} rows={row_count}")

# COMMAND ----------
def generate_retail_orders(n: int, fail_mode: str):
    df = (
        spark.range(0, n)
        .withColumn("order_id", F.concat(F.lit("O"), F.lpad(F.col("id").cast("string"), 8, "0")))
        .withColumn("customer_id", F.concat(F.lit("C"), F.lpad((F.col("id") % 1000).cast("string"), 6, "0")))
        .withColumn("amount", F.round(F.rand(42) * 200 + 5, 2))
        .withColumn(
            "currency",
            F.when(F.col("id") % 3 == 0, F.lit("USD"))
            .when(F.col("id") % 3 == 1, F.lit("EUR"))
            .otherwise(F.lit("GBP")),
        )
        .withColumn("channel", F.when(F.col("id") % 2 == 0, F.lit("web")).otherwise(F.lit("store")))
        .withColumn(
            "status",
            F.when(F.col("id") % 10 == 0, F.lit("CANCELLED")).otherwise(F.lit("COMPLETED")),
        )
        .withColumn("order_ts", F.current_timestamp())
        .drop("id")
    )
    if fail_mode == "schema_drift_order_ts":
        df = df.withColumnRenamed("order_ts", "order_time")
    return df


def generate_payments(n: int, fail_mode: str):
    df = (
        spark.range(0, n)
        .withColumn("txn_id", F.concat(F.lit("T"), F.lpad(F.col("id").cast("string"), 8, "0")))
        .withColumn("order_id", F.concat(F.lit("O"), F.lpad((F.col("id") % 2000).cast("string"), 8, "0")))
        .withColumn("amount", F.round(F.rand(7) * 200 + 3, 2))
        .withColumn("payment_ts", F.current_timestamp())
        .withColumn(
            "method",
            F.when(F.col("id") % 3 == 0, F.lit("card"))
            .when(F.col("id") % 3 == 1, F.lit("paypal"))
            .otherwise(F.lit("wallet")),
        )
        .withColumn(
            "status",
            F.when(F.col("id") % 20 == 0, F.lit("DECLINED")).otherwise(F.lit("APPROVED")),
        )
        .drop("id")
    )
    if fail_mode == "duplicate_txn_id":
        df = df.withColumn(
            "txn_id",
            F.when(F.col("order_id") == "O00000000", F.lit("DUP-0001")).otherwise(F.col("txn_id")),
        )
    return df


def generate_inventory(n: int, fail_mode: str):
    df = (
        spark.range(0, n)
        .withColumn("sku", F.concat(F.lit("SKU-"), F.lpad((F.col("id") % 500).cast("string"), 5, "0")))
        .withColumn("warehouse_id", F.concat(F.lit("WH-"), F.lpad((F.col("id") % 10).cast("string"), 2, "0")))
        .withColumn("on_hand", (F.rand(11) * 100).cast("int"))
        .withColumn("snapshot_ts", F.current_timestamp())
        .drop("id")
    )
    if fail_mode == "negative_on_hand":
        df = df.withColumn(
            "on_hand", F.when(F.col("sku") == "SKU-00000", F.lit(-5)).otherwise(F.col("on_hand"))
        )
    return df


def generate_customer(n: int, fail_mode: str):
    df = (
        spark.range(0, n)
        .withColumn("customer_id", F.concat(F.lit("C"), F.lpad(F.col("id").cast("string"), 6, "0")))
        .withColumn("email", F.concat(F.lit("user"), F.col("id"), F.lit("@example.com")))
        .withColumn("signup_ts", F.current_timestamp())
        .withColumn(
            "region",
            F.when(F.col("id") % 4 == 0, F.lit("NA"))
            .when(F.col("id") % 4 == 1, F.lit("EMEA"))
            .when(F.col("id") % 4 == 2, F.lit("APAC"))
            .otherwise(F.lit("LATAM")),
        )
        .drop("id")
    )
    if fail_mode == "missing_email":
        df = df.withColumnRenamed("email", "contact_email")
    return df


def generate_clickstream(n: int, fail_mode: str):
    df = (
        spark.range(0, n)
        .withColumn("event_id", F.concat(F.lit("E"), F.lpad(F.col("id").cast("string"), 8, "0")))
        .withColumn("user_id", F.concat(F.lit("U"), F.lpad((F.col("id") % 2000).cast("string"), 6, "0")))
        .withColumn("event_ts", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        .withColumn(
            "page",
            F.when(F.col("id") % 3 == 0, F.lit("/home"))
            .when(F.col("id") % 3 == 1, F.lit("/product"))
            .otherwise(F.lit("/checkout")),
        )
        .withColumn("referrer", F.when(F.col("id") % 2 == 0, F.lit("ad"))
                    .otherwise(F.lit("organic")))
        .drop("id")
    )
    if fail_mode == "invalid_event_ts":
        df = df.withColumn(
            "event_ts",
            F.when(F.col("event_id") == "E00000000", F.lit("2025-99-99 25:61:00")).otherwise(F.col("event_ts")),
        )
    return df


def generate_finance(n: int, fail_mode: str):
    df = (
        spark.range(0, n)
        .withColumn("ledger_id", F.concat(F.lit("L"), F.lpad(F.col("id").cast("string"), 8, "0")))
        .withColumn("account_id", F.concat(F.lit("A"), F.lpad((F.col("id") % 500).cast("string"), 5, "0")))
        .withColumn("amount_local", F.round(F.rand(23) * 10000 + 50, 2))
        .withColumn(
            "currency",
            F.when(F.col("id") % 3 == 0, F.lit("USD"))
            .when(F.col("id") % 3 == 1, F.lit("EUR"))
            .otherwise(F.lit("JPY")),
        )
        .withColumn("as_of_date", F.current_date())
        .drop("id")
    )
    if fail_mode == "missing_fx_rate":
        df = df.withColumn(
            "currency",
            F.when(F.col("ledger_id") == "L00000000", F.lit("ZZZ")).otherwise(F.col("currency")),
        )
    return df


# COMMAND ----------
if pipeline_id == "retail_orders":
    raw_df = generate_retail_orders(row_count, fail_mode)
elif pipeline_id == "payments_recon":
    raw_df = generate_payments(row_count, fail_mode)
elif pipeline_id == "inventory_snapshot":
    raw_df = generate_inventory(row_count, fail_mode)
elif pipeline_id == "customer_360":
    raw_df = generate_customer(row_count, fail_mode)
elif pipeline_id == "clickstream_sessions":
    raw_df = generate_clickstream(row_count, fail_mode)
elif pipeline_id == "finance_close":
    raw_df = generate_finance(row_count, fail_mode)
else:
    raise ValueError(f"Unsupported pipeline_id: {pipeline_id}")

print(f"[INGEST] Writing raw table: {raw_table}")
raw_df.write.format("delta").mode("overwrite").saveAsTable(raw_table)
