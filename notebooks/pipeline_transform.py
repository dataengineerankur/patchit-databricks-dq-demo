# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./_config

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------
try:
    dbutils.widgets.text("pipeline_id", "retail_orders")
    dbutils.widgets.text("catalog", "patchit")
    dbutils.widgets.text("schema", "dq")
    dbutils.widgets.text("fail_mode", "")
except Exception:
    pass

pipeline_id = dbutils.widgets.get("pipeline_id") if "dbutils" in globals() else "retail_orders"
catalog = dbutils.widgets.get("catalog") if "dbutils" in globals() else "patchit"
schema = dbutils.widgets.get("schema") if "dbutils" in globals() else "dq"
fail_mode = dbutils.widgets.get("fail_mode") if "dbutils" in globals() else ""

config = get_pipeline_config(pipeline_id)
raw_table = table_name(catalog, schema, config["raw_table"])
silver_table = table_name(catalog, schema, config["silver_table"])

print(f"[TRANSFORM] pipeline_id={pipeline_id} fail_mode={fail_mode}")

# COMMAND ----------
def ensure_column(df, col_name, default_expr):
    if col_name in df.columns:
        return df
    return df.withColumn(col_name, default_expr)


def transform_retail(df):
    df = ensure_column(df, "order_ts", F.lit(None).cast("timestamp"))
    df = df.withColumn("order_date", F.to_date("order_ts"))
    df = df.withColumn(
        "amount_usd",
        F.when(F.col("currency") == "EUR", F.col("amount") * F.lit(1.1))
        .when(F.col("currency") == "GBP", F.col("amount") * F.lit(1.3))
        .otherwise(F.col("amount")),
    )
    return df


def transform_payments(df):
    df = df.withColumn("payment_ts", F.to_timestamp("payment_ts"))
    df = df.withColumn("is_failed", F.col("status") == F.lit("DECLINED"))
    return df


def transform_inventory(df):
    df = df.withColumn("on_hand", F.col("on_hand").cast("int"))
    df = df.withColumn("is_low_stock", F.col("on_hand") < F.lit(10))
    return df


def transform_customer(df):
    df = ensure_column(df, "customer_id", F.lit(None).cast("string"))
    df = ensure_column(df, "email", F.lit(None).cast("string"))
    df = df.withColumn("email", F.lower("email"))
    df = df.withColumn("email_domain", F.split(F.col("email"), "@").getItem(1))
    return df


def transform_clickstream(df):
    df = df.withColumn("event_ts", F.to_timestamp("event_ts", "yyyy-MM-dd HH:mm:ss"))
    w = Window.partitionBy("user_id").orderBy("event_ts")
    df = df.withColumn("prev_ts", F.lag("event_ts").over(w))
    df = df.withColumn(
        "new_session",
        (F.col("prev_ts").isNull())
        | (F.col("event_ts").cast("long") - F.col("prev_ts").cast("long") > F.lit(1800)),
    )
    df = df.withColumn("session_id", F.sum(F.col("new_session").cast("int")).over(w))
    return df.drop("prev_ts")


def transform_finance(df):
    fx_rates = spark.createDataFrame(
        [
            ("USD", 1.0),
            ("EUR", 1.1),
            ("JPY", 0.009),
        ],
        ["currency", "fx_rate"],
    )
    df = df.join(fx_rates, on="currency", how="left")
    df = df.withColumn("amount_usd", F.col("amount_local") * F.col("fx_rate"))
    return df


# COMMAND ----------
raw_df = spark.table(raw_table)

if pipeline_id == "retail_orders":
    silver_df = transform_retail(raw_df)
elif pipeline_id == "payments_recon":
    silver_df = transform_payments(raw_df)
elif pipeline_id == "inventory_snapshot":
    silver_df = transform_inventory(raw_df)
elif pipeline_id == "customer_360":
    silver_df = transform_customer(raw_df)
elif pipeline_id == "clickstream_sessions":
    silver_df = transform_clickstream(raw_df)
elif pipeline_id == "finance_close":
    silver_df = transform_finance(raw_df)
else:
    raise ValueError(f"Unsupported pipeline_id: {pipeline_id}")

print(f"[TRANSFORM] Writing silver table: {silver_table}")
silver_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)
