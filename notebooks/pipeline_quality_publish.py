# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./_config

# COMMAND ----------
from pyspark.sql import functions as F

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
silver_table = table_name(catalog, schema, config["silver_table"])
gold_table = table_name(catalog, schema, config["gold_table"])

print(f"[QUALITY] pipeline_id={pipeline_id} fail_mode={fail_mode}")

# COMMAND ----------
def fail_if(count: int, message: str):
    if count > 0:
        raise ValueError(f"{message} (rows={count})")


# COMMAND ----------
silver_df = spark.table(silver_table)

if pipeline_id == "retail_orders":
    null_ts = silver_df.filter(F.col("order_ts").isNull()).count()
    fail_if(null_ts, "Schema drift detected: order_ts missing")
    negative_amt = silver_df.filter(F.col("amount") < 0).count()
    fail_if(negative_amt, "Invalid order amount")

elif pipeline_id == "payments_recon":
    dup_txn = (
        silver_df.groupBy("txn_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    fail_if(dup_txn, "Duplicate transaction ids detected")

elif pipeline_id == "inventory_snapshot":
    negative_on_hand = silver_df.filter(F.col("on_hand") < 0).count()
    fail_if(negative_on_hand, "Negative on_hand inventory detected")

elif pipeline_id == "customer_360":
    invalid_email = silver_df.filter(
        F.col("email").isNull() | (~F.col("email").contains("@"))
    ).count()
    fail_if(invalid_email, "Invalid or missing email detected")

elif pipeline_id == "clickstream_sessions":
    invalid_ts = silver_df.filter(F.col("event_ts").isNull()).count()
    fail_if(invalid_ts, "Invalid event_ts detected")

elif pipeline_id == "finance_close":
    missing_fx = silver_df.filter(F.col("amount_usd").isNull()).count()
    fail_if(missing_fx, "Missing FX rates for currency conversion")

else:
    # No specific quality checks for this pipeline_id
    print(f"[QUALITY] No specific quality checks configured for pipeline_id: {pipeline_id}")

print(f"[QUALITY] Writing gold table: {gold_table}")
silver_df.write.format("delta").mode("overwrite").saveAsTable(gold_table)
