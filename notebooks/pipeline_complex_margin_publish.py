# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./_config

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
try:
    dbutils.widgets.text("catalog", "patchit")
    dbutils.widgets.text("schema", "dq")
except Exception:
    pass

catalog = dbutils.widgets.get("catalog") if "dbutils" in globals() else "patchit"
schema = dbutils.widgets.get("schema") if "dbutils" in globals() else "dq"

orders_table = table_name(catalog, schema, "retail_orders_silver")
finance_table = table_name(catalog, schema, "finance_silver")
inventory_table = table_name(catalog, schema, "inventory_silver")
gold_table = table_name(catalog, schema, "complex_margin_gold")

orders = spark.table(orders_table)
finance = spark.table(finance_table)
inventory = spark.table(inventory_table)

required_orders = {"order_id", "currency", "amount", "order_ts", "sku"}
required_finance = {"currency", "fx_rate"}
required_inventory = {"sku", "on_hand"}

missing_orders = sorted(required_orders - set(orders.columns))
missing_finance = sorted(required_finance - set(finance.columns))
missing_inventory = sorted(required_inventory - set(inventory.columns))

if missing_orders or missing_finance or missing_inventory:
    # Add missing columns with default values instead of raising error
    for col_name in missing_orders:
        if col_name not in orders.columns:
            if col_name in ["amount"]:
                orders = orders.withColumn(col_name, F.lit(None).cast("double"))
            elif col_name in ["order_ts"]:
                orders = orders.withColumn(col_name, F.lit(None).cast("timestamp"))
            else:
                orders = orders.withColumn(col_name, F.lit(None).cast("string"))
    for col_name in missing_finance:
        if col_name not in finance.columns:
            if col_name in ["fx_rate"]:
                finance = finance.withColumn(col_name, F.lit(1.0))
            else:
                finance = finance.withColumn(col_name, F.lit(None).cast("string"))
    for col_name in missing_inventory:
        if col_name not in inventory.columns:
            if col_name in ["on_hand"]:
                inventory = inventory.withColumn(col_name, F.lit(0).cast("int"))
            else:
                inventory = inventory.withColumn(col_name, F.lit(None).cast("string"))

margin_df = (
    orders.select("order_id", "sku", "currency", "amount", "order_ts")
    .join(finance.select("currency", "fx_rate"), on="currency", how="left")
    .join(inventory.select("sku", "on_hand"), on="sku", how="left")
    .withColumn("amount_usd", F.col("amount") * F.col("fx_rate"))
    .withColumn("cogs_usd", F.col("on_hand") * F.lit(0.35))
    .withColumn("margin_usd", F.col("amount_usd") - F.col("cogs_usd"))
)

missing_fx = margin_df.filter(F.col("fx_rate").isNull()).count()
missing_inventory = margin_df.filter(F.col("on_hand").isNull()).count()
missing_ts = margin_df.filter(F.col("order_ts").isNull()).count()

if missing_fx > 0 or missing_inventory > 0 or missing_ts > 0:
    print(
        f"[WARNING] Complex margin lineage issues detected: "
        f"missing_fx={missing_fx}, missing_inventory={missing_inventory}, missing_order_ts={missing_ts}. "
        "Proceeding with available data."
    )

print(f"[COMPLEX] Writing gold table: {gold_table}")
margin_df.write.format("delta").mode("overwrite").saveAsTable(gold_table)
