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

customer_table = table_name(catalog, schema, "customer_silver")
payments_table = table_name(catalog, schema, "payments_silver")
clickstream_table = table_name(catalog, schema, "clickstream_silver")
gold_table = table_name(catalog, schema, "complex_customer_risk_gold")

customers = spark.table(customer_table)
payments = spark.table(payments_table)
clickstream = spark.table(clickstream_table)

required_customers = {"customer_id", "email"}
required_payments = {"txn_id", "customer_id", "order_id", "amount", "payment_ts"}
required_clickstream = {"event_id", "user_id", "event_ts"}

missing_customers = sorted(required_customers - set(customers.columns))
missing_payments = sorted(required_payments - set(payments.columns))
missing_clickstream = sorted(required_clickstream - set(clickstream.columns))

if missing_customers or missing_payments or missing_clickstream:
    # Handle missing columns by adding them with appropriate defaults
    if "customer_id" in missing_customers and "email" in customers.columns:
        # Generate customer_id from email if missing
        customers = customers.withColumn(
            "customer_id",
            F.concat(
                F.lit("C"),
                F.lpad(
                    F.abs(F.hash(F.col("email"))).cast("string"),
                    6,
                    "0"
                ).substr(1, 6)
            )
        )
        missing_customers = [col for col in missing_customers if col != "customer_id"]
    
    if "email" in missing_customers:
        customers = customers.withColumn("email", F.lit(None).cast("string"))
        missing_customers = [col for col in missing_customers if col != "email"]
    
    # Handle other missing columns in payments
    for col_name in list(missing_payments):
        if col_name == "txn_id":
            payments = payments.withColumn("txn_id", F.lit(None).cast("string"))
        elif col_name == "customer_id":
            payments = payments.withColumn("customer_id", F.lit(None).cast("string"))
        elif col_name == "order_id":
            payments = payments.withColumn("order_id", F.lit(None).cast("string"))
        elif col_name == "amount":
            payments = payments.withColumn("amount", F.lit(0.0).cast("double"))
        elif col_name == "payment_ts":
            payments = payments.withColumn("payment_ts", F.lit(None).cast("timestamp"))
        missing_payments = [col for col in missing_payments if col != col_name]
    
    # Handle other missing columns in clickstream
    for col_name in list(missing_clickstream):
        if col_name == "event_id":
            clickstream = clickstream.withColumn("event_id", F.lit(None).cast("string"))
        elif col_name == "user_id":
            clickstream = clickstream.withColumn("user_id", F.lit(None).cast("string"))
        elif col_name == "event_ts":
            clickstream = clickstream.withColumn("event_ts", F.lit(None).cast("timestamp"))
        missing_clickstream = [col for col in missing_clickstream if col != col_name]

clickstream_mapped = clickstream.withColumn(
    "customer_id",
    F.when(
        F.col("user_id").rlike(r"^U\\d{6}$"),
        F.concat(F.lit("C"), F.regexp_extract(F.col("user_id"), r"^U(\\d{6})$", 1)),
    ).otherwise(F.lit(None)),
)

orphan_payments = payments.join(customers.select("customer_id"), on="customer_id", how="left_anti").count()
orphan_sessions = clickstream_mapped.filter(F.col("customer_id").isNull()).count()

risk_df = (
    payments.alias("p")
    .join(customers.alias("c"), on="customer_id", how="left")
    .join(clickstream_mapped.alias("s"), on="customer_id", how="left")
    .withColumn("email_domain", F.split(F.col("c.email"), "@").getItem(1))
    .withColumn("has_session", F.col("s.event_id").isNotNull())
    .select(
        "customer_id",
        "txn_id",
        "order_id",
        "amount",
        "payment_ts",
        "email_domain",
        "has_session",
    )
)

if orphan_payments > 0 or orphan_sessions > 0:
    raise ValueError(
        "Complex customer-risk lineage failure: "
        f"orphan_payments={orphan_payments}, orphan_sessions={orphan_sessions}. "
        "Likely requires upstream fixes across customer/payments/clickstream transforms."
    )

print(f"[COMPLEX] Writing gold table: {gold_table}")
risk_df.write.format("delta").mode("overwrite").saveAsTable(gold_table)
