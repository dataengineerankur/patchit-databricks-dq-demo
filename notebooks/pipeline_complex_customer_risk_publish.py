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

# Handle missing columns gracefully
if "customer_id" in missing_customers:
    # Try to derive customer_id from email (format: userN@example.com -> C{N:06d})
    if "email" in customers.columns:
        customers = customers.withColumn(
            "customer_id",
            F.concat(
                F.lit("C"),
                F.lpad(
                    F.regexp_extract(F.col("email"), r"user(\d+)@", 1),
                    6,
                    "0"
                )
            )
        )
    else:
        customers = customers.withColumn("customer_id", F.lit(None).cast("string"))

if "email" in missing_customers:
    customers = customers.withColumn("email", F.lit(None).cast("string"))

if "customer_id" in missing_payments:
    payments = payments.withColumn("customer_id", F.lit(None).cast("string"))

# Re-check for any remaining missing columns
missing_customers_final = sorted(required_customers - set(customers.columns))
missing_payments_final = sorted(required_payments - set(payments.columns))
missing_clickstream_final = sorted(required_clickstream - set(clickstream.columns))

if missing_customers_final or missing_payments_final or missing_clickstream_final:
    raise ValueError(
        "Upstream contract drift detected for complex customer risk flow: "
        f"customer_missing={missing_customers_final}, payments_missing={missing_payments_final}, clickstream_missing={missing_clickstream_final}"
    )

clickstream_mapped = clickstream.withColumn(
    "customer_id",
    F.when(
        F.col("user_id").rlike(r"^U\d{6}$"),
        F.concat(F.lit("C"), F.regexp_extract(F.col("user_id"), r"^U(\d{6})$", 1)),
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
