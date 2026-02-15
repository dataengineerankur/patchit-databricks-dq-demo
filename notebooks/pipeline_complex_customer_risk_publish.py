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

# Handle missing columns by adding them with appropriate defaults
for col in missing_customers:
    if col == "customer_id":
        customers = customers.withColumn("customer_id", F.lit(None).cast("string"))
    elif col == "email":
        customers = customers.withColumn("email", F.lit(None).cast("string"))

for col in missing_payments:
    if col in ["customer_id", "order_id", "txn_id"]:
        payments = payments.withColumn(col, F.lit(None).cast("string"))
    elif col == "amount":
        payments = payments.withColumn(col, F.lit(0.0).cast("double"))
    elif col == "payment_ts":
        payments = payments.withColumn(col, F.lit(None).cast("timestamp"))

for col in missing_clickstream:
    if col in ["event_id", "user_id"]:
        clickstream = clickstream.withColumn(col, F.lit(None).cast("string"))
    elif col == "event_ts":
        clickstream = clickstream.withColumn(col, F.lit(None).cast("timestamp"))

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
    raise ValueError(
        "Upstream contract drift detected for complex customer risk flow: "
        f"customer_missing={missing_customers}, payments_missing={missing_payments}, clickstream_missing={missing_clickstream}"
    )

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
