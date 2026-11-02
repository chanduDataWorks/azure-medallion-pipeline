# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver → Gold
# MAGIC
# MAGIC Builds the reporting layer from silver Delta tables:
# MAGIC
# MAGIC | Gold table | Grain | Used for |
# MAGIC | --- | --- | --- |
# MAGIC | `fact_claims` | one row per claim | Power BI detail, drill-through |
# MAGIC | `plan_monthly_summary` | plan × month | PMPM, member months, denial rate |
# MAGIC | `provider_summary` | provider | provider cost and denial benchmarking |
# MAGIC
# MAGIC Gold tables are small aggregates, so they are fully rebuilt each run.

# COMMAND ----------

dbutils.widgets.dropdown("environment", "free_edition", ["free_edition", "azure"])
dbutils.widgets.text("storage_account", "stpayerclaimscd01")
dbutils.widgets.text("catalog", "workspace")

from lake_paths import lake_targets

lake = lake_targets(
    environment=dbutils.widgets.get("environment"),
    storage_account=dbutils.widgets.get("storage_account"),
    catalog=dbutils.widgets.get("catalog"),
)

# COMMAND ----------

from transforms import build_fact_claims, build_plan_monthly_summary, build_provider_summary
from delta_utils import read_delta, overwrite_delta

members = read_delta(spark, lake.silver("members"))
providers = read_delta(spark, lake.silver("providers"))
claims = read_delta(spark, lake.silver("claims"))

# COMMAND ----------

fact_claims = build_fact_claims(claims, members, providers)

overwrite_delta(fact_claims, lake.gold("fact_claims"))
overwrite_delta(build_plan_monthly_summary(fact_claims, members), lake.gold("plan_monthly_summary"))
overwrite_delta(build_provider_summary(fact_claims), lake.gold("provider_summary"))

# COMMAND ----------

# MAGIC %md ### Quick checks

# COMMAND ----------

summary = read_delta(spark, lake.gold("plan_monthly_summary"))
display(summary.orderBy("month_start", ascending=False).limit(10))

# COMMAND ----------

# reconciliation: gold fact must match silver claims exactly
gold_fact = read_delta(spark, lake.gold("fact_claims"))

silver_rows, gold_rows = claims.count(), gold_fact.count()
silver_paid = claims.agg({"paid_amount": "sum"}).collect()[0][0]
gold_paid = gold_fact.agg({"paid_amount": "sum"}).collect()[0][0]

assert silver_rows == gold_rows, f"row count mismatch: silver {silver_rows} vs gold {gold_rows}"
assert silver_paid == gold_paid, f"paid mismatch: silver {silver_paid} vs gold {gold_paid}"
print(f"reconciliation passed: {gold_rows} claims, paid {gold_paid}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power BI
# MAGIC - **Free Edition:** gold tables are already Unity Catalog tables (`<catalog>.payer.gold_*`).
# MAGIC - **Azure:** register the Delta paths as external tables, then connect Power BI through a Databricks SQL Warehouse.
