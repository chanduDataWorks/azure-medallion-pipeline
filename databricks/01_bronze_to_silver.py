# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze → Silver
# MAGIC
# MAGIC Reads the bronze files for one `ingest_date` (Parquet from ADF, or CSV), then:
# MAGIC 1. cleans and types each entity (`transforms.py`)
# MAGIC 2. de-duplicates resubmitted claims (latest `loaded_at` wins)
# MAGIC 3. sends bad claim rows to a **quarantine** table with a reason
# MAGIC 4. MERGEs clean rows into **silver** Delta tables (upsert on primary key)
# MAGIC
# MAGIC | `environment` | Bronze | Silver |
# MAGIC | --- | --- | --- |
# MAGIC | `azure` (called by ADF) | ADLS `bronze` container | Delta paths in ADLS `silver` container |
# MAGIC | `free_edition` | Unity Catalog volume | Unity Catalog tables `<catalog>.payer.silver_*` |

# COMMAND ----------

dbutils.widgets.dropdown("environment", "free_edition", ["free_edition", "azure"])
dbutils.widgets.text("storage_account", "stpayerclaimscd01")
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("ingest_date", "")

from lake_paths import lake_targets

lake = lake_targets(
    environment=dbutils.widgets.get("environment"),
    storage_account=dbutils.widgets.get("storage_account"),
    catalog=dbutils.widgets.get("catalog"),
)

# COMMAND ----------

def latest_ingest_date(entity_root: str) -> str:
    """Newest ingest_date=YYYY-MM-DD partition under a folder."""
    dates = [f.name.rstrip("/").split("=", 1)[1]
             for f in dbutils.fs.ls(entity_root) if f.name.startswith("ingest_date=")]
    if not dates:
        raise ValueError(f"No ingest_date=... folders found under {entity_root}")
    return max(dates)


ingest_date = dbutils.widgets.get("ingest_date") or latest_ingest_date(f"{lake.bronze_root}/claims")
print(f"environment={lake.environment}\ningest_date={ingest_date}\nbronze={lake.bronze_root}")

# COMMAND ----------

from pyspark.sql import functions as F

from transforms import clean_members, clean_providers, clean_claims, split_valid_claims
from delta_utils import upsert_delta, append_delta


def read_bronze(entity: str):
    """Bronze is Parquet when ADF produced it, CSV when sample files were loaded."""
    folder = lake.bronze(entity, ingest_date)
    names = [f.name for f in dbutils.fs.ls(folder)]
    if any(n.endswith(".csv") for n in names):
        return spark.read.option("header", True).option("inferSchema", False).csv(folder)
    return spark.read.parquet(folder)

# COMMAND ----------

# MAGIC %md ### Members and providers

# COMMAND ----------

members = clean_members(read_bronze("members"))
providers = clean_providers(read_bronze("providers"))

results = [
    upsert_delta(spark, members, lake.silver("members"), "member_id"),
    upsert_delta(spark, providers, lake.silver("providers"), "provider_npi"),
]

# COMMAND ----------

# MAGIC %md ### Claims with data quality gate

# COMMAND ----------

claims = clean_claims(read_bronze("claims"))
valid_claims, rejected_claims = split_valid_claims(claims)

results.append(upsert_delta(spark, valid_claims, lake.silver("claims"), "claim_id"))

rejected_count = rejected_claims.count()
if rejected_count > 0:
    append_delta(rejected_claims.withColumn("ingest_date", F.lit(ingest_date)),
                 lake.silver("quarantine_claims"))

print(f"rejected claims sent to quarantine: {rejected_count}")
for r in results:
    print(r)

# COMMAND ----------

# Return a small summary to the caller (visible in the ADF activity output)
import json
dbutils.notebook.exit(json.dumps({"ingest_date": ingest_date,
                                  "rejected_claims": rejected_count,
                                  "results": results}, default=str))
