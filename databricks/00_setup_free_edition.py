# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup (Databricks Free Edition only)
# MAGIC
# MAGIC Creates the Unity Catalog objects the pipeline uses and the bronze folders to upload into.
# MAGIC Run this **once**. On full Azure you don't need it - ADF writes bronze to ADLS.
# MAGIC
# MAGIC | Object | Purpose |
# MAGIC | --- | --- |
# MAGIC | schema `<catalog>.payer` | holds silver and gold tables |
# MAGIC | volume `<catalog>.payer.lakehouse` | holds bronze files (the Parquet ADF produced) |

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("ingest_date", "")
dbutils.widgets.dropdown("load_sample_csv", "no", ["no", "yes"])

catalog = dbutils.widgets.get("catalog")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.payer")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.payer.lakehouse")

bronze_root = f"/Volumes/{catalog}/payer/lakehouse/bronze/payer"
for entity in ["members", "providers", "claims"]:
    dbutils.fs.mkdirs(f"{bronze_root}/{entity}")

print(f"Schema and volume ready. Bronze root:\n  {bronze_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Put the bronze files in place
# MAGIC
# MAGIC **Normal path:** upload the 3 Parquet files ADF wrote to ADLS, keeping the folder layout:
# MAGIC ```
# MAGIC bronze/payer/members/ingest_date=YYYY-MM-DD/members.parquet
# MAGIC bronze/payer/providers/ingest_date=YYYY-MM-DD/providers.parquet
# MAGIC bronze/payer/claims/ingest_date=YYYY-MM-DD/claims.parquet
# MAGIC ```
# MAGIC Catalog → `payer` → `lakehouse` → open each entity folder → create the `ingest_date=...` folder → **Upload to this volume**.
# MAGIC
# MAGIC **Fallback (no ADF):** set `load_sample_csv = yes` and `ingest_date`, then run the next cell.
# MAGIC It copies the repo's `sample_data/*.csv` into bronze so the notebooks can run on their own.

# COMMAND ----------

import os
import shutil

if dbutils.widgets.get("load_sample_csv") == "yes":
    ingest_date = dbutils.widgets.get("ingest_date")
    if not ingest_date:
        raise ValueError("Set ingest_date (yyyy-MM-dd) to load the sample CSVs")

    sample_dir = os.path.abspath(os.path.join(os.getcwd(), "..", "sample_data"))
    for entity in ["members", "providers", "claims"]:
        target_dir = f"{bronze_root}/{entity}/ingest_date={ingest_date}"
        os.makedirs(target_dir, exist_ok=True)
        shutil.copy(os.path.join(sample_dir, f"raw_{entity}.csv"), f"{target_dir}/{entity}.csv")
        print(f"copied raw_{entity}.csv -> {target_dir}")
else:
    print("Skipped: load_sample_csv = no (upload the ADF Parquet files instead)")

# COMMAND ----------

display(dbutils.fs.ls(f"{bronze_root}/claims"))
