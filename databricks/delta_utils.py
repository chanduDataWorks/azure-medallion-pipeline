"""
Delta Lake helpers shared by the notebooks.

Every function takes a `target` that is either
  - a storage path  (abfss://..., /Volumes/..., dbfs:/...)  -> path-based Delta table
  - a table name    (catalog.schema.table)                  -> Unity Catalog managed table
so the same notebook code runs on full Azure and on Databricks Free Edition.
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

_PATH_PREFIXES = ("/", "abfss:", "abfs:", "dbfs:", "s3:", "s3a:", "gs:", "file:")


def is_path(target: str) -> bool:
    return target.startswith(_PATH_PREFIXES)


def delta_exists(spark: SparkSession, target: str) -> bool:
    if is_path(target):
        return DeltaTable.isDeltaTable(spark, target)
    return spark.catalog.tableExists(target)


def read_delta(spark: SparkSession, target: str) -> DataFrame:
    if is_path(target):
        return spark.read.format("delta").load(target)
    return spark.read.table(target)


def _write(df: DataFrame, target: str, mode: str, overwrite_schema: bool = False) -> None:
    writer = df.write.format("delta").mode(mode)
    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")
    else:
        writer = writer.option("mergeSchema", "true")
    if is_path(target):
        writer.save(target)
    else:
        writer.saveAsTable(target)


def upsert_delta(spark: SparkSession, df: DataFrame, target: str, key: str) -> dict:
    """
    MERGE df into the Delta table `target` on `key`.
    Creates the table on the first run. Returns row metrics for logging.
    """
    if not delta_exists(spark, target):
        _write(df, target, "overwrite", overwrite_schema=True)
        return {"target": target, "action": "created", "rows": df.count()}

    table = DeltaTable.forPath(spark, target) if is_path(target) else DeltaTable.forName(spark, target)
    (
        table.alias("t")
        .merge(df.alias("s"), f"t.{key} = s.{key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    # operationMetrics of the MERGE we just ran
    metrics = table.history(1).select("operationMetrics").collect()[0][0] or {}
    return {
        "target": target,
        "action": "merged",
        "inserted": int(metrics.get("numTargetRowsInserted", 0)),
        "updated": int(metrics.get("numTargetRowsUpdated", 0)),
    }


def overwrite_delta(df: DataFrame, target: str) -> dict:
    """Full refresh for gold aggregates (small, cheap to rebuild)."""
    _write(df, target, "overwrite", overwrite_schema=True)
    return {"target": target, "action": "overwritten"}


def append_delta(df: DataFrame, target: str) -> dict:
    """Append-only writes (quarantine)."""
    _write(df, target, "append")
    return {"target": target, "action": "appended"}
