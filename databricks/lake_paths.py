"""
Where each layer lives, for the two ways this project runs.

  azure         ADLS Gen2 + Delta paths (full Azure: ADF -> Databricks job cluster)
  free_edition  Databricks Free Edition: bronze files in a Unity Catalog volume,
                silver/gold as Unity Catalog managed Delta tables

Pure Python (no Spark) so it is unit-tested locally.
"""

from dataclasses import dataclass

ENVIRONMENTS = ("azure", "free_edition")
SCHEMA = "payer"
VOLUME = "lakehouse"


@dataclass(frozen=True)
class LakeTargets:
    environment: str
    bronze_root: str          # folder holding <entity>/ingest_date=YYYY-MM-DD/
    silver_prefix: str
    gold_prefix: str

    def silver(self, name: str) -> str:
        return f"{self.silver_prefix}{name}"

    def gold(self, name: str) -> str:
        return f"{self.gold_prefix}{name}"

    def bronze(self, entity: str, ingest_date: str) -> str:
        return f"{self.bronze_root}/{entity}/ingest_date={ingest_date}"


def lake_targets(environment: str, storage_account: str = "", catalog: str = "workspace") -> LakeTargets:
    if environment == "azure":
        if not storage_account:
            raise ValueError("storage_account is required when environment='azure'")
        base = f"dfs.core.windows.net/{SCHEMA}"
        return LakeTargets(
            environment=environment,
            bronze_root=f"abfss://bronze@{storage_account}.{base}",
            silver_prefix=f"abfss://silver@{storage_account}.{base}/",
            gold_prefix=f"abfss://gold@{storage_account}.{base}/",
        )

    if environment == "free_edition":
        return LakeTargets(
            environment=environment,
            bronze_root=f"/Volumes/{catalog}/{SCHEMA}/{VOLUME}/bronze/{SCHEMA}",
            silver_prefix=f"{catalog}.{SCHEMA}.silver_",
            gold_prefix=f"{catalog}.{SCHEMA}.gold_",
        )

    raise ValueError(f"environment must be one of {ENVIRONMENTS}, got {environment!r}")
