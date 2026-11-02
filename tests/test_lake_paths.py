"""Tests for lake_paths.py and delta_utils.is_path - no Spark session needed."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "databricks"))

from delta_utils import is_path  # noqa: E402
from lake_paths import lake_targets  # noqa: E402


def test_free_edition_uses_volume_and_uc_tables():
    lake = lake_targets("free_edition", catalog="workspace")
    assert lake.bronze("claims", "2026-11-02") == \
        "/Volumes/workspace/payer/lakehouse/bronze/payer/claims/ingest_date=2026-11-02"
    assert lake.silver("claims") == "workspace.payer.silver_claims"
    assert lake.gold("plan_monthly_summary") == "workspace.payer.gold_plan_monthly_summary"
    assert is_path(lake.bronze_root)
    assert not is_path(lake.silver("claims"))


def test_azure_uses_adls_paths():
    lake = lake_targets("azure", storage_account="stdemo")
    assert lake.bronze("members", "2026-11-02") == \
        "abfss://bronze@stdemo.dfs.core.windows.net/payer/members/ingest_date=2026-11-02"
    assert lake.silver("claims") == "abfss://silver@stdemo.dfs.core.windows.net/payer/claims"
    assert lake.gold("fact_claims") == "abfss://gold@stdemo.dfs.core.windows.net/payer/fact_claims"
    assert is_path(lake.silver("claims"))


def test_azure_requires_storage_account():
    with pytest.raises(ValueError):
        lake_targets("azure", storage_account="")


def test_unknown_environment_rejected():
    with pytest.raises(ValueError):
        lake_targets("aws")
