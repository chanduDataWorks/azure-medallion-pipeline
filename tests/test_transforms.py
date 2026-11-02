"""
Unit tests for databricks/transforms.py - run locally, no Azure needed.

    pip install -r requirements-dev.txt
    pytest -q
"""

import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "databricks"))

from transforms import (  # noqa: E402
    build_fact_claims,
    build_plan_monthly_summary,
    clean_claims,
    clean_members,
    clean_providers,
    split_valid_claims,
)

CLAIM_COLS = ["claim_id", "member_id", "provider_npi", "claim_type", "procedure_code",
              "primary_dx_code", "service_date", "received_date", "paid_date", "billed_amount",
              "allowed_amount", "paid_amount", "claim_status", "denial_reason", "loaded_at"]


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


def claim(claim_id="C1", member_id="M1", status="PAID", billed="100.00", paid="60.00",
          loaded_at="2025-01-02 00:00:00", service_date="2025-01-01"):
    return (claim_id, member_id, "1234567890", "PROF", "99213", "I10", service_date,
            "2025-01-02", "2025-01-10", billed, "70.00", paid, status, "", loaded_at)


def test_clean_claims_keeps_latest_resubmission(spark):
    df = spark.createDataFrame([
        claim(status="DENIED", paid="0.00", loaded_at="2025-01-02 00:00:00"),
        claim(status="PAID", paid="63.00", loaded_at="2025-02-05 00:00:00"),
    ], CLAIM_COLS)

    out = clean_claims(df).collect()

    assert len(out) == 1
    assert out[0]["claim_status"] == "PAID"
    assert out[0]["paid_amount"] == Decimal("63.00")


def test_clean_claims_standardises_status_and_blanks(spark):
    df = spark.createDataFrame([claim(status="  paid ")], CLAIM_COLS)
    row = clean_claims(df).collect()[0]
    assert row["claim_status"] == "PAID"
    assert row["denial_reason"] is None
    assert row["service_date"] == date(2025, 1, 1)


def test_split_valid_claims_quarantines_bad_rows(spark):
    df = spark.createDataFrame([
        claim(claim_id="C1"),
        claim(claim_id="C2", member_id=""),
        claim(claim_id="C3", billed="abc"),
        claim(claim_id="C4", status="UNKNOWN"),
        claim(claim_id="C5", paid="-5.00"),
    ], CLAIM_COLS)

    valid, rejected = split_valid_claims(clean_claims(df))
    reasons = {r["claim_id"]: r["reject_reason"] for r in rejected.collect()}

    assert [r["claim_id"] for r in valid.collect()] == ["C1"]
    assert reasons == {
        "C2": "missing member_id",
        "C3": "invalid amount",
        "C4": "unknown claim_status",
        "C5": "negative amount",
    }


def test_clean_members_plan_type_and_active_flag(spark):
    cols = ["member_id", "first_name", "last_name", "date_of_birth", "gender", "plan_code",
            "county", "coverage_start_date", "coverage_end_date", "updated_at"]
    df = spark.createDataFrame([
        ("M1", "mary", "SMITH", "1980-05-01", "f", "hmo_gold", "Orange", "2024-01-01", "", "x"),
        ("M2", "Joe", "Lee", "1950-02-01", "M", "MEDICARE_ADV", "Orange", "2024-01-01", "2024-06-30", "x"),
    ], cols)

    rows = {r["member_id"]: r for r in clean_members(df).collect()}

    assert rows["M1"]["plan_type"] == "HMO"
    assert rows["M1"]["first_name"] == "Mary"
    assert rows["M1"]["is_active"] is True
    assert rows["M2"]["plan_type"] == "Medicare Advantage"
    assert rows["M2"]["is_active"] is False


def test_plan_monthly_summary_member_months_and_pmpm(spark):
    member_cols = ["member_id", "first_name", "last_name", "date_of_birth", "gender", "plan_code",
                   "county", "coverage_start_date", "coverage_end_date", "updated_at"]
    members = clean_members(spark.createDataFrame([
        ("M1", "A", "B", "1980-01-01", "F", "HMO_GOLD", "X", "2024-01-01", "", "x"),
        ("M2", "C", "D", "1980-01-01", "M", "HMO_GOLD", "X", "2024-01-01", "", "x"),
        # termed before January 2025 -> not counted
        ("M3", "E", "F", "1980-01-01", "M", "HMO_GOLD", "X", "2024-01-01", "2024-12-15", "x"),
    ], member_cols))
    providers = clean_providers(spark.createDataFrame(
        [("1234567890", "Dr X", "Cardiology", "SPECIALIST", "Irvine", "CA", "IN")],
        ["provider_npi", "provider_name", "specialty", "provider_type", "city", "state", "network_status"]))
    claims = clean_claims(spark.createDataFrame([
        claim(claim_id="C1", member_id="M1", paid="60.00"),
        claim(claim_id="C2", member_id="M2", paid="40.00"),
    ], CLAIM_COLS))

    fact = build_fact_claims(claims, members, providers)
    row = build_plan_monthly_summary(fact, members).collect()[0]

    assert row["member_months"] == 2
    assert row["claim_count"] == 2
    assert row["total_paid"] == Decimal("100.00")
    assert float(row["paid_pmpm"]) == 50.0
    assert float(row["denial_rate"]) == 0.0
