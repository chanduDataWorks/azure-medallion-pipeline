"""
Pure PySpark transformations for the payer claims medallion pipeline.

Every function takes DataFrames and returns DataFrames - no reads, no writes.
That keeps the business logic unit-testable on a laptop (see tests/) while
the notebooks handle I/O and Delta MERGE on Databricks.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

VALID_CLAIM_STATUSES = ["PAID", "DENIED", "PENDING"]


# --------------------------------------------------------------------------- helpers
def _clean_str(col_name: str):
    """Trim whitespace and turn empty strings into NULL."""
    c = F.trim(F.col(col_name))
    return F.when(c == "", None).otherwise(c)


def _to_date(col_name: str):
    return F.to_date(_clean_str(col_name), "yyyy-MM-dd")


def _to_amount(col_name: str):
    return F.expr(f"try_cast(trim({col_name}) as decimal(12,2))")


# --------------------------------------------------------------------------- silver
def clean_members(df: DataFrame) -> DataFrame:
    """Bronze members (all strings) -> typed, standardised silver members."""
    plan = F.upper(_clean_str("plan_code"))
    return (
        df.select(
            _clean_str("member_id").alias("member_id"),
            F.initcap(_clean_str("first_name")).alias("first_name"),
            F.initcap(_clean_str("last_name")).alias("last_name"),
            _to_date("date_of_birth").alias("date_of_birth"),
            F.upper(_clean_str("gender")).alias("gender"),
            plan.alias("plan_code"),
            F.when(plan.startswith("HMO"), "HMO")
             .when(plan.startswith("PPO"), "PPO")
             .when(plan.startswith("MEDICARE"), "Medicare Advantage")
             .otherwise("Other").alias("plan_type"),
            _clean_str("county").alias("county"),
            _to_date("coverage_start_date").alias("coverage_start_date"),
            _to_date("coverage_end_date").alias("coverage_end_date"),
        )
        .withColumn("is_active", F.col("coverage_end_date").isNull())
        .dropDuplicates(["member_id"])
    )


def clean_providers(df: DataFrame) -> DataFrame:
    return (
        df.select(
            _clean_str("provider_npi").alias("provider_npi"),
            _clean_str("provider_name").alias("provider_name"),
            _clean_str("specialty").alias("specialty"),
            F.upper(_clean_str("provider_type")).alias("provider_type"),
            _clean_str("city").alias("city"),
            F.upper(_clean_str("state")).alias("state"),
            (F.upper(_clean_str("network_status")) == "IN").alias("is_in_network"),
        )
        .dropDuplicates(["provider_npi"])
    )


def clean_claims(df: DataFrame) -> DataFrame:
    """
    Type and standardise claims, then keep only the latest version of each
    claim_id (resubmissions arrive with the same id and a later loaded_at).
    """
    typed = df.select(
        _clean_str("claim_id").alias("claim_id"),
        _clean_str("member_id").alias("member_id"),
        _clean_str("provider_npi").alias("provider_npi"),
        F.upper(_clean_str("claim_type")).alias("claim_type"),
        _clean_str("procedure_code").alias("procedure_code"),
        F.upper(_clean_str("primary_dx_code")).alias("primary_dx_code"),
        _to_date("service_date").alias("service_date"),
        _to_date("received_date").alias("received_date"),
        _to_date("paid_date").alias("paid_date"),
        _to_amount("billed_amount").alias("billed_amount"),
        _to_amount("allowed_amount").alias("allowed_amount"),
        _to_amount("paid_amount").alias("paid_amount"),
        F.upper(_clean_str("claim_status")).alias("claim_status"),
        F.upper(_clean_str("denial_reason")).alias("denial_reason"),
        F.to_timestamp(_clean_str("loaded_at")).alias("loaded_at"),
    )

    latest_first = Window.partitionBy("claim_id").orderBy(F.col("loaded_at").desc())
    return (
        typed.withColumn("_rn", F.row_number().over(latest_first))
             .filter(F.col("_rn") == 1)
             .drop("_rn")
    )


def split_valid_claims(df: DataFrame) -> tuple:
    """
    Data quality gate. Returns (valid, rejected); rejected rows carry a
    reject_reason and are written to a quarantine table instead of silver.
    """
    reason = (
        F.when(F.col("claim_id").isNull(), "missing claim_id")
         .when(F.col("member_id").isNull(), "missing member_id")
         .when(F.col("service_date").isNull(), "invalid service_date")
         .when(F.col("billed_amount").isNull() | F.col("paid_amount").isNull(), "invalid amount")
         .when((F.col("billed_amount") < 0) | (F.col("paid_amount") < 0), "negative amount")
         .when(~F.col("claim_status").isin(VALID_CLAIM_STATUSES), "unknown claim_status")
    )
    flagged = df.withColumn("reject_reason", reason)
    valid = flagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected = flagged.filter(F.col("reject_reason").isNotNull())
    return valid, rejected


# --------------------------------------------------------------------------- gold
def build_fact_claims(claims: DataFrame, members: DataFrame, providers: DataFrame) -> DataFrame:
    """Claim-grain fact enriched with plan and provider attributes."""
    m = members.select("member_id", "plan_code", "plan_type", "date_of_birth")
    p = providers.select("provider_npi", "specialty", "provider_type", "is_in_network")
    return (
        claims.join(m, "member_id", "left")
              .join(p, "provider_npi", "left")
              .withColumn("service_month", F.trunc("service_date", "month"))
              .withColumn("is_denied", F.col("claim_status") == "DENIED")
              .withColumn("days_received_to_paid", F.datediff("paid_date", "received_date"))
              .withColumn("member_age_at_service",
                          F.floor(F.months_between("service_date", "date_of_birth") / 12).cast("int"))
              .drop("date_of_birth")
    )


def build_plan_monthly_summary(fact_claims: DataFrame, members: DataFrame) -> DataFrame:
    """
    Plan x month KPIs. A member counts toward a month's member months if their
    coverage overlaps any day of that month. PMPM = paid / member months.
    """
    months = fact_claims.select(F.col("service_month").alias("month_start")).distinct()

    member_months = (
        months.join(
            members,
            (F.col("coverage_start_date") < F.add_months("month_start", 1))
            & (F.coalesce("coverage_end_date", F.lit("9999-12-31").cast("date")) >= F.col("month_start")),
            "inner",
        )
        .groupBy("plan_code", "month_start")
        .agg(F.countDistinct("member_id").alias("member_months"))
    )

    claim_kpis = (
        fact_claims.groupBy("plan_code", F.col("service_month").alias("month_start"))
        .agg(
            F.count("*").alias("claim_count"),
            F.sum(F.col("is_denied").cast("int")).alias("denied_claim_count"),
            F.sum("billed_amount").alias("total_billed"),
            F.sum("allowed_amount").alias("total_allowed"),
            F.sum("paid_amount").alias("total_paid"),
        )
    )

    return (
        member_months.join(claim_kpis, ["plan_code", "month_start"], "left")
        .fillna(0, subset=["claim_count", "denied_claim_count"])
        .withColumn("total_paid", F.coalesce("total_paid", F.lit(0)))
        .withColumn("paid_pmpm", F.round(F.col("total_paid") / F.col("member_months"), 2))
        .withColumn("denial_rate",
                    F.round(F.col("denied_claim_count") / F.when(F.col("claim_count") > 0, F.col("claim_count")), 4))
        .orderBy("month_start", "plan_code")
    )


def build_provider_summary(fact_claims: DataFrame) -> DataFrame:
    """One row per provider: volume, cost, denial rate and payment speed."""
    return (
        fact_claims.groupBy("provider_npi", "specialty", "is_in_network")
        .agg(
            F.count("*").alias("claim_count"),
            F.sum("paid_amount").alias("total_paid"),
            F.round(F.avg(F.col("is_denied").cast("int")), 4).alias("denial_rate"),
            F.round(F.avg("days_received_to_paid"), 1).alias("avg_days_to_pay"),
        )
    )
