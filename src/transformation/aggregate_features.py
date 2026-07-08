from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.transformation.utils import (
    read_delta,
    write_delta_partitioned,
    print_record_count
)

from src.transformation.transformation_config import GOLD_TABLE_PATHS


# ============================================================
# Transaction stress aggregation
# Summarizes the stressed transaction amounts by scenario and region.
# ============================================================

def build_total_transaction_stress(spark: SparkSession):

    # Read the gold-layer transactions table that was produced by the scenario step.
    df = read_delta(spark, GOLD_TABLE_PATHS["transactions"])

    # Report the number of rows entering this aggregation step.
    print_record_count("Transactions input", df)

    # Aggregate stressed transaction values for each scenario and region combination.
    result = df.groupBy(
        "scenario_id", "region_code"
    ).agg(
        F.sum("stressed_transaction_amount").alias("total_stressed_transactions")
    )

    # Store the aggregated output as a Delta table partitioned by scenario.
    write_delta_partitioned(
        df=result,
        output_path=GOLD_TABLE_PATHS["transactions"] / "aggregates_transactions",
        partition_cols=["scenario_id"]
    )

    print("[SUCCESS] Transaction stress aggregate created")


# ============================================================
# Cash and liquidity aggregation
# Calculates the total stressed liquidity impact by scenario and region.
# ============================================================

def build_total_liquidity(spark: SparkSession):

    # Read the gold-layer cash table for liquidity-focused analysis.
    df = read_delta(spark, GOLD_TABLE_PATHS["cash"])

    # Report the number of cash records before aggregation.
    print_record_count("Cash input", df)

    # Aggregate stressed liquidity values for each scenario and region pair.
    result = df.groupBy(
        "scenario_id", "region_code"
    ).agg(
        F.sum("stressed_liquidity").alias("total_stressed_liquidity")
    )

    # Persist the liquidity aggregate to a partitioned Delta table.
    write_delta_partitioned(
        df=result,
        output_path=GOLD_TABLE_PATHS["cash"] / "aggregates_cash",
        partition_cols=["scenario_id"]
    )

    print("[SUCCESS] Liquidity aggregate created")


# ============================================================
# Loan exposure aggregation
# Summarizes the stressed loan exposure by scenario and region.
# ============================================================

def build_total_loan_exposure(spark: SparkSession):

    # Read the gold-layer loans table to calculate portfolio stress exposure.
    df = read_delta(spark, GOLD_TABLE_PATHS["loans"])

    # Report the number of loan records entering the aggregation logic.
    print_record_count("Loans input", df)

    # Aggregate stressed exposure values for each scenario and region pair.
    result = df.groupBy(
        "scenario_id", "region_code"
    ).agg(
        F.sum("stressed_exposure").alias("total_stressed_exposure")
    )

    # Write the exposure aggregate to a partitioned Delta table.
    write_delta_partitioned(
        df=result,
        output_path=GOLD_TABLE_PATHS["loans"] / "aggregates_loans",
        partition_cols=["scenario_id"]
    )

    print("[SUCCESS] Loan exposure aggregate created")


# ============================================================
# Intraday position aggregation
# Calculates the total stressed intraday position by scenario and region.
# ============================================================

def build_intraday_position(spark: SparkSession):

    # Read the gold-layer intraday table to summarize position risk.
    df = read_delta(spark, GOLD_TABLE_PATHS["intraday"])

    # Report the number of intraday rows before aggregation.
    print_record_count("Intraday input", df)

    # Aggregate stressed position values for each scenario and region pair.
    result = df.groupBy(
        "scenario_id", "region_code"
    ).agg(
        F.sum("stressed_net_position").alias("total_stressed_position")
    )

    # Persist the intraday aggregate into a partitioned Delta table.
    write_delta_partitioned(
        df=result,
        output_path=GOLD_TABLE_PATHS["intraday"] / "aggregates_intraday",
        partition_cols=["scenario_id"]
    )

    print("[SUCCESS] Intraday aggregate created")


# ============================================================
# Combined stress metrics
# Joins all stress-related aggregates into one combined view by scenario.
# ============================================================

def build_combined_metrics(spark: SparkSession):

    # Read each gold-layer table that contributes to the combined stress view.
    txn = read_delta(spark, GOLD_TABLE_PATHS["transactions"])
    cash = read_delta(spark, GOLD_TABLE_PATHS["cash"])
    loans = read_delta(spark, GOLD_TABLE_PATHS["loans"])
    intraday = read_delta(spark, GOLD_TABLE_PATHS["intraday"])

    # Create one aggregate per stress metric grouped by scenario.
    txn_agg = txn.groupBy("scenario_id").agg(
        F.sum("stressed_transaction_amount").alias("txn_total")
    )

    cash_agg = cash.groupBy("scenario_id").agg(
        F.sum("stressed_liquidity").alias("liq_total")
    )

    loans_agg = loans.groupBy("scenario_id").agg(
        F.sum("stressed_exposure").alias("loan_total")
    )

    intraday_agg = intraday.groupBy("scenario_id").agg(
        F.sum("stressed_net_position").alias("intraday_total")
    )

    # Join all aggregated metrics into a single scenario-level result set.
    combined = txn_agg \
        .join(cash_agg, "scenario_id", "outer") \
        .join(loans_agg, "scenario_id", "outer") \
        .join(intraday_agg, "scenario_id", "outer")

    # Replace missing values with zero so the combined output is easier to consume.
    combined = combined.fillna(0)

    # Write the combined metrics to a Delta table partitioned by scenario.
    write_delta_partitioned(
        df=combined,
        output_path=GOLD_TABLE_PATHS["transactions"] / "aggregates_combined",
        partition_cols=["scenario_id"]
    )

    print("[SUCCESS] Combined stress metrics created")


# ============================================================
# Aggregate orchestration
# Runs all aggregate feature builders in sequence.
# ============================================================

def run_aggregates(spark: SparkSession):

    print("[STAGE] Aggregate Feature Creation (Gold → Analytics)")

    # Execute each aggregation step in order so the downstream metrics build on prior outputs.
    build_total_transaction_stress(spark)
    build_total_liquidity(spark)
    build_total_loan_exposure(spark)
    build_intraday_position(spark)
    build_combined_metrics(spark)