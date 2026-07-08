from __future__ import annotations

from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.transformation.utils import (
    read_delta,
    read_parquet,
    write_delta_partitioned,
    print_record_count
)

from src.transformation.transformation_config import (
    SILVER_TABLE_PATHS,
    GOLD_TABLE_PATHS,
    GOLD_PARTITIONS,
    BRONZE_TABLE_PATHS,
    TABLE_DATE_COLUMNS
)


# ============================================================
# Build the scenario input dataframe
# Loads the scenario table from bronze storage and keeps only the
# columns needed for the stress transformation logic.
# ============================================================

def build_scenario_df(spark: SparkSession) -> DataFrame:
    """
    Load scenario table from Bronze (parquet).
    """
    scenario_df = read_parquet(spark, BRONZE_TABLE_PATHS["scenario"])

    # Keep only the columns required for scenario-based stress modeling
    scenario_df = scenario_df.select(
        "scenario_id",
        "market_stress_factor"
    )

    print_record_count("Scenario records", scenario_df)

    scenario_df = scenario_df.limit(5)

    return scenario_df


# ============================================================
# Apply the scenario context to business data
# Cross-joins the business dataset with a small broadcasted subset of
# scenario rows to keep the transformation stable and efficient.
# ============================================================

def apply_scenario(df: DataFrame, scenario_df: DataFrame) -> DataFrame:
    """
    Join scenario with business data.

    Uses broadcast since scenario is small.
    """

    enriched_df = df.crossJoin(F.broadcast(scenario_df.limit(5)))

    return enriched_df


# ============================================================
# Derive stress-adjusted business metrics
# Uses the scenario stress factor to create adjusted values for each
# supported table type and adds a severity label for reporting.
# ============================================================

def add_stress_columns(df: DataFrame, table_name: str) -> DataFrame:
    """
    Add derived stress-based columns.

    These simulate stress scenarios using scenario factors.
    """

    # Generic stress multiplier
    if "market_stress_factor" in df.columns:

        if table_name == "transactions" and "transaction_amount_usd" in df.columns:
            df = df.withColumn(
                "stressed_transaction_amount",
                F.col("transaction_amount_usd") * F.col("market_stress_factor")
            )

        elif table_name == "cash" and "available_liquidity_usd" in df.columns:
            df = df.withColumn(
                "stressed_liquidity",
                F.col("available_liquidity_usd") * F.col("market_stress_factor")
            )

        elif table_name == "loans" and "outstanding_balance_usd" in df.columns:
            df = df.withColumn(
                "stressed_exposure",
                F.col("outstanding_balance_usd") * F.col("market_stress_factor")
            )

        elif table_name == "intraday" and "net_position_usd" in df.columns:
            df = df.withColumn(
                "stressed_net_position",
                F.col("net_position_usd") * F.col("market_stress_factor")
            )

    # Add severity labeling (optional but useful)
    df = df.withColumn(
        "stress_level",
        F.when(F.col("market_stress_factor") >= 1.5, "HIGH")
         .when(F.col("market_stress_factor") >= 1.2, "MEDIUM")
         .otherwise("LOW")
    )

    return df


# ============================================================
# Transform one table into the gold layer
# Reads the silver data, joins it with scenario context, adds stress
# metrics, and writes the output into the gold Delta tables.
# ============================================================

def transform_table(spark: SparkSession, table_name: str):

    print(f"[STARTED] Scenario transformation for {table_name}")

    # --------------------------------------------------------
    # Read the silver table for the requested business domain
    # --------------------------------------------------------
    df = read_delta(spark, SILVER_TABLE_PATHS[table_name])

    print_record_count("Silver records", df)

    # --------------------------------------------------------
    # Load the scenario rows that will enrich the table
    # --------------------------------------------------------
    scenario_df = build_scenario_df(spark)

    # --------------------------------------------------------
    # Apply the scenario context through a controlled cross join
    # --------------------------------------------------------
    df = apply_scenario(df, scenario_df)

    print_record_count("After scenario join", df)

    # --------------------------------------------------------
    # Add stress-adjusted columns to the transformed data
    # --------------------------------------------------------
    df = add_stress_columns(df, table_name)

    # --------------------------------------------------------
    # Configure the target partitioning for the gold output
    # --------------------------------------------------------
    partition_cols = GOLD_PARTITIONS[table_name]

    # Ensure date column exists
    date_col = TABLE_DATE_COLUMNS[table_name]
    if date_col not in df.columns:
        df = df.withColumn(date_col, F.current_date())

    # --------------------------------------------------------
    # Write the transformed dataset to the gold layer
    # --------------------------------------------------------
    write_delta_partitioned(
        df=df,
        output_path=GOLD_TABLE_PATHS[table_name],
        partition_cols=partition_cols
    )

    print(f"[SUCCESS] Gold table created for {table_name}")


# ============================================================
# Run the scenario transformation stage
# Executes the gold-layer transformation flow for each requested table.
# ============================================================

def run_scenario_transformations(spark: SparkSession, tables: List[str]):

    print("[STAGE] Scenario Transformation (Silver → Gold)")

    for table in tables:
        transform_table(spark, table)