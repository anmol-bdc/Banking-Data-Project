from __future__ import annotations

from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.transformation.utils import (
    read_delta,
    normalize_columns,
    deduplicate,
    ensure_date,
    print_record_count,
    write_delta_partitioned
)

from src.transformation.transformation_config import (
    BRONZE_TABLE_PATHS,
    SILVER_TABLE_PATHS,
    TABLE_AMOUNT_COLUMNS,
    TABLE_DATE_COLUMNS,
    SILVER_PARTITIONS
)

from src.transformation.currency_api_client import fetch_latest_rates


# ============================================================
# Build the FX reference dataframe
# Converts the FX rate dictionary into a Spark DataFrame that can be
# joined with business data during currency conversion.
# ============================================================

def build_fx_dataframe(spark: SparkSession, fx_rates: dict) -> DataFrame:
    """
    Converts Python dict of FX rates into Spark DataFrame.
    """
    rows = [(k, float(v)) for k, v in fx_rates.items()]
    return spark.createDataFrame(rows, ["currency_code", "rate_to_usd"])


# ============================================================
# Apply FX conversion to business data
# Joins each record with the FX reference table and creates USD-based
# versions of monetary columns for downstream reporting.
# ============================================================

def apply_fx_conversion(df: DataFrame, table_name: str, fx_df: DataFrame) -> DataFrame:
    """
    Join FX rates and convert monetary columns to USD.
    """

    amount_cols = TABLE_AMOUNT_COLUMNS.get(table_name, [])

    # Ensure currency column exists
    if "currency_code" not in df.columns:
        df = df.withColumn("currency_code", F.lit("USD"))

    df = df.join(fx_df, on="currency_code", how="left")

    # Fill missing rates with 1.0
    df = df.withColumn(
        "rate_to_usd",
        F.coalesce(F.col("rate_to_usd"), F.lit(1.0))
    )

    # Convert amount columns
    for col in amount_cols:
        if col in df.columns:
            df = df.withColumn(
                f"{col}_usd",
                F.col(col) * F.col("rate_to_usd")
            )

    return df


# ============================================================
# Convert one source table into the silver layer
# Reads bronze data, standardizes it, applies FX logic, and writes the
# transformed output into the silver Delta tables for reuse.
# ============================================================

def convert_table(spark: SparkSession, table_name: str):

    print(f"[STARTED] Currency conversion for {table_name}")

    # --------------------------------------------------------
    # Read Bronze data
    # --------------------------------------------------------
    df = read_delta(spark, BRONZE_TABLE_PATHS[table_name])

    # Deduplicate immediately after reading Bronze
    df = df.dropDuplicates()

    # --------------------------------------------------------
    # Apply basic cleaning
    # --------------------------------------------------------
    df = normalize_columns(df)
    df = deduplicate(df)

    print_record_count("After deduplication", df)

    # --------------------------------------------------------
    # Fetch FX rates for the current table
    # --------------------------------------------------------
    fx_rates = fetch_latest_rates()

    fx_df = build_fx_dataframe(spark, fx_rates)

    # --------------------------------------------------------
    # Apply the conversion logic
    # --------------------------------------------------------
    df = apply_fx_conversion(df, table_name, fx_df)

    # --------------------------------------------------------
    # Ensure the date column exists
    # --------------------------------------------------------
    date_col = TABLE_DATE_COLUMNS[table_name]
    df = ensure_date(df, date_col)

    # --------------------------------------------------------
    # Write the transformed data to the silver layer
    # --------------------------------------------------------
    partition_cols = SILVER_PARTITIONS[table_name]

    write_delta_partitioned(
        df=df,
        output_path=SILVER_TABLE_PATHS[table_name],
        partition_cols=partition_cols
    )

    print(f"[SUCCESS] Silver created for {table_name}")


# ============================================================
# Run the conversion stage for a list of tables
# Executes the bronze-to-silver transformation flow for each requested
# table in sequence.
# ============================================================

def run_currency_conversion(spark: SparkSession, tables: List[str]):

    print("[STAGE] Currency Conversion (Bronze → Silver)")

    for table in tables:
        convert_table(spark, table)
