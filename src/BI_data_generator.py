"""
BI_data_generator.py

This script prepares the curated Gold-layer data for Power BI by reading
Delta tables from the data lake and exporting them as Parquet files in a
folder structure that is easier for reporting tools to consume.

Expected output location:
data/
└── PowerBI/
    ├── transactions/
    ├── cash/
    ├── loans/
    ├── intraday/
    ├── aggregates_transactions/
    ├── aggregates_cash/
    ├── aggregates_loans/
    ├── aggregates_intraday/
    └── aggregates_combined/
"""

from pathlib import Path

from pyspark.sql import SparkSession

from src.transformation.utils import get_spark


# ============================================================
# PATHS
# ============================================================

# Project root is one level above this script, so we can build reliable
# paths to the source gold data and the destination Power BI export folder.
PROJECT_ROOT = Path(__file__).resolve().parent.parent

GOLD_PATH = PROJECT_ROOT / "data" / "gold"

POWERBI_PATH = PROJECT_ROOT / "data" / "PowerBI"


# ============================================================
# EXPORT FUNCTION
# ============================================================

def export_table_for_powerbi(
    spark: SparkSession,
    source_path: Path,
    target_path: Path
):
    """Read a Gold-layer Delta table and export it as a Parquet dataset."""

    # Let the user know which source table is being prepared.
    print(f"[INFO] Reading: {source_path}")

    # Load the data directly from the Delta table stored in the gold layer.
    df = spark.read.format("delta").load(str(source_path))

    # Write the dataset to the Power BI output folder as a Parquet copy.
    print(f"[INFO] Writing: {target_path}")

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(str(target_path))
    )

    # Confirm that the export completed successfully.
    print(f"[SUCCESS] Exported {target_path.name}")


# ============================================================
# MAIN
# ============================================================

def main():
    """Run the full export workflow for the main and aggregate gold tables."""

    print("\n" + "=" * 90)
    print("POWER BI DATASET GENERATION")
    print("=" * 90)

    # Create a Spark session using the shared project helper.
    spark = get_spark()

    try:
        # Make sure the target folder exists before writing export files.
        POWERBI_PATH.mkdir(
            parents=True,
            exist_ok=True
        )

        # ----------------------------------------------------
        # MAIN GOLD TABLES
        # ----------------------------------------------------

        # These are the primary business tables that Power BI will usually
        # use directly for reporting and dashboarding.
        gold_tables = [
            "transactions",
            "cash",
            "loans",
            "intraday"
        ]

        for table in gold_tables:
            source = GOLD_PATH / table
            target = POWERBI_PATH / table

            # Only export the table if the source exists in the gold layer.
            if source.exists():
                export_table_for_powerbi(
                    spark,
                    source,
                    target
                )

        # ----------------------------------------------------
        # AGGREGATE TABLES
        # ----------------------------------------------------

        # These tables contain pre-aggregated metrics and are useful for
        # faster analytical queries and summary reporting in Power BI.
        aggregate_tables = [
            ("transactions/aggregates_transactions",
             "aggregates_transactions"),

            ("cash/aggregates_cash",
             "aggregates_cash"),

            ("loans/aggregates_loans",
             "aggregates_loans"),

            ("intraday/aggregates_intraday",
             "aggregates_intraday"),

            ("transactions/aggregates_combined",
             "aggregates_combined"),
        ]

        for source_suffix, target_name in aggregate_tables:
            source = GOLD_PATH / source_suffix
            target = POWERBI_PATH / target_name

            # Export each aggregate dataset when it is present.
            if source.exists():
                export_table_for_powerbi(
                    spark,
                    source,
                    target
                )

        print("\n[SUCCESS] Power BI datasets generated successfully")

        print(f"\nLocation:")
        print(POWERBI_PATH)

    finally:
        # Always stop the Spark session to avoid leaving resources running.
        spark.stop()

        print("[INFO] Spark session stopped")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()