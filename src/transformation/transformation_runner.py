from __future__ import annotations

from src.common.logger import get_logger

from src.transformation.utils import get_spark

from src.transformation.transformation_config import TRANSFORMABLE_TABLES

from src.transformation.currency_conversion import run_currency_conversion
from src.transformation.scenario_transformations import run_scenario_transformations
from src.transformation.aggregate_features import run_aggregates

logger = get_logger(__name__)


# ============================================================
# Pipeline stages
# Defines the three main execution phases of the transformation flow:
# bronze-to-silver enrichment, silver-to-gold scenario modeling, and
# gold-layer aggregate generation.
# ============================================================

def run_bronze_to_silver(spark):
    """
    Stage 1: Bronze → Silver
    Currency Conversion + Cleaning
    """

    logger.info("Starting Bronze → Silver stage")

    print("\n" + "=" * 90)
    print("[PIPELINE] Stage 1 → Bronze → Silver (Currency Conversion)")
    print("=" * 90)

    run_currency_conversion(
        spark=spark,
        tables=TRANSFORMABLE_TABLES
    )

    logger.info("Bronze → Silver stage completed")


def run_silver_to_gold(spark):
    """
    Stage 2: Silver → Gold
    Scenario Enrichment + Stress Calculations
    """

    logger.info("Starting Silver → Gold stage")

    print("\n" + "=" * 90)
    print("[PIPELINE] Stage 2 → Silver → Gold (Scenario Transformations)")
    print("=" * 90)

    run_scenario_transformations(
        spark=spark,
        tables=TRANSFORMABLE_TABLES
    )

    logger.info("Silver → Gold stage completed")


def run_gold_aggregates(spark):
    """
    Stage 3: Gold → Aggregates
    Analytical outputs for reporting
    """

    logger.info("Starting Gold → Aggregates stage")

    print("\n" + "=" * 90)
    print("[PIPELINE] Stage 3 → Gold → Aggregates")
    print("=" * 90)

    run_aggregates(spark)

    logger.info("Gold → Aggregates stage completed")


# ============================================================
# Main pipeline runner
# Creates the Spark session, executes the pipeline stages in order,
# and handles both successful completion and failure reporting.
# ============================================================

def main():

    logger.info("Transformation pipeline started")

    print("\n" + "=" * 90)
    print("BANKING DATA TRANSFORMATION PIPELINE")
    print("=" * 90)

    spark = get_spark()

    logger.info("Spark session created")

    try:
        # ----------------------------------------------------
        # Stage 1: Bronze to silver transformation
        # ----------------------------------------------------
        run_bronze_to_silver(spark)

        # ----------------------------------------------------
        # Stage 2: Silver to gold enrichment
        # ----------------------------------------------------
        run_silver_to_gold(spark)

        # ----------------------------------------------------
        # Stage 3: Gold to aggregate reporting outputs
        # ----------------------------------------------------
        run_gold_aggregates(spark)

        logger.info(
            "Full transformation pipeline completed successfully"
        )

        print("\n" + "=" * 90)
        print("[SUCCESS] Full Transformation Pipeline Completed")
        print("=" * 90)

    except Exception as e:

        logger.error(
            f"Transformation pipeline failed: {e}"
        )

        print(f"\n[ERROR] Pipeline failed: {e}")

    finally:

        logger.info("Stopping Spark session")

        spark.stop()

        logger.info("Spark session stopped")

        print("[INFO] Spark session stopped")


# ============================================================
# Entry point
# Executes the transformation pipeline when this module is run directly.
# ============================================================

if __name__ == "__main__":
    main()