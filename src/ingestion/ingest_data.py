from __future__ import annotations

from src.common.logger import get_logger

from .utils import (
    TABLE_DESCRIPTIONS,
    get_spark,
    ingest_table,
    latest_csv_for_table,
    parse_user_choice,
)

logger = get_logger(__name__)


# Display the available tables and their business purpose so users can choose
# which data feeds should be ingested from the raw layer.
def print_table_menu() -> None:
    print("=" * 90)
    print("BANKING INGESTION PIPELINE")
    print("=" * 90)
    print("Select one or more tables to ingest:")
    for key, desc in TABLE_DESCRIPTIONS.items():
        print(f"- {key:<12} : {desc}")
    print("=" * 90)


# Run the ingestion workflow end to end by collecting user input, validating
# the requested tables, and sending each available source file through the
# ingestion function.
def main() -> None:

    logger.info("Ingestion pipeline started")

    print_table_menu()

    raw_choice = input("What would you like to ingest? ").strip()

    selected_tables = parse_user_choice(raw_choice)

    logger.info(
        f"User selected tables: {selected_tables}"
    )

    if not selected_tables:
        logger.warning("No valid table names provided")
        print("No valid table names provided.")
        return

    # Identify any requested tables that do not currently have a raw CSV file so
    # they can be skipped with a clear message instead of failing silently.
    missing_tables = [
        t for t in selected_tables
        if latest_csv_for_table(t) is None
    ]

    if missing_tables:

        logger.warning(
            f"Missing source files for tables: {missing_tables}"
        )

        print("\nThese tables have no raw CSV available and will be skipped:")

        for t in missing_tables:
            print(f"   - {t}")

    # Build the final list of work that can actually be processed with the data
    # currently available in the raw folder.
    runnable_tables = [
        t for t in selected_tables
        if latest_csv_for_table(t) is not None
    ]

    if not runnable_tables:
        logger.warning(
            "None of the selected tables have available source CSV files"
        )

        print(
            "None of the selected tables have available source CSV files."
        )

        return

    spark = get_spark()

    logger.info("Spark session created")

    try:

        for table_name in runnable_tables:

            logger.info(
                f"Starting ingestion for table: {table_name}"
            )

            print(
                f"\nStarting ingestion for table: {table_name}"
            )

            # Scenario data is written differently from the operational feeds, so
            # it follows a slightly simpler ingestion path.
            if table_name == "scenario":

                ingest_table(
                    spark=spark,
                    table_name=table_name
                )

            # All other tables follow the standard ingestion logic and are stored
            # in a partitioned Delta layout for downstream processing.
            else:

                ingest_table(
                    spark=spark,
                    table_name=table_name
                )

            logger.info(
                f"{table_name} ingested successfully"
            )

        logger.info(
            "Ingestion pipeline completed successfully"
        )

        print(
            "\nIngestion pipeline completed successfully."
        )

    finally:

        logger.info("Stopping Spark session")

        spark.stop()

        logger.info("Spark session stopped")

        print("Spark session stopped.")


if __name__ == "__main__":
    main()