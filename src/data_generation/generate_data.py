from __future__ import annotations

import sys
from typing import Dict

import pandas as pd

from src.common.logger import get_logger

from .utils import (
    TABLE_DESCRIPTIONS,
    default_rows_for_table,
    generate_and_save_table,
    parse_user_choice,
)

logger = get_logger(__name__)


def print_menu() -> None:
    # Show the user a friendly welcome banner and explain what they can generate.
    print("=" * 78)
    print("BANKING CSV GENERATOR")
    print("=" * 78)
    print("You can generate all or any combination of the below tables:")
    for key, desc in TABLE_DESCRIPTIONS.items():
        print(f"- {key:<12} : {desc}")
    print()
    print("Examples:")
    print("  all")
    print("  intraday")
    print("  loans fx_rates")
    print("  cash, transactions, scenario")
    print("=" * 78)


def main() -> None:

    logger.info("Data generation pipeline started")

    print_menu()

    raw_choice = input("What would you like to generate? ").strip()

    try:
        # Convert the free-form input into a clean list of requested tables.
        selected_tables = parse_user_choice(raw_choice)

        logger.info(
            f"User selected tables: {selected_tables}"
        )

    except ValueError as exc:
        # Stop gracefully when the user enters something the parser cannot understand.
        logger.error(f"Invalid user input: {exc}")
        print(f"[ERROR] {exc}")
        sys.exit(1)

    # Loans often act as a foundation for other datasets, so they should be created first.
    # If the user requests loans directly, we generate them right away.
    # If they request intraday or transactions without explicitly asking for loans,
    # we create loans temporarily in memory so downstream data can still reference them.

    ordered_tables = []

    requires_loans_for_reference = any(
        t in selected_tables
        for t in ["intraday", "transactions"]
    )

    if "loans" in selected_tables:
        ordered_tables.append("loans")

    elif requires_loans_for_reference:
        ordered_tables.append("loans__memory_only")

    for table_name in selected_tables:
        if table_name != "loans":
            ordered_tables.append(table_name)

    # Keep track of tables already generated in memory so later steps can reuse them.

    shared_context: Dict[str, pd.DataFrame] = {}
    summary_rows = []

    print()
    print("Starting generation...")
    print()

    logger.info(
        f"Generation order: {ordered_tables}"
    )

    for table_name in ordered_tables:

        logger.info(
            f"Processing table: {table_name}"
        )

        if table_name == "loans__memory_only":

            # This is a temporary, in-memory generation step used to prepare loan data
            # for downstream tables without creating a separate CSV file first.

            df, _ = generate_and_save_table(
                "loans",
                total_rows=default_rows_for_table("loans"),
                shared_context=shared_context
            )

            shared_context["loans"] = df

            logger.info(
                "Generated loans in-memory for downstream references"
            )

            print(
                "[INFO] Loans generated first to maintain downstream loan references."
            )

            continue

        # Decide how many rows to generate for the current table before creating it.

        target_rows = default_rows_for_table(table_name)

        df, output_path = generate_and_save_table(
            table_name=table_name,
            total_rows=target_rows,
            shared_context=shared_context,
        )

        # Keep the most recent loans data available for any later tables that depend on it.

        if table_name == "loans":
            shared_context["loans"] = df

        # Store a compact summary of this table for the final report at the end.

        summary_rows.append(
            (
                table_name,
                len(df),
                len(df.columns),
                str(output_path),
            )
        )

        logger.info(
            f"{table_name} generated successfully "
            f"(rows={len(df)}, cols={len(df.columns)})"
        )

        print(
            f"[SUCCESS] {table_name:<12} -> rows={len(df):,} cols={len(df.columns):,}"
        )
        print(f"          saved to: {output_path}")

    print()
    print("=" * 78)
    print("GENERATION SUMMARY")
    print("=" * 78)

    for table_name, row_count, col_count, output_path in summary_rows:
        print(
            f"{table_name:<12} | rows={row_count:>6,} | cols={col_count:>2,} | {output_path}"
        )

    print("=" * 78)

    logger.info(
        "Data generation pipeline completed successfully"
    )


if __name__ == "__main__":
    main()