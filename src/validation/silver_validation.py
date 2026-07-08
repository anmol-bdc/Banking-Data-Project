from .validation_utils import (
    check_nulls,
    check_duplicates
)


def validate_silver_table(df, table_name):

    print(f"\nSILVER VALIDATION : {table_name}")

    null_results = check_nulls(df)

    failed_columns = {
        k: v
        for k, v in null_results.items()
        if v > 0
    }

    if failed_columns:
        print(
            f"WARNING : Null Values Found"
        )
        print(failed_columns)

    else:
        print("PASSED : Null Check")

    if (
        table_name == "transactions"
        and "transaction_id" in df.columns
    ):
        duplicates = check_duplicates(
            df,
            "transaction_id"
        )

        print(
            f"Duplicate Transactions = {duplicates}"
        )

    if (
        table_name == "fx_rates"
        and "exchange_rate" in df.columns
    ):
        invalid_rates = (
            df.filter(
                df.exchange_rate <= 0
            )
            .count()
        )

        print(
            f"Invalid FX Rates = {invalid_rates}"
        )

    return True