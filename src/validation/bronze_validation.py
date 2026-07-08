from .validation_utils import (
    check_row_count,
    check_columns_exist
)

EXPECTED_SCHEMAS = {
    "transactions": [
        "transaction_id",
        "customer_id",
        "transaction_amount"
    ],

    "cash": [
        "region_code",
        "available_liquidity"
    ],

    "loans": [
        "loan_id",
        "customer_id",
        "outstanding_balance"
    ],

    "intraday": [
        "position_id",
        "net_position"
    ],

    "fx_rates": [
        "currency_code",
        "exchange_rate"
    ],

    "scenario": [
        "scenario_id",
        "stress_factor"
    ]
}


def validate_bronze_table(df, table_name):

    print(f"\nBRONZE VALIDATION : {table_name}")

    row_count = check_row_count(df)

    if row_count == 0:
        print("FAILED : Empty table")
        return False

    print(f"PASSED : Row count = {row_count}")

    missing_columns = check_columns_exist(
        df,
        EXPECTED_SCHEMAS[table_name]
    )

    if missing_columns:
        print(
            f"FAILED : Missing Columns {missing_columns}"
        )
        return False

    print("PASSED : Schema Validation")

    return True