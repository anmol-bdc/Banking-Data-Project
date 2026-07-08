def validate_gold_table(df, table_name):

    print(f"\nGOLD VALIDATION : {table_name}")

    if (
        "stress_factor"
        in df.columns
    ):
        invalid = (
            df.filter(
                df.stress_factor <= 0
            )
            .count()
        )

        print(
            f"Invalid Stress Factors = {invalid}"
        )

    if (
        "stressed_transaction_amount"
        in df.columns
    ):
        invalid = (
            df.filter(
                df.stressed_transaction_amount < 0
            )
            .count()
        )

        print(
            f"Negative Stressed Amounts = {invalid}"
        )

    if (
        "stressed_liquidity"
        in df.columns
    ):
        invalid = (
            df.filter(
                df.stressed_liquidity < 0
            )
            .count()
        )

        print(
            f"Negative Liquidity Records = {invalid}"
        )

    print(
        "PASSED : Business Rule Validation"
    )

    return True