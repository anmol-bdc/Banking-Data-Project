from src.ingestion.utils import get_spark
from delta import configure_spark_with_delta_pip

from .bronze_validation import (
    validate_bronze_table
)

from .silver_validation import (
    validate_silver_table
)

from .gold_validation import (
    validate_gold_table
)


def main():

    spark = get_spark()

    print("=" * 70)
    print("VALIDATION PIPELINE")
    print("=" * 70)

    # -----------------------
    # BRONZE
    # -----------------------

    transactions_bronze = spark.read.format(
        "delta"
    ).load(
        "data/bronze/transactions"
    )

    validate_bronze_table(
        transactions_bronze,
        "transactions"
    )

    # -----------------------
    # SILVER
    # -----------------------

    transactions_silver = spark.read.format(
        "delta"
    ).load(
        "data/silver/transactions"
    )

    validate_silver_table(
        transactions_silver,
        "transactions"
    )

    # -----------------------
    # GOLD
    # -----------------------

    transactions_gold = spark.read.format(
        "delta"
    ).load(
        "data/gold/transactions"
    )

    validate_gold_table(
        transactions_gold,
        "transactions"
    )

    print("\n")
    print("=" * 70)
    print("VALIDATION COMPLETED")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()