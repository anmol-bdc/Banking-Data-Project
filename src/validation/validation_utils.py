from pyspark.sql.functions import col


def check_row_count(df):
    return df.count()


def check_nulls(df):
    results = {}

    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        results[column] = null_count

    return results


def check_duplicates(df, key_column):
    return (
        df.groupBy(key_column)
        .count()
        .filter("count > 1")
        .count()
    )


def check_columns_exist(df, expected_columns):
    missing = list(
        set(expected_columns) - set(df.columns)
    )

    return missing