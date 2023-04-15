"""Provide functions for checking data quality."""

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


def contain_target_columns(df: DataFrame, columns: list, table_name: str) -> None:
    "check if row  df contain target columns"
    df_columns = df.columns
    for col in columns:
        if col not in df_columns:
            raise ValueError(
                f"Data quality check failed. Source {table_name} not contain: {col}"
            )


def contain_null(df: DataFrame, key_cols: list, table_name: str) -> None:
    " check that there are no null values in the key columns of the trip table"
    null_count = df.select(
        [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in key_cols]
    ).collect()
    for col_name, null_val_count in zip(key_cols, null_count):
        if null_val_count[col_name] != 0:
            raise ValueError(
                f"Data quality check failed. {null_val_count[col_name]} null values found in table {table_name} column {col_name}."
            )
    else:
        print(
            "No null values found in key columns of trip table. Data quality check passed."
        )


def contain_rows(df: DataFrame, table_name: str) -> None:
    """Check if the table contains data"""
    assert df.count() > 0, f"Table {table_name} contains no values."
