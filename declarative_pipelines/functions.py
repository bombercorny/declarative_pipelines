from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from declarative_pipelines.registry import register


def with_lit_column(df: DataFrame, column_name: str, value: str) -> DataFrame:
    return df.withColumn(column_name, F.lit(value))


@register
def dummy_transformation(source_dfs: dict[str, DataFrame], foo: str, bar: str) -> DataFrame:
    df1 = source_dfs["df1"]
    df2 = source_dfs["df2"]
    df1 = with_lit_column(df1, "foo_column", foo)
    df2 = with_lit_column(df2, "bar_column", bar)
    unioned_df = df1.unionByName(df2, allowMissingColumns=True)
    return unioned_df


@register
def donothing_transformation(source_dfs: dict[str, DataFrame]) -> DataFrame:
    return source_dfs["df"]


@register
def with_primary_key_counts(source_dfs: dict[str, DataFrame], primary_key_column: Union[str, list[str]]) -> DataFrame:
    """
    Validate that the primary key column(s) in the DataFrame are unique.

    Args:
        source_dfs (dict[str, DataFrame]): A dictionary of source DataFrames.
        primary_key_column (Union[str, list[str]]): The primary key column(s) to validate.

    Returns:
        DataFrame: The original DataFrame if the primary key is valid.

    Raises:
        ValueError: If the primary key is not unique.
    """
    df = source_dfs["df"]
    if isinstance(primary_key_column, str):
        primary_key_column = [primary_key_column]

    return df.groupBy(primary_key_column).agg(F.count("*").alias("count"))
