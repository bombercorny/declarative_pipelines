import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.testing.utils import assertDataFrameEqual

from declarative_pipelines.functions import (
    donothing_transformation,
    dummy_transformation,
    with_lit_column,
)


class TestWithLitColumn:
    def test_adds_column_with_expected_value(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("a",), ("b",)], ["id"])

        result = with_lit_column(df, "new_col", "hello")

        expected_df = spark.createDataFrame([("a", "hello"), ("b", "hello")], ["id", "new_col"])
        assertDataFrameEqual(
            result,
            expected_df,
            ignoreColumnOrder=True,
            checkRowOrder=True,
        )


class TestDonothingTransformation:
    def test_returns_the_df_unchanged(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
        result_df = donothing_transformation({"df": df})

        assertDataFrameEqual(
            result_df,
            df,
            ignoreColumnOrder=True,
            checkRowOrder=True,
        )


class TestDummyTransformation:
    @pytest.fixture()
    def source_dfs(self, spark: SparkSession) -> dict:
        schema = StructType(
            [
                StructField("col1", StringType(), True),
                StructField("col2", StringType(), True),
            ]
        )
        df1 = spark.createDataFrame([("val1", "x")], schema)
        df2 = spark.createDataFrame([("val2", "y")], schema)
        return {"df1": df1, "df2": df2}

    def test_output_contains_foo_column(self, source_dfs: dict) -> None:
        result = dummy_transformation(source_dfs, foo="foo_val", bar="bar_val")

        assert "foo_column" in result.columns
