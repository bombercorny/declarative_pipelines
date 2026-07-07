from pyspark.sql import DataFrame, SparkSession

import declarative_pipelines.functions  # noqa: F401 — triggers @register decorators
from declarative_pipelines.registry import TRANSFORMATION_REGISTRY


def get_source_dfs(spark: SparkSession, sources: list[dict[str, str]]) -> dict[str, DataFrame]:
    source_dfs = {}
    for source in sources:
        source_dfs[source.get("alias", "df")] = spark.read.table(source["fully_qualified_name"])
    return source_dfs
