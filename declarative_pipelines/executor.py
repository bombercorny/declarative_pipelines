import json
from dataclasses import dataclass
from typing import Callable

import pyspark.sql.functions as F
from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

from declarative_pipelines import TRANSFORMATION_REGISTRY, get_source_dfs
from declarative_pipelines.models import ExpectationType, TargetType, TransformationStep


@dataclass
class PipelineContext:
    spark: SparkSession
    transformation_registry: dict[str, Callable]
    expectation_decorators: dict[ExpectationType, Callable]
    target_decorators: dict[TargetType, Callable]


def _row_to_step(step_dict: dict) -> TransformationStep:
    step_dict["transformation"]["kwargs"] = json.loads(step_dict["transformation"]["kwargs"] or "{}")
    return TransformationStep(**step_dict)


def load_steps(df: DataFrame, pipeline_id: str) -> list[TransformationStep]:
    """Read and deserialize all steps for this pipeline_id."""
    rows = df.filter(F.col("pipeline_id") == pipeline_id).collect()

    if not rows:
        raise ValueError(f"No pipeline configuration found for pipeline_id='{pipeline_id}'")

    steps_data = rows[0].asDict(recursive=True)["steps"]
    return [_row_to_step(step_dict) for step_dict in steps_data]


def create_transformation_step(step: TransformationStep, ctx: PipelineContext) -> Callable:
    def _dataset():
        source_dfs = get_source_dfs(spark=ctx.spark, sources=[vars(s) for s in step.sources])
        fn = ctx.transformation_registry[step.transformation.transformation_id]
        return fn(source_dfs, **step.transformation.kwargs)

    # Apply the target decorator first (innermost position in the decorator stack).
    # Temporary views do not support comment or schema parameters.
    if step.target.target_type == TargetType.TEMPORARY_VIEW:
        _dataset = ctx.target_decorators[TargetType.TEMPORARY_VIEW](name=step.target.fully_qualified_name)(_dataset)
    else:
        decorator_kwargs: dict = {
            "name": step.target.fully_qualified_name,
            "comment": step.target.comment,
        }
        if step.target.schema:
            decorator_kwargs["schema"] = step.target.schema

        _dataset = ctx.target_decorators[step.target.target_type](**decorator_kwargs)(_dataset)

    # Apply expectations on top (reversed so the first expectation in config is outermost).
    for exp in reversed(step.target.expectations):
        _dataset = ctx.expectation_decorators[exp.expectation_type](exp.name, exp.condition)(_dataset)

    return _dataset


def run_pipeline(spark: SparkSession, pipeline_id: str, config_table: str) -> None:
    """Build a PipelineContext with the standard Databricks decorators and run all steps."""
    ctx = PipelineContext(
        spark=spark,
        transformation_registry=TRANSFORMATION_REGISTRY,
        expectation_decorators={
            ExpectationType.EXPECT: dp.expect,
            ExpectationType.EXPECT_OR_DROP: dp.expect_or_drop,
            ExpectationType.EXPECT_OR_FAIL: dp.expect_or_fail,
        },
        target_decorators={
            TargetType.TABLE: dp.table,
            TargetType.MATERIALIZED_VIEW: dp.materialized_view,
            TargetType.TEMPORARY_VIEW: dp.temporary_view,
        },
    )
    config_df = spark.read.table(config_table)
    for step in load_steps(df=config_df, pipeline_id=pipeline_id):
        create_transformation_step(step, ctx)
