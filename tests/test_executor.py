from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from declarative_pipelines.executor import PipelineContext, _row_to_step, create_transformation_step, load_steps
from declarative_pipelines.models import ExpectationType, TargetType, TransformationStep
from declarative_pipelines.schema import CONFIG_SCHEMA

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_step_dict(
    transformation_id: str = "dummy",
    kwargs_json: str | None = None,
    target_type: str = "temporary_view",
    fully_qualified_name: str = "my_view",
) -> dict:
    return {
        "sources": [{"fully_qualified_name": "cat.sch.tbl", "alias": "df"}],
        "transformation": {"transformation_id": transformation_id, "kwargs": kwargs_json},
        "target": {
            "fully_qualified_name": fully_qualified_name,
            "target_type": target_type,
            "comment": None,
            "schema": None,
            "expectations": [],
        },
    }


def _make_pipeline_row_dict(
    pipeline_id: str = "pipeline_001",
    steps: list[dict] | None = None,
) -> dict:
    return {
        "pipeline_id": pipeline_id,
        "pipeline_name": None,
        "pipeline_description": None,
        "steps": steps or [_make_step_dict()],
    }


# ---------------------------------------------------------------------------
# _row_to_step
# ---------------------------------------------------------------------------


class TestRowToStep:
    def test_returns_transformation_step(self):
        assert isinstance(_row_to_step(_make_step_dict()), TransformationStep)

    def test_deserializes_kwargs_json(self):
        step = _row_to_step(_make_step_dict(kwargs_json='{"foo": "bar", "baz": 1}'))
        assert step.transformation.kwargs == {"foo": "bar", "baz": 1}

    def test_kwargs_none_becomes_empty_dict(self):
        step = _row_to_step(_make_step_dict(kwargs_json=None))
        assert step.transformation.kwargs == {}

    def test_kwargs_empty_string_becomes_empty_dict(self):
        step = _row_to_step(_make_step_dict(kwargs_json=""))
        assert step.transformation.kwargs == {}

    def test_transformation_id_is_preserved(self):
        step = _row_to_step(_make_step_dict(transformation_id="donothing_transformation"))
        assert step.transformation.transformation_id == "donothing_transformation"


# ---------------------------------------------------------------------------
# load_steps
# ---------------------------------------------------------------------------


class TestLoadSteps:
    @pytest.fixture()
    def config_df(self, spark: SparkSession):
        data = [
            _make_pipeline_row_dict(
                pipeline_id="pipeline_001",
                steps=[
                    _make_step_dict(transformation_id="dummy"),
                    _make_step_dict(transformation_id="donothing", fully_qualified_name="other_view"),
                ],
            ),
            _make_pipeline_row_dict(
                pipeline_id="pipeline_002",
                steps=[_make_step_dict(transformation_id="dummy")],
            ),
        ]
        return spark.createDataFrame(data, schema=CONFIG_SCHEMA)

    def test_returns_steps_for_matching_pipeline_id(self, config_df):
        steps = load_steps(config_df, "pipeline_001")
        assert len(steps) == 2

    def test_filters_out_other_pipeline_ids(self, config_df):
        steps = load_steps(config_df, "pipeline_002")
        assert len(steps) == 1
        assert steps[0].transformation.transformation_id == "dummy"

    def test_raises_for_unknown_pipeline_id(self, config_df):
        with pytest.raises(ValueError, match="pipeline_id='unknown'"):
            load_steps(config_df, "unknown")


# ---------------------------------------------------------------------------
# create_transformation_step
# ---------------------------------------------------------------------------


def _make_step(
    target_type: str = "temporary_view",
    fully_qualified_name: str = "my_view",
    comment: str | None = None,
    schema: str | None = None,
    expectations: list[dict] | None = None,
    transformation_id: str = "dummy",
) -> TransformationStep:
    return TransformationStep(
        sources=[{"fully_qualified_name": "cat.sch.tbl", "alias": "df"}],
        transformation={"transformation_id": transformation_id, "kwargs": {}},
        target={
            "fully_qualified_name": fully_qualified_name,
            "target_type": target_type,
            "comment": comment,
            "schema": schema,
            "expectations": expectations or [],
        },
    )


def _make_ctx() -> PipelineContext:
    """PipelineContext with passthrough mock decorators."""

    def _mock_decorator():
        # outer(**kwargs) returns inner; inner(fn) passes fn through
        inner = MagicMock(side_effect=lambda fn: fn)
        outer = MagicMock(return_value=inner)
        return outer

    return PipelineContext(
        spark=MagicMock(),
        transformation_registry={"dummy": MagicMock()},
        expectation_decorators={
            ExpectationType.EXPECT: _mock_decorator(),
            ExpectationType.EXPECT_OR_DROP: _mock_decorator(),
            ExpectationType.EXPECT_OR_FAIL: _mock_decorator(),
        },
        target_decorators={
            TargetType.TABLE: _mock_decorator(),
            TargetType.MATERIALIZED_VIEW: _mock_decorator(),
            TargetType.TEMPORARY_VIEW: _mock_decorator(),
        },
    )


class TestCreateTransformationStep:
    def test_returns_callable(self):
        result = create_transformation_step(_make_step(), _make_ctx())
        assert callable(result)

    def test_temporary_view_decorator_called_with_name_only(self):
        ctx = _make_ctx()
        create_transformation_step(_make_step(target_type="temporary_view", fully_qualified_name="my_view"), ctx)
        ctx.target_decorators[TargetType.TEMPORARY_VIEW].assert_called_once_with(name="my_view")

    def test_temporary_view_decorator_not_passed_comment_or_schema(self):
        ctx = _make_ctx()
        create_transformation_step(
            _make_step(target_type="temporary_view", fully_qualified_name="my_view", comment="c", schema="col STRING"),
            ctx,
        )
        call_kwargs = ctx.target_decorators[TargetType.TEMPORARY_VIEW].call_args.kwargs
        assert "comment" not in call_kwargs
        assert "schema" not in call_kwargs

    def test_table_decorator_called_with_name_and_comment(self):
        ctx = _make_ctx()
        create_transformation_step(
            _make_step(target_type="table", fully_qualified_name="cat.sch.tbl", comment="my comment"), ctx
        )
        ctx.target_decorators[TargetType.TABLE].assert_called_once_with(name="cat.sch.tbl", comment="my comment")

    def test_schema_passed_to_decorator_when_present(self):
        ctx = _make_ctx()
        create_transformation_step(
            _make_step(target_type="table", fully_qualified_name="cat.sch.tbl", schema="col1 STRING"), ctx
        )
        call_kwargs = ctx.target_decorators[TargetType.TABLE].call_args.kwargs
        assert call_kwargs["schema"] == "col1 STRING"

    def test_schema_not_passed_to_decorator_when_absent(self):
        ctx = _make_ctx()
        create_transformation_step(
            _make_step(target_type="table", fully_qualified_name="cat.sch.tbl", schema=None), ctx
        )
        call_kwargs = ctx.target_decorators[TargetType.TABLE].call_args.kwargs
        assert "schema" not in call_kwargs

    def test_expectation_decorator_called_with_name_and_condition(self):
        ctx = _make_ctx()
        create_transformation_step(
            _make_step(
                expectations=[{"expectation_type": "expect", "name": "col not null", "condition": "col IS NOT NULL"}]
            ),
            ctx,
        )
        ctx.expectation_decorators[ExpectationType.EXPECT].assert_called_once_with("col not null", "col IS NOT NULL")

    def test_all_expectation_decorators_applied(self):
        ctx = _make_ctx()
        create_transformation_step(
            _make_step(
                expectations=[
                    {"expectation_type": "expect", "name": "exp1", "condition": "a > 0"},
                    {"expectation_type": "expect_or_drop", "name": "exp2", "condition": "b IS NOT NULL"},
                ]
            ),
            ctx,
        )
        ctx.expectation_decorators[ExpectationType.EXPECT].assert_called_once()
        ctx.expectation_decorators[ExpectationType.EXPECT_OR_DROP].assert_called_once()

    def test_expectations_applied_in_reverse_order(self):
        call_order = []
        ctx = _make_ctx()

        def _tracking_decorator(name, condition):
            call_order.append(name)
            return lambda fn: fn

        ctx.expectation_decorators[ExpectationType.EXPECT] = _tracking_decorator
        ctx.expectation_decorators[ExpectationType.EXPECT_OR_DROP] = _tracking_decorator

        create_transformation_step(
            _make_step(
                expectations=[
                    {"expectation_type": "expect", "name": "first", "condition": "a > 0"},
                    {"expectation_type": "expect_or_drop", "name": "second", "condition": "b IS NOT NULL"},
                ]
            ),
            ctx,
        )
        # reversed: "second" decorator applied before "first"
        assert call_order == ["second", "first"]
