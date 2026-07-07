from dataclasses import dataclass, field
from enum import Enum


class TargetType(str, Enum):
    TABLE = "table"
    MATERIALIZED_VIEW = "materialized_view"
    TEMPORARY_VIEW = "temporary_view"


class ExpectationType(str, Enum):
    EXPECT = "expect"
    EXPECT_OR_DROP = "expect_or_drop"
    EXPECT_OR_FAIL = "expect_or_fail"


@dataclass
class Source:
    fully_qualified_name: str
    alias: str


@dataclass
class Expectation:
    expectation_type: ExpectationType
    name: str
    condition: str

    def __post_init__(self):
        self.expectation_type = ExpectationType(self.expectation_type)


@dataclass
class Target:
    fully_qualified_name: str
    target_type: TargetType
    comment: str = ""
    schema: str | None = None
    expectations: list[Expectation] = field(default_factory=list)

    def __post_init__(self):
        self.target_type = TargetType(self.target_type)
        self.comment = self.comment or ""
        self.expectations = [Expectation(**e) if isinstance(e, dict) else e for e in (self.expectations or [])]
        self._validate_fully_qualified_name()

    def _validate_fully_qualified_name(self) -> None:
        parts = self.fully_qualified_name.split(".")
        if self.target_type == TargetType.TEMPORARY_VIEW:
            if len(parts) != 1:
                raise ValueError(
                    f"Temporary view name must be a plain identifier without catalog or schema, "
                    f"got: '{self.fully_qualified_name}'"
                )
        else:
            if len(parts) != 3:
                raise ValueError(
                    f"Target '{self.target_type.value}' requires a fully qualified name "
                    f"(catalog.schema.table), got: '{self.fully_qualified_name}'"
                )


@dataclass
class Transformation:
    transformation_id: str
    kwargs: dict = field(default_factory=dict)

    def __post_init__(self):
        self.kwargs = self.kwargs or {}


@dataclass
class TransformationStep:
    sources: list[Source]
    transformation: Transformation
    target: Target

    def __post_init__(self):
        self.sources = [Source(**s) if isinstance(s, dict) else s for s in (self.sources or [])]
        if isinstance(self.transformation, dict):
            self.transformation = Transformation(**self.transformation)
        if isinstance(self.target, dict):
            self.target = Target(**self.target)
