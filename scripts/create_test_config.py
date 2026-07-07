from declarative_pipelines.connection import spark
from declarative_pipelines.schema import CONFIG_SCHEMA as schema

data = [
    (
        "pipeline_001",
        "SDP Test Pipeline",
        "Example pipeline using declarative approach.",
        [
            {
                "sources": [
                    {
                        "fully_qualified_name": "catalog.schema.sdp_source1",
                        "alias": "df1",
                    },
                    {
                        "fully_qualified_name": "catalog.schema.sdp_source2",
                        "alias": "df2",
                    },
                ],
                "transformation": {
                    "transformation_id": "dummy_transformation",
                    "kwargs": '{"foo": "hello", "bar": "world"}',
                },
                "target": {
                    "target_type": "temporary_view",
                    "fully_qualified_name": "target1",
                    "comment": "table comment",
                    "schema": "col1 BIGINT COMMENT 'col1 comment', col2 BIGINT COMMENT 'col2 comment', foo_column STRING COMMENT 'foo column', bar_column STRING COMMENT 'bar column'",
                    "expectations": [
                        {
                            "expectation_type": "expect_or_fail",
                            "name": "col1 must be positive",
                            "condition": "col1 >= 1",
                        },
                    ],
                },
            },
            {
                "sources": [{"fully_qualified_name": "target1", "alias": "df"}],
                "transformation": {"transformation_id": "donothing_transformation"},
                "target": {
                    "target_type": "materialized_view",
                    "fully_qualified_name": "catalog.schema.target2",
                    "comment": "table comment2",
                    "schema": "col1 BIGINT COMMENT 'col1 comment2', col2 BIGINT COMMENT 'col2 comment2!', foo_column STRING COMMENT 'foo column', bar_column STRING COMMENT 'bar column'",
                    "expectations": [
                        {
                            "expectation_type": "expect_or_fail",
                            "name": "col1 must be positive",
                            "condition": "col1 >= 1",
                        },
                        {
                            "expectation_type": "expect_or_drop",
                            "name": "bar column not null",
                            "condition": "bar_column IS NOT NULL",
                        },
                    ],
                },
            },
            {
                "sources": [{"fully_qualified_name": "target1", "alias": "df"}],
                "transformation": {"transformation_id": "donothing_transformation"},
                "target": {
                    "target_type": "materialized_view",
                    "fully_qualified_name": "catalog.schema.target3",
                    "expectations": [
                        {
                            "expectation_type": "expect_or_fail",
                            "name": "col1 must be positive",
                            "condition": "col1 >= 1",
                        },
                    ],
                },
            },
            {
                "sources": [
                    {
                        "fully_qualified_name": "catalog.schema.target3",
                        "alias": "df",
                    }
                ],
                "transformation": {
                    "transformation_id": "with_primary_key_counts",
                    "kwargs": '{"primary_key_column": ["col1"]}',
                },
                "target": {
                    "target_type": "materialized_view",
                    "fully_qualified_name": "catalog.schema.check_unique_target3",
                    "expectations": [
                        {
                            "expectation_type": "expect_or_fail",
                            "name": "col1 must be unique",
                            "condition": "count <= 1",
                        },
                    ],
                },
            },
        ],
    ),
]

df = spark.createDataFrame(data, schema)

df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("catalog.schema.declarative_pipeline_config")
