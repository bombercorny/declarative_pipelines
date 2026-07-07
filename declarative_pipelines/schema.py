from pyspark.sql.types import ArrayType, StringType, StructField, StructType

_STEP_SCHEMA = StructType(
    [
        StructField(
            "sources",
            ArrayType(
                StructType(
                    [
                        StructField("fully_qualified_name", StringType()),
                        StructField("alias", StringType()),
                    ]
                )
            ),
        ),
        StructField(
            "transformation",
            StructType(
                [
                    StructField("transformation_id", StringType()),
                    StructField("kwargs", StringType()),
                ]
            ),
        ),
        StructField(
            "target",
            StructType(
                [
                    StructField("target_type", StringType()),
                    StructField("fully_qualified_name", StringType()),
                    StructField("comment", StringType()),
                    StructField("schema", StringType()),
                    StructField(
                        "expectations",
                        ArrayType(
                            StructType(
                                [
                                    StructField("expectation_type", StringType()),
                                    StructField("name", StringType()),
                                    StructField("condition", StringType()),
                                ]
                            )
                        ),
                    ),
                ]
            ),
        ),
    ]
)

CONFIG_SCHEMA = StructType(
    [
        StructField("pipeline_id", StringType()),
        StructField("pipeline_name", StringType()),
        StructField("pipeline_description", StringType()),
        StructField("steps", ArrayType(_STEP_SCHEMA)),
    ]
)
