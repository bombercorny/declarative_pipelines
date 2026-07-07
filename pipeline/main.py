from declarative_pipelines.executor import run_pipeline

# Injected by the DABs pipeline configuration (see databricks.yml)
run_pipeline(
    spark=spark,
    pipeline_id=spark.conf.get("pipeline_id"),
    config_table=spark.conf.get("config_table"),
)
