import os
from functools import lru_cache

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession


def get_profile_name():
    return os.environ.get("DATABRICKS_PROFILE_NAME", "DEFAULT")


@lru_cache
def get_spark() -> SparkSession:
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()
    else:
        from databricks.connect import DatabricksSession
        from dotenv import load_dotenv

        load_dotenv()

        profile_name = get_profile_name()
        spark = DatabricksSession.builder.profile(profile_name).getOrCreate()
        return spark


@lru_cache
def get_workspace_client():
    return WorkspaceClient(profile=get_profile_name())


@lru_cache
def get_dbutils():
    w = get_workspace_client()
    return w.dbutils


spark = get_spark()
dbutils = get_dbutils()
workspace_client = get_workspace_client()
