import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    from declarative_pipelines.connection import spark as _spark

    return _spark
