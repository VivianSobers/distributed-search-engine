import os

import pytest

from dse.spark_session import build_session


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse_dir = tmp_path_factory.mktemp("warehouse")
    warehouse_uri = f"file://{warehouse_dir}"
    iceberg_jar = os.environ.get("ICEBERG_JAR", "")
    session = build_session(
        "dse-tests",
        **{
            "spark.master": "local[2]",
            "spark.sql.shuffle.partitions": "2",
            "spark.ui.enabled": "false",
            "spark.ui.showConsoleProgress": "false",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.iceberg_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg_catalog.type": "hadoop",
            "spark.sql.catalog.iceberg_catalog.warehouse": warehouse_uri,
            "spark.hadoop.fs.defaultFS": "file:///",
            "spark.jars": iceberg_jar,
        },
    )
    yield session
    session.stop()
