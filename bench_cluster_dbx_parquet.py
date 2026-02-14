"""Ali Rahimi Cluster + Databricks Connector: materialize as parquet to workspace."""

import os
from pathlib import Path

import configuration

with configuration.benchmark_spark(
    Path(__file__).resolve().with_suffix(".result.txt"),
) as spark:
    df = spark.sql(configuration.QUERY)
    df.cache()
    # Databricks Connect on DBR 18.0.x can't resolve short format names
    # ("parquet", "delta") in df.write due to a ServiceLoader bug in the
    # Connect write handler.  The fully-qualified V2 class bypasses that.
    df.write.mode("overwrite").format(
        "org.apache.spark.sql.execution.datasources"
        ".v2.parquet.ParquetDataSourceV2"
    ).save(
        f"/Workspace/Users/{os.environ['USER']}@forhims.com"
        f"/swolness_pamphlet/assignments"
    )
