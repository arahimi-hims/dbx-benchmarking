"""Ali Rahimi Cluster + Databricks Connector: materialize as Unity Catalog table."""

from pathlib import Path

import configuration

with configuration.benchmark_spark(
    Path(__file__).resolve().with_suffix(".result.txt"),
) as spark:
    spark.sql(
        f"CREATE OR REPLACE TABLE "
        f"{configuration.CATALOG}.default.swolness_cluster_dbx_uc AS "
        + configuration.QUERY
    )
