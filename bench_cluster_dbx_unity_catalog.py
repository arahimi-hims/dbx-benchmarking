"""Ali Rahimi Cluster + Databricks Connector: materialize as Unity Catalog table."""

import configuration

with configuration.benchmark_spark(
    configuration.result_path(__file__),
) as spark:
    spark.sql(
        f"CREATE OR REPLACE TABLE "
        f"{configuration.CATALOG}.default.swolness_cluster_dbx_uc AS "
        + configuration.QUERY
    )
