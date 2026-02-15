"""Ali Rahimi Cluster + Databricks Connector: materialize as Unity Catalog table."""

import configuration

spark = configuration.get_spark()

with configuration.benchmark(configuration.result_path(__file__)):
    spark.sql(
        f"CREATE OR REPLACE TABLE "
        f"{configuration.CATALOG}.default.swolness_cluster_dbx_uc AS "
        + configuration.QUERY
    )
