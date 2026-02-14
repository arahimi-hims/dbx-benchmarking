"""Ali Rahimi Cluster + SQL Connector: materialize as a delta table."""

import os
from pathlib import Path

import databricks.sdk as dbx_sdk

import configuration

with configuration.benchmark_sql(
    Path(__file__).resolve().with_suffix(".result.txt"),
    f"/sql/protocolv1/o/{dbx_sdk.WorkspaceClient().get_workspace_id()}/"
    + os.environ["DATABRICKS_CLUSTER_ID"],
) as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            f"CREATE OR REPLACE TABLE "
            f"{configuration.CATALOG}.default.swolness_cluster_sql_delta "
            "USING DELTA AS "
            + configuration.QUERY
        )
