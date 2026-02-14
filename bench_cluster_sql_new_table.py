"""Ali Rahimi Cluster + SQL Connector: materialize as a new table."""

import os

import databricks.sdk as dbx_sdk

import configuration

with configuration.benchmark_sql(
    configuration.result_path(__file__),
    f"/sql/protocolv1/o/{dbx_sdk.WorkspaceClient().get_workspace_id()}/"
    + os.environ["DATABRICKS_CLUSTER_ID"],
) as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            f"CREATE OR REPLACE TABLE "
            f"{configuration.CATALOG}.default.swolness_cluster_sql_new AS "
            + configuration.QUERY
        )
