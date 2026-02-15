"""Ali Rahimi Cluster + SQL Connector: materialize as a delta table."""

import os

import databricks.sdk as dbx_sdk

import configuration

connection = configuration.get_sql_connection(
    f"/sql/protocolv1/o/{dbx_sdk.WorkspaceClient().get_workspace_id()}/"
    + os.environ["DATABRICKS_CLUSTER_ID"],
)
try:
    with configuration.benchmark(configuration.result_path(__file__)):
        with connection.cursor() as cursor:
            cursor.execute(
                f"CREATE OR REPLACE TABLE "
                f"{configuration.CATALOG}.default.swolness_cluster_sql_delta "
                "USING DELTA AS "
                + configuration.QUERY
            )
finally:
    connection.close()
