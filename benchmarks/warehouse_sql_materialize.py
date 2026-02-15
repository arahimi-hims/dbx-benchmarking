"""Ads Looker Warehouse + SQL Connector: materialize as a new table."""

import configuration

connection = configuration.get_sql_connection(
    f"/sql/1.0/warehouses/{configuration.WAREHOUSE_ID}"
)
try:
    with configuration.benchmark(configuration.result_path(__file__)):
        with connection.cursor() as cursor:
            cursor.execute(
                f"CREATE OR REPLACE TABLE "
                f"{configuration.CATALOG}.default.swolness_warehouse_sql AS "
                + configuration.QUERY
            )
finally:
    connection.close()
