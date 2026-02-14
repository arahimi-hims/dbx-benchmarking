"""Ads Looker Warehouse + SQL Connector: materialize as a new table."""

import configuration

with configuration.benchmark_sql(
    configuration.result_path(__file__),
    f"/sql/1.0/warehouses/{configuration.WAREHOUSE_ID}",
) as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            f"CREATE OR REPLACE TABLE "
            f"{configuration.CATALOG}.default.swolness_warehouse_sql AS "
            + configuration.QUERY
        )
