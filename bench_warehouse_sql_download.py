"""Ads Looker Warehouse + SQL Connector: download to laptop."""

import pyarrow.parquet as pq

import configuration

LOCAL_DATA_DIR = configuration.result_path(__file__).parent / "data"
LOCAL_DATA_DIR.mkdir(exist_ok=True)

with configuration.benchmark_sql(
    configuration.result_path(__file__),
    f"/sql/1.0/warehouses/{configuration.WAREHOUSE_ID}",
) as connection:
    with connection.cursor() as cursor:
        cursor.execute(configuration.QUERY)
        table = cursor.fetchall_arrow()

    pq.write_table(table, LOCAL_DATA_DIR / "assignments.parquet")
    print(
        f"Saved {table.num_rows} rows to "
        f"{LOCAL_DATA_DIR / 'assignments.parquet'}"
    )
