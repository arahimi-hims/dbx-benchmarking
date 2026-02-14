"""Ads Looker Warehouse + SQL Connector: download to laptop."""

from pathlib import Path

import pyarrow.parquet as pq

import configuration

LOCAL_DATA_DIR = Path(__file__).resolve().parent / "data"
LOCAL_DATA_DIR.mkdir(exist_ok=True)

with configuration.benchmark_sql(
    Path(__file__).resolve().with_suffix(".result.txt"),
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
