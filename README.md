# Databricks Benchmarking

All scripts run the same query (experiment exposures joined with experiments metadata) but vary across three dimensions: compute target, connector, and where results are materialized.

| Script                                                                   | Compute   | Connector                  | Materialization                                     | Running Time                                                  |
| ------------------------------------------------------------------------ | --------- | -------------------------- | --------------------------------------------------- | ------------------------------------------------------------- |
| [bench_cluster_dbx_parquet.py](bench_cluster_dbx_parquet.py)             | Cluster   | Databricks Connect (Spark) | Workspace (parquet file)                            | 34.1s  |
| [bench_cluster_dbx_unity_catalog.py](bench_cluster_dbx_unity_catalog.py) | Cluster   | Databricks Connect (Spark) | Unity Catalog (hive_metastore table)                | 12.8s  |
| [bench_cluster_sql_delta_table.py](bench_cluster_sql_delta_table.py)     | Cluster   | SQL Connector              | Unity Catalog (delta table, explicit `USING DELTA`) | 15.6s  |
| [bench_cluster_sql_new_table.py](bench_cluster_sql_new_table.py)         | Cluster   | SQL Connector              | Unity Catalog (table, default format)               | 36.4s  |
| [bench_warehouse_sql_download.py](bench_warehouse_sql_download.py)       | Warehouse | SQL Connector              | Local machine (parquet download)                    | 12.8s  |
| [bench_warehouse_sql_materialize.py](bench_warehouse_sql_materialize.py) | Warehouse | SQL Connector              | Unity Catalog (table)                               | 110.2s |

**Command**: Run each of the scripts in this directory that starts with `bench\_`
in parallel. As each finishes, update the above table by crawling through the
bench\_\*\_result.txt files and copying the timings or crash report into the **Running Time** column.
