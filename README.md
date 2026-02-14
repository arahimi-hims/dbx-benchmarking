# Databricks Benchmarking

All scripts run the same query (experiment exposures joined with experiments metadata) but vary across three dimensions: compute target, connector, and where results are materialized.

| Script                                                                   | Running Time | Compute   | Connector                  | Materialization                                                     |
| ------------------------------------------------------------------------ | ------------ | --------- | -------------------------- | ------------------------------------------------------------------- |
| [bench_cluster_dbx_parquet.py](bench_cluster_dbx_parquet.py)             | 1021.1s      | Cluster   | Databricks Connect (Spark) | `/Workspace/Users/{USER}@forhims.com/swolness_pamphlet/assignments` |
| [bench_cluster_dbx_unity_catalog.py](bench_cluster_dbx_unity_catalog.py) | 1023.2s      | Cluster   | Databricks Connect (Spark) | `{CATALOG}.default.swolness_cluster_dbx_uc`                         |
| [bench_cluster_sql_delta_table.py](bench_cluster_sql_delta_table.py)     | 1062.7s      | Cluster   | SQL Connector              | `{CATALOG}.default.swolness_cluster_sql_delta`                      |
| [bench_cluster_sql_new_table.py](bench_cluster_sql_new_table.py)         | 835.0s       | Cluster   | SQL Connector              | `{CATALOG}.default.swolness_cluster_sql_new`                        |
| [bench_warehouse_sql_download.py](bench_warehouse_sql_download.py)       | 550.7s       | Warehouse | SQL Connector              | local `data/assignments.parquet`                                    |
| [bench_warehouse_sql_materialize.py](bench_warehouse_sql_materialize.py) | 251.9s       | Warehouse | SQL Connector              | `{CATALOG}.default.swolness_warehouse_sql`                          |

**Command**: Run each of the scripts in this directory that starts with `bench\_`
in parallel. As each finishes, update the above table by crawling through the
bench\_\*\_result.txt files and copying the timings or crash report into the **Running Time** column.
