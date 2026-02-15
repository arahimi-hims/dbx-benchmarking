# Databricks Benchmarking

I've tried various ways to run a SQL query and materialize the results. I'm
finding that the results can vary by more than a factor of 4x. Surprisingly, one
of the fastest ways to do is to **materialize by downloading the result to my
laptop** over the public internet. This approach turns out to be faster than
any of the solutions that rely on a Databricks Cluster. It seems like the
Databricks Cluster infrastructure is slower than transferring data through my
phone (yes, to make my point, my laptop is hotspotted off of my phone over an
LTE connection).

All scripts run the same query but vary across three dimensions: compute
target, connector, and where results are materialized.

| Script                                                                   | Running Time | Compute   | Connector                  | Materialization                                                     |
| ------------------------------------------------------------------------ | ------------ | --------- | -------------------------- | ------------------------------------------------------------------- |
| [bench_cluster_dbx_parquet.py](bench_cluster_dbx_parquet.py)             | 1019.7s      | Cluster   | Databricks Connect (Spark) | `/Workspace/Users/{USER}@forhims.com/swolness_pamphlet/assignments` |
| [bench_cluster_dbx_unity_catalog.py](bench_cluster_dbx_unity_catalog.py) | 741.5s       | Cluster   | Databricks Connect (Spark) | `{CATALOG}.default.swolness_cluster_dbx_uc`                         |
| [bench_cluster_sql_delta_table.py](bench_cluster_sql_delta_table.py)     | 676.0s       | Cluster   | SQL Connector              | `{CATALOG}.default.swolness_cluster_sql_delta`                      |
| [bench_cluster_sql_new_table.py](bench_cluster_sql_new_table.py)         | 692.8s       | Cluster   | SQL Connector              | `{CATALOG}.default.swolness_cluster_sql_new`                        |
| [bench_warehouse_sql_download.py](bench_warehouse_sql_download.py)       | 553.4s       | Warehouse | SQL Connector              | local `data/assignments.parquet`                                    |
| [bench_warehouse_sql_materialize.py](bench_warehouse_sql_materialize.py) | 289.2s       | Warehouse | SQL Connector              | `{CATALOG}.default.swolness_warehouse_sql`                          |
