# Databricks Benchmarking

I've tried various ways to run a SQL query and materialize the results.

The main findings are that:

1. Using Databricks Connect introduces major slowdowns.
2. Using a Cluster instead of a Warehouse introducues a major slowdown even if
   you connect to the Cluster using the SQL Connector.

The performance differences are on the order of minutes, so substantial. In
fact, using Databricks Connect is slower than materializing the results **by
downloading them my laptop** over the public internet, when it's hotspotted off an LTE phone connection.

I'm keen to understand why Databricks Connect and the Clusters are slow slow,
even though I'm executing the same SQL query in all cases. The impact of this
result is that we're better off materializing data out of Databricks quickly,
then either transferring it to a laptop, to AWS tooling, or even shuttling the
result back to GCP for exploratory work.

# Detailed results

All scripts run the same query but vary across three dimensions: compute
target, connector, and where results are materialized.

| Script                                                                   | Running Time | Compute   | Connector                  | Materialization                                                     |
| ------------------------------------------------------------------------ | ------------ | --------- | -------------------------- | ------------------------------------------------------------------- |
| [bench_cluster_dbx_parquet.py](bench_cluster_dbx_parquet.py)             | 635.7s       | Cluster   | Databricks Connect (Spark) | `/Workspace/Users/{USER}@forhims.com/swolness_pamphlet/assignments` |
| [bench_cluster_dbx_unity_catalog.py](bench_cluster_dbx_unity_catalog.py) | 504.6s       | Cluster   | Databricks Connect (Spark) | `{CATALOG}.default.swolness_cluster_dbx_uc`                         |
| [bench_warehouse_sql_download.py](bench_warehouse_sql_download.py)       | 501.6s       | Warehouse | SQL Connector              | download to my laptop                                               |
| [bench_cluster_sql_new_table.py](bench_cluster_sql_new_table.py)         | 472.4s       | Cluster   | SQL Connector              | `{CATALOG}.default.swolness_cluster_sql_new`                        |
| [bench_cluster_sql_delta_table.py](bench_cluster_sql_delta_table.py)     | 441.3s       | Cluster   | SQL Connector              | `{CATALOG}.default.swolness_cluster_sql_delta`                      |
| [bench_warehouse_sql_materialize.py](bench_warehouse_sql_materialize.py) | 306.9s       | Warehouse | SQL Connector              | `{CATALOG}.default.swolness_warehouse_sql`                          |

The fastest way to materialize a result is by saving it as a table using a
warehouse worker. Addiing a Cluster worker in the flow inclurs a 2 minute
slowdown (not counting for the spinup time of the Cluster). Downloding the
result to my laptop is only 30 seconds slower than the faster Cluster-based
approach. Bafflingly, asking the Cluster worker to save the results to a Parquet
or Delta file is _slower_ than asking it to do this through its SQL code path,
which I presume eventually ends up saving the file to a Delta file.

# Appendix: Instance details

I don't think it has to do with the size of the instance. The Cluster node I'm
using seems over-powered compared to the Warehouse node.

|                    | Warehouse             | Cluster                  |
| ------------------ | --------------------- | ------------------------ |
| Name               | mle generic warehouse | arahimi Personal Compute |
| Size / Spark ver   | Small                 | 18.0.x-scala2.13         |
| Serverless         | True                  | N/A                      |
| Workers            | 1–2 clusters          | 1–5                      |
| Driver instance    | (managed)             | m5d.xlarge               |
| Driver cores       | (managed)             | 4.0                      |
| Driver memory (MB) | (managed)             | 16384                    |
| Worker instance    | (managed)             | i3.4xlarge               |
| Worker cores       | (managed)             | 16.0                     |
| Worker memory (MB) | (managed)             | 124928                   |
| Photon             | True                  | PHOTON                   |
| Spot policy        | COST_OPTIMIZED        | SPOT_WITH_FALLBACK       |
| Data security mode | N/A                   | SINGLE_USER              |
