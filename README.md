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

| Script                                                                  | Running Time | Compute   | Connector                  | Materialization                                                     |
| ----------------------------------------------------------------------- | ------------ | --------- | -------------------------- | ------------------------------------------------------------------- |
| [cluster_dbx_parquet.py](benchmarks/cluster_dbx_parquet.py)             | 609.4s       | Cluster   | Databricks Connect (Spark) | `/Workspace/Users/{USER}@forhims.com/swolness_pamphlet/assignments` |
| [cluster_dbx_unity_catalog.py](benchmarks/cluster_dbx_unity_catalog.py) | 435.9s       | Cluster   | Databricks Connect (Spark) | `{CATALOG}.default.swolness_cluster_dbx_uc`                         |
| [warehouse_sql_download.py](benchmarks/warehouse_sql_download.py)       | 757.8s       | Warehouse | SQL Connector              | download to my laptop                                               |
| [cluster_sql_new_table.py](benchmarks/cluster_sql_new_table.py)         | 198.0s       | Cluster   | SQL Connector              | `{CATALOG}.default.swolness_cluster_sql_new`                        |
| [cluster_sql_delta_table.py](benchmarks/cluster_sql_delta_table.py)     | 190.5s       | Cluster   | SQL Connector              | `{CATALOG}.default.swolness_cluster_sql_delta`                      |
| [warehouse_sql_materialize.py](benchmarks/warehouse_sql_materialize.py) | 308.0s       | Warehouse | SQL Connector              | `{CATALOG}.default.swolness_warehouse_sql`                          |

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

|                    | Warehouse             | Cluster                |
| ------------------ | --------------------- | ---------------------- |
| Name               | mle generic warehouse | Ollie Personal compute |
| Size / Spark ver   | Small                 | 17.3.x-scala2.13       |
| Serverless         | True                  | N/A                    |
| Workers            | 1–2 clusters          | 4–4                    |
| Driver instance    | (managed)             | m6id.2xlarge           |
| Driver cores       | (managed)             | 8.0                    |
| Driver memory (MB) | (managed)             | 32768                  |
| Worker instance    | (managed)             | m6i.4xlarge            |
| Worker cores       | (managed)             | 16.0                   |
| Worker memory (MB) | (managed)             | 65536                  |
| Photon             | True                  | PHOTON                 |
| Spot policy        | COST_OPTIMIZED        | SPOT_WITH_FALLBACK     |
| Data security mode | N/A                   | USER_ISOLATION         |

# Appendix: Re-running benchmarks

You can run each benchmark by hand by running

```bash
uv run benchmarks/<benchmark-name>.py
```

To update the README from existing result, you can then run

```bash
uv run scripts/update_readme.py
```

Alternatively, you can re-run all, or a subset of, benchmarks and update the
README in one go:

```bash
# re-run all benchmarks
uv run scripts/update_readme.py benchmarks/*.py

# re-run specific benchmarks
uv run scripts/update_readme.py benchmarks/warehouse_*py
```

This will execute the given scripts sequentially via `uv run`, wait for each
to finish (writing its timing to `results/`), then refresh the README tables
with the new numbers.
