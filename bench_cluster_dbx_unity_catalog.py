"""Ali Rahimi Cluster + Databricks Connector: materialize as Unity Catalog table.

BUG: crashes with DATA_SOURCE_NOT_FOUND for "tahoe" (Delta's internal name).

The CREATE OR REPLACE TABLE DDL implicitly creates a Delta table (UC's
default).  During analysis the server calls DataSource.lookupDataSource("tahoe")
which uses Java's ServiceLoader to find the registered DataSourceRegister.

On DBR 18.0.x, Spark Connect sessions run inside an ArtifactManager ClassLoader
(see ArtifactManager.withClassLoaderIfNeeded in the stack trace) that doesn't
inherit the META-INF/services registrations from the parent ClassLoader.  So
ServiceLoader never finds the "tahoe" provider, falls back to loading
tahoe.DefaultSource by class name, and throws ClassNotFoundException.

This is the same ServiceLoader bug noted in bench_cluster_dbx_parquet.py.  The
parquet benchmark works around it by using the fully-qualified V2 class name,
which is loaded directly and bypasses ServiceLoader.  That workaround doesn't
transfer here because Unity Catalog identifies Delta by the short name "delta"/
"tahoe" -- passing the FQ class makes UC reject the table as "Non-Delta".
"""

from pathlib import Path

import configuration

with configuration.benchmark_spark(
    Path(__file__).resolve().with_suffix(".result.txt"),
) as spark:
    spark.sql(
        f"CREATE OR REPLACE TABLE "
        f"{configuration.CATALOG}.default.swolness_cluster_dbx_uc AS "
        + configuration.QUERY
    )
