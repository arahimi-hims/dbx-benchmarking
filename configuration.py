import contextlib
import os
import time
import traceback
from pathlib import Path
from typing import Iterator

import databricks.connect
import databricks.sdk
import databricks.sql
import databricks.sql.client
import dotenv

dotenv.load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=True)

QUERY = """\
SELECT
  re.id AS experiment_id,
  re.name AS experiment_name,
  re.status,
  e.session_id,
  e.user_id,
  get_json_object(e.event_properties, '$.variant_id') AS variant_id,
  e.event_time AS exposure_time
FROM us_dpe_production_silver.base_amplitude_sensitive.base_amplitude__events e
INNER JOIN us_dpe_production_silver.base_growthbook.base_growthbook__experiments re
  ON get_json_object(e.event_properties, '$.experiment_id') = re.name
WHERE
  e.event_type = 'experiment:expose'
  AND (re.status = 'running' OR re.date_created >= DATE_SUB(CURRENT_DATE(), 90))
  AND e.event_time >= '2025-01-01'
"""

QUERY = "SELECT 1"

WAREHOUSE_ID = "749a06d455c0aa5b"
CATALOG = f"us_mle_{os.environ['USER']}_gold"


def _check_warehouse_health(warehouse_id: str) -> None:
    warehouse = databricks.sdk.WorkspaceClient().warehouses.get(warehouse_id)
    if warehouse.state.value != "RUNNING":
        raise RuntimeError(
            f"Warehouse {warehouse.name!r} ({warehouse.id}) "
            f"is {warehouse.state.value}, not RUNNING"
        )
    if warehouse.health and warehouse.health.status.value != "HEALTHY":
        raise RuntimeError(
            f"Warehouse {warehouse.name!r} ({warehouse.id}) "
            f"health is {warehouse.health.status.value}, not HEALTHY"
        )
    print(f"Health check: warehouse {warehouse.name!r} is RUNNING and HEALTHY")


def _check_cluster_health(cluster_id: str) -> None:
    cluster = databricks.sdk.WorkspaceClient().clusters.get(cluster_id)
    if cluster.state.value != "RUNNING":
        raise RuntimeError(
            f"Cluster {cluster.cluster_name!r} ({cluster.cluster_id}) "
            f"is {cluster.state.value}, not RUNNING"
        )
    if not cluster.driver:
        raise RuntimeError(
            f"Cluster {cluster.cluster_name!r} ({cluster.cluster_id}) "
            f"is RUNNING but has no driver node"
        )
    print(f"Health check: cluster {cluster.cluster_name!r} is RUNNING, driver present")


def _check_sql_resource_health(http_path: str) -> None:
    """Determines whether http_path targets a warehouse or cluster and
    verifies the resource is running and healthy.
    """
    resource_id = http_path.rsplit("/", 1)[-1]
    if "/warehouses/" in http_path:
        _check_warehouse_health(resource_id)
    else:
        _check_cluster_health(resource_id)


@contextlib.contextmanager
def benchmark(
    result_file: Path,
) -> Iterator[None]:
    """Times a block and writes the result to a file.

    Captures exceptions so the script exits cleanly after writing the
    crash report.
    """
    start = time.perf_counter()
    try:
        yield
        msg = f"OK  {time.perf_counter() - start:.1f}s"
        print(msg)
        result_file.write_text(msg + "\n")
    except Exception:
        msg = (
            f"CRASH after {time.perf_counter() - start:.1f}s"
            f"\n\n{traceback.format_exc()}"
        )
        print(msg)
        result_file.write_text(msg)
        raise


@contextlib.contextmanager
def benchmark_sql(
    result_file: Path,
    http_path: str,
) -> Iterator[databricks.sql.client.Connection]:
    """Benchmarks a block that uses a SQL connection.

    Creates a connection, yields it, and closes it on exit.  Timing and
    crash reporting are handled by the base ``benchmark`` context manager.
    """
    _check_sql_resource_health(http_path)
    with benchmark(result_file):
        connection = databricks.sql.connect(
            server_hostname=os.environ["DATABRICKS_HOST"]
            .replace("https://", "")
            .rstrip("/"),
            http_path=http_path,
            access_token=os.environ["DATABRICKS_TOKEN"],
            catalog=CATALOG,
        )
        try:
            yield connection
        finally:
            connection.close()


@contextlib.contextmanager
def benchmark_spark(
    result_file: Path,
) -> Iterator[databricks.connect.DatabricksSession]:
    """Benchmarks a block that uses a Spark session.

    Creates a session, yields it, and closes it on exit.  Timing and
    crash reporting are handled by the base ``benchmark`` context manager.
    """
    _check_cluster_health(os.environ["DATABRICKS_CLUSTER_ID"])
    with benchmark(result_file):
        yield databricks.connect.DatabricksSession.builder.getOrCreate()
