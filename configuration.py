import contextlib
import logging
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

# Modify the environment variables to include databricks credentials.
dotenv.load_dotenv(Path(__file__).resolve().parent / ".env", override=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

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

WAREHOUSE_ID = "749a06d455c0aa5b"
CATALOG = f"us_mle_{os.environ['USER']}_gold"


def _check_warehouse_health(warehouse_id: str) -> None:
    ws = databricks.sdk.WorkspaceClient()

    # Ensure the warehouse is running. If it's stopped, start it. Otherwise,
    # keep waiting.
    while True:
        warehouse = ws.warehouses.get(warehouse_id)
        if warehouse.state.value == "STOPPED":
            logging.info(
                f"Warehouse {warehouse.name!r} ({warehouse.id}) "
                f"is STOPPED — starting it …"
            )
            ws.warehouses.start(warehouse_id)
            time.sleep(20)
            continue
        if warehouse.state.value != "RUNNING":
            logging.info(
                f"Warehouse {warehouse.name!r} ({warehouse.id}) "
                f"is {warehouse.state.value}, not RUNNING — retrying in 20 s …"
            )
            time.sleep(20)
            continue
        if warehouse.health and warehouse.health.status.value != "HEALTHY":
            logging.info(
                f"Warehouse {warehouse.name!r} ({warehouse.id}) "
                f"health is {warehouse.health.status.value}, not HEALTHY"
                f" — retrying in 20 s …"
            )
            time.sleep(20)
            continue
        logging.info(
            f"Health check: warehouse {warehouse.name!r} is RUNNING and HEALTHY"
        )
        break


def _check_cluster_health(cluster_id: str) -> None:
    client = databricks.sdk.WorkspaceClient()

    # Ensure the cluster is running. If it's terminated, start it. Otherwise,
    # keep waiting.
    while True:
        cluster = client.clusters.get(cluster_id)
        if cluster.state.value == "TERMINATED":
            logging.info(
                f"Cluster {cluster.cluster_name!r} ({cluster.cluster_id}) "
                f"is TERMINATED — starting it …"
            )
            client.clusters.start(cluster_id)
            time.sleep(20)
            continue
        if cluster.state.value != "RUNNING":
            logging.info(
                f"Cluster {cluster.cluster_name!r} ({cluster.cluster_id}) "
                f"is {cluster.state.value}, not RUNNING — retrying in 20 s …"
            )
            time.sleep(20)
            continue
        if not cluster.driver:
            logging.info(
                f"Cluster {cluster.cluster_name!r} ({cluster.cluster_id}) "
                f"is RUNNING but has no driver node — retrying in 20 s …"
            )
            time.sleep(20)
            continue
        logging.info(
            f"Health check: cluster {cluster.cluster_name!r} is RUNNING, driver present"
        )
        break


def _check_sql_resource_health(http_path: str) -> None:
    """Determines whether http_path targets a warehouse or cluster and
    verifies the resource is running and healthy.
    """
    resource_id = http_path.rsplit("/", 1)[-1]
    if "/warehouses/" in http_path:
        _check_warehouse_health(resource_id)
    else:
        _check_cluster_health(resource_id)


def result_path(script: str) -> Path:
    """Return the result path for a benchmark script inside ``results/``."""
    results_dir = Path(__file__).resolve().parent / "results"
    results_dir.mkdir(exist_ok=True)
    return results_dir / Path(script).with_suffix(".txt").name


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
        logging.info(msg)
        result_file.write_text(msg + "\n")
    except Exception:
        msg = (
            f"CRASH after {time.perf_counter() - start:.1f}s"
            f"\n\n{traceback.format_exc()}"
        )
        logging.info(msg)
        result_file.write_text(msg)
        raise


def get_sql_connection(http_path: str) -> databricks.sql.client.Connection:
    """Returns a SQL connection after verifying the target resource is healthy.

    The caller is responsible for closing the connection when done.
    """
    _check_sql_resource_health(http_path)
    return databricks.sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"]
        .replace("https://", "")
        .rstrip("/"),
        http_path=http_path,
        access_token=os.environ["DATABRICKS_TOKEN"],
        catalog=CATALOG,
    )


def get_spark() -> databricks.connect.DatabricksSession:
    "Returns a Spark session after verifying the cluster is healthy."
    _check_cluster_health(os.environ["DATABRICKS_CLUSTER_ID"])
    return databricks.connect.DatabricksSession.builder.getOrCreate()
