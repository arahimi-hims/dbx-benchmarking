"""Cluster + SQL Connector: Custom benchmark using credentials from .env file.

Cluster Configuration:
- Workers: 4 × m6i.4xlarge (16 cores, 64GB RAM each)
- Driver: m6i.2xlarge (8 cores, 32GB RAM)
- Total: 64 executor cores + 8 driver cores
- Runtime: 17.3 Photon
"""

import os
import time
from pathlib import Path

import databricks.sql
import databricks.sdk
import dotenv

# Load environment variables from .env file
dotenv.load_dotenv(Path(__file__).resolve().parent / ".env", override=True)

# Configuration from environment variables
DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
CLUSTER_ID = os.environ["CLUSTER_ID"]
CATALOG = os.environ["CATALOG"]
TABLE_NAME = os.environ["TABLE_NAME"]

# Benchmark query from configuration.py
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

def main():
    print(f"Starting benchmark (including connection setup)...")
    start_time = time.perf_counter()  # Timer starts immediately
    
    # Get workspace ID for cluster connection
    ws = databricks.sdk.WorkspaceClient(
        host=f"https://{DATABRICKS_HOST}",
        token=DATABRICKS_TOKEN
    )
    workspace_id = ws.get_workspace_id()
    
    cluster_http_path = f"/sql/protocolv1/o/{workspace_id}/{CLUSTER_ID}"
    
    print(f"Connecting to cluster at {DATABRICKS_HOST}...")
    print(f"Using cluster ID: {CLUSTER_ID}")
    print(f"Cluster HTTP path: {cluster_http_path}")
    print(f"Target table: {CATALOG}.default.{TABLE_NAME}")
    
    # Connect to Databricks cluster
    connection = databricks.sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=cluster_http_path,
        access_token=DATABRICKS_TOKEN,
        catalog=CATALOG,
    )
    
    try:
        
        with connection.cursor() as cursor:
            # Create table from query results
            create_table_sql = (
                f"CREATE OR REPLACE TABLE "
                f"{CATALOG}.default.{TABLE_NAME} AS "
                + QUERY
            )
            print(f"Executing: CREATE OR REPLACE TABLE {CATALOG}.default.{TABLE_NAME} AS ...")
            cursor.execute(create_table_sql)
        
        elapsed = time.perf_counter() - start_time
        print(f"\n✅ SUCCESS - Total time (connection + query): {elapsed:.1f}s")
        print(f"   Table location: {CATALOG}.default.{TABLE_NAME}")
        print(f"\n📊 Compare to baseline:")
        print(f"   bench_cluster_sql_new_table.py:   472.4s (original cluster)")
        print(f"   bench_cluster_sql_delta_table.py: 441.3s (original cluster)")
        print(f"   bench_cluster_sql_custom.py:      {elapsed:.1f}s (custom cluster)")
        
    except Exception as e:
        elapsed = time.perf_counter() - start_time if 'start_time' in locals() else 0
        print(f"\n❌ ERROR after {elapsed:.2f}s:")
        print(f"   {type(e).__name__}: {e}")
        raise
    finally:
        connection.close()
        print("\nConnection closed.")

if __name__ == "__main__":
    main()
