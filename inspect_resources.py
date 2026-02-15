"""Compare the shape of the configured warehouse and cluster."""

import os

import databricks.sdk

import configuration

ws = databricks.sdk.WorkspaceClient()

# ── Fetch resources ──────────────────────────────────────────────────────────
wh = ws.warehouses.get(configuration.WAREHOUSE_ID)

cluster_id = os.environ["DATABRICKS_CLUSTER_ID"]
cl = ws.clusters.get(cluster_id)

driver_type_id = cl.driver_node_type_id or cl.node_type_id
worker_type_id = cl.node_type_id

node_types = {nt.node_type_id: nt for nt in ws.clusters.list_node_types().node_types}
driver_nt = node_types.get(driver_type_id)
worker_nt = node_types.get(worker_type_id)

# ── Side-by-side summary ─────────────────────────────────────────────────────
rows = [
    ("", "Warehouse", "Cluster"),
    ("Name", wh.name, cl.cluster_name),
    ("Size / Spark ver", wh.cluster_size, cl.spark_version),
    ("Serverless", str(wh.enable_serverless_compute), "N/A"),
    (
        "Workers",
        f"{wh.min_num_clusters}–{wh.max_num_clusters} clusters",
        f"{cl.autoscale.min_workers}–{cl.autoscale.max_workers}"
        if cl.autoscale
        else str(cl.num_workers),
    ),
    (
        "Driver instance",
        "(managed)",
        f"{driver_nt.node_instance_type.instance_type_id}"
        if driver_nt and driver_nt.node_instance_type
        else str(driver_type_id),
    ),
    (
        "Driver cores",
        "(managed)",
        str(driver_nt.num_cores) if driver_nt else "?",
    ),
    (
        "Driver memory (MB)",
        "(managed)",
        str(driver_nt.memory_mb) if driver_nt else "?",
    ),
    (
        "Worker instance",
        "(managed)",
        f"{worker_nt.node_instance_type.instance_type_id}"
        if worker_nt and worker_nt.node_instance_type
        else str(worker_type_id),
    ),
    (
        "Worker cores",
        "(managed)",
        str(worker_nt.num_cores) if worker_nt else "?",
    ),
    (
        "Worker memory (MB)",
        "(managed)",
        str(worker_nt.memory_mb) if worker_nt else "?",
    ),
    ("Photon", str(wh.enable_photon), str(cl.runtime_engine).rsplit(".", 1)[-1]),
    (
        "Spot policy",
        str(wh.spot_instance_policy).rsplit(".", 1)[-1],
        str(cl.aws_attributes and cl.aws_attributes.availability).rsplit(".", 1)[-1],
    ),
    ("Data security mode", "N/A", str(cl.data_security_mode).rsplit(".", 1)[-1]),
]

col_widths = [
    max(len(str(r[0])) for r in rows),
    max(len(str(r[1])) for r in rows),
    max(len(str(r[2])) for r in rows),
]

for i, row in enumerate(rows):
    line = (
        f"| {str(row[0]):<{col_widths[0]}}"
        f" | {str(row[1]):<{col_widths[1]}}"
        f" | {str(row[2]):<{col_widths[2]}} |"
    )
    print(line)
    if i == 0:
        print(
            f"| {'-' * col_widths[0]} | {'-' * col_widths[1]} | {'-' * col_widths[2]} |"
        )
