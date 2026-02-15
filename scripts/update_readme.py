"""Read times from results/*.txt and update the tables in README.md."""

import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
README = ROOT / "README.md"

# Matches a benchmark table row, capturing the script name and time value.
#   group 1 (unnamed): everything up to and including the | before the time
#   group 2 / "name":  script stem, e.g. "cluster_dbx_parquet"
#   group "time":       time value including unit, e.g. "635.7s"
ROW_RE = re.compile(
    r"^(\| \[(?P<name>\w+)\.py\]\(benchmarks/\2\.py\)\s+\|)"  # script column
    r"\s+(?P<time>[\d.]+s)"                                     # time value
)


def parse_time(path: Path) -> str | None:
    """Extract the time value from a result file like 'OK  635.7s'."""
    m = re.search(r"([\d.]+)s", path.read_text().strip())
    return m.group(1) if m else None


def update_readme() -> None:
    times: dict[str, str] = {}
    for p in (ROOT / "results").glob("*.txt"):
        t = parse_time(p)
        if t is not None:
            times[p.stem] = t  # e.g. "cluster_dbx_parquet" -> "635.7"

    if not times:
        print("No result files found.")
        return

    # Use split("\n") instead of splitlines() to preserve trailing newline.
    lines = README.read_text().split("\n")
    updated = 0

    for i, line in enumerate(lines):
        m = ROW_RE.match(line)
        if m and m.group("name") in times:
            old_time = m.group("time")
            new_time = f"{times[m.group('name')]}s"
            # Pad to keep column alignment.
            new_time_padded = new_time + " " * max(0, len(old_time) - len(new_time))
            lines[i] = line[: m.start("time")] + new_time_padded + line[m.end("time") :]
            updated += 1

    README.write_text("\n".join(lines))
    print(f"Updated {updated} rows in README.md")
    for name, t in sorted(times.items()):
        print(f"  {name}: {t}s")


def insert_resources() -> None:
    """Fetch warehouse/cluster details and rebuild the instance-details table in README."""
    import databricks.sdk
    import dotenv

    dotenv.load_dotenv(ROOT / ".env", override=True)

    ws = databricks.sdk.WorkspaceClient()

    # ── Fetch resources ──────────────────────────────────────────────────
    wh = ws.warehouses.get("749a06d455c0aa5b")

    cl = ws.clusters.get(os.environ["DATABRICKS_CLUSTER_ID"])

    driver_type_id = cl.driver_node_type_id or cl.node_type_id
    worker_type_id = cl.node_type_id

    node_types = {nt.node_type_id: nt for nt in ws.clusters.list_node_types().node_types}
    driver_nt = node_types.get(driver_type_id)
    worker_nt = node_types.get(worker_type_id)

    # ── Build rows ───────────────────────────────────────────────────────
    rows = [
        ("", "Warehouse", "Cluster"),
        ("Name", wh.name, cl.cluster_name),
        ("Size / Spark ver", wh.cluster_size, cl.spark_version),
        ("Serverless", str(wh.enable_serverless_compute), "N/A"),
        (
            "Workers",
            f"{wh.min_num_clusters}\u2013{wh.max_num_clusters} clusters",
            f"{cl.autoscale.min_workers}\u2013{cl.autoscale.max_workers}"
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

    # ── Format as markdown table ─────────────────────────────────────────
    col_widths = [
        max(len(str(r[0])) for r in rows),
        max(len(str(r[1])) for r in rows),
        max(len(str(r[2])) for r in rows),
    ]
    table_lines: list[str] = []
    for i, row in enumerate(rows):
        table_lines.append(
            f"| {str(row[0]):<{col_widths[0]}}"
            f" | {str(row[1]):<{col_widths[1]}}"
            f" | {str(row[2]):<{col_widths[2]}} |"
        )
        if i == 0:
            table_lines.append(
                f"| {'-' * col_widths[0]} | {'-' * col_widths[1]} | {'-' * col_widths[2]} |"
            )

    # ── Replace the table in README ──────────────────────────────────────
    lines = README.read_text().split("\n")

    # Find the "Appendix: Instance details" heading.
    header_idx = None
    for i, line in enumerate(lines):
        if "Appendix: Instance details" in line:
            header_idx = i
            break

    if header_idx is None:
        print("Could not find 'Appendix: Instance details' section in README.")
        return

    # Find the first and last table rows (lines starting with '|') after the header.
    table_start = table_end = None
    for i in range(header_idx + 1, len(lines)):
        if lines[i].startswith("|"):
            if table_start is None:
                table_start = i
            table_end = i
        elif table_start is not None:
            break

    if table_start is None:
        print("Could not find instance-details table in README.")
        return

    lines[table_start : table_end + 1] = table_lines
    README.write_text("\n".join(lines))
    print(f"Rebuilt instance-details table ({len(table_lines)} lines)")


if __name__ == "__main__":
    update_readme()
    insert_resources()
