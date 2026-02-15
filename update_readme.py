"""Read times from results/*.txt and update the table in README.md."""

import re
from pathlib import Path

RESULTS_DIR = Path(__file__).parent / "results"
README = Path(__file__).parent / "README.md"

# Matches a benchmark table row, capturing the script name and time value.
#   group 1 (unnamed): everything up to and including the | before the time
#   group 2 / "name":  script stem, e.g. "bench_cluster_dbx_parquet"
#   group "time":       time value including unit, e.g. "635.7s"
ROW_RE = re.compile(
    r"^(\| \[(?P<name>\w+)\.py\]\(\2\.py\)\s+\|)"  # script column
    r"\s+(?P<time>[\d.]+s)"                          # time value
)


def parse_time(path: Path) -> str | None:
    """Extract the time value from a result file like 'OK  635.7s'."""
    text = path.read_text().strip()
    m = re.search(r"([\d.]+)s", text)
    return m.group(1) if m else None


def update_readme() -> None:
    times: dict[str, str] = {}
    for p in RESULTS_DIR.glob("*.txt"):
        t = parse_time(p)
        if t is not None:
            times[p.stem] = t  # e.g. "bench_cluster_dbx_parquet" -> "635.7"

    if not times:
        print("No result files found.")
        return

    # Use split("\n") instead of splitlines() to preserve trailing newline.
    readme = README.read_text()
    lines = readme.split("\n")
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


if __name__ == "__main__":
    update_readme()
