from __future__ import annotations

import csv
from pathlib import Path


def compare_runs(run_dirs: list[Path], out_csv: Path) -> None:
    rows: list[tuple[str, str, str]] = []
    for run in run_dirs:
        m = run / "metrics.csv"
        if not m.exists():
            continue
        with m.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            data: dict[str, str] = {}
            for row in reader:
                metric = row.get("metric")
                value = row.get("value")
                if metric is not None and value is not None:
                    data[metric] = value
        rows.append((run.name, "f1", data.get("f1", "0.0")))
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["run", "metric", "value"])
        for r in rows:
            w.writerow(list(r))
