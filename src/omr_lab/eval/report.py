from __future__ import annotations

import csv
from pathlib import Path


def build_report(source_dir: Path, out_txt: Path) -> None:
    m = source_dir / "metrics.csv"
    lines: list[str] = []
    if m.exists():
        with m.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                metric = row.get("metric")
                value = row.get("value")
                if metric and value:
                    lines.append(f"{metric}: {value}")
    else:
        lines.append("metrics.csv not found")
    out_txt.parent.mkdir(parents=True, exist_ok=True)
    out_txt.write_text("\n".join(lines) + "\n", encoding="utf-8")
