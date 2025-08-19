from __future__ import annotations

import csv
from pathlib import Path


def _collect_basenames(root: Path, exts: set[str]) -> set[str]:
    names: set[str] = set()
    for p in root.rglob("*"):
        if p.is_file() and p.suffix.lower() in exts:
            names.add(p.stem)
    return names


def eval_filelevel(pred_dir: Path, gt_dir: Path, out_csv: Path) -> None:
    exts = {".musicxml", ".xml", ".mxl"}
    pred = _collect_basenames(pred_dir, exts)
    gt = _collect_basenames(gt_dir, exts)
    tp = len(pred & gt)
    fp = len(pred - gt)
    fn = len(gt - pred)
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["metric", "value"])
        w.writerow(["precision", f"{precision:.4f}"])
        w.writerow(["recall", f"{recall:.4f}"])
        w.writerow(["f1", f"{f1:.4f}"])
        w.writerow(["tp", tp])
        w.writerow(["fp", fp])
        w.writerow(["fn", fn])
