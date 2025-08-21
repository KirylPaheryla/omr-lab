from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from omr_lab.common.logging import log


@dataclass
class QaReport:
    """Summary of dataset QA statistics."""

    images: int
    annotations: int
    syllables_with_bbox: int
    bbox_coverage_pct: float
    images_with_lyrics: int


def qa_coco(coco_path: Path, pages_csv: Path | None = None) -> QaReport:
    """
    Perform simple QA checks on a COCO dataset.

    Args:
        coco_path: Path to a COCO JSON file.
        pages_csv: Optional path to `pages.csv` manifest. Used to count
                   how many images contain lyrics.

    Returns:
        QaReport with dataset statistics:
          - total images
          - total annotations
          - number of syllables with bounding boxes
          - percentage of coverage (annotations with bbox > 0)
          - number of images that contain lyrics
    """
    data = json.loads(coco_path.read_text(encoding="utf-8"))
    ann = data.get("annotations", [])
    imgs = data.get("images", [])
    with_bbox = sum(
        1 for a in ann if any(float(x) > 0 for x in a.get("bbox", [0, 0, 0, 0]))
    )
    coverage = (with_bbox / len(ann) * 100.0) if ann else 0.0

    images_with_lyrics = 0
    if pages_csv and pages_csv.exists():
        # count by the has_lyrics column
        for line in pages_csv.read_text(encoding="utf-8").splitlines()[1:]:
            parts = line.split(",")
            if len(parts) >= 6 and parts[5].strip() in {"1", "true", "True"}:
                images_with_lyrics += 1

    rep = QaReport(
        images=len(imgs),
        annotations=len(ann),
        syllables_with_bbox=with_bbox,
        bbox_coverage_pct=coverage,
        images_with_lyrics=images_with_lyrics,
    )
    log.info("data_qa", **rep.__dict__)
    return rep
