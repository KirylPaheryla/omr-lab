from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

# Allowed category names for lyrics annotations
CategoryName = Literal["syllable", "text_line"]


@dataclass
class CocoImage:
    """Represents an image entry in COCO format."""

    id: int
    file_name: str
    width: int
    height: int


@dataclass
class CocoAnnotation:
    """Represents an annotation entry in COCO format."""

    id: int
    image_id: int
    category_id: int
    bbox: list[float]  # [x, y, w, h]
    iscrowd: int = 0
    text: str | None = None
    syllabic: str | None = None  # "single|begin|middle|end"
    note_id: str | None = None  # link to note from IR
    xml_id: str | None = None  # xml:id from SVG if extracted
    score: float | None = None  # reserved for future use


@dataclass
class CocoCategory:
    """Represents a category entry in COCO format."""

    id: int
    name: CategoryName
    supercategory: str


def default_categories() -> list[CocoCategory]:
    """Return default categories for lyrics (syllables and text lines)."""
    return [
        CocoCategory(id=1, name="syllable", supercategory="lyrics"),
        CocoCategory(id=2, name="text_line", supercategory="lyrics"),
    ]


def write_coco(
    path: Path,
    images: list[CocoImage],
    ann: list[CocoAnnotation],
    cats: list[CocoCategory] | None = None,
) -> None:
    """
    Write dataset to COCO JSON format.

    Args:
        path: Output JSON file path.
        images: List of image entries.
        ann: List of annotation entries.
        cats: Optional list of categories (defaults to `default_categories`).

    The function ensures the directory exists and writes
    JSON with UTF-8 encoding and pretty indentation.
    """
    data: dict[str, Any] = {
        "images": [asdict(i) for i in images],
        "annotations": [asdict(a) for a in ann],
        "categories": [asdict(c) for c in (cats or default_categories())],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
