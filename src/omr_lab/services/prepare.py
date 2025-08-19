from __future__ import annotations

import shutil
from collections.abc import Iterable
from pathlib import Path

from omr_lab.common.logging import log

SUPPORTED_EXT = {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp"}


def prepare_dataset(input_path: Path, output_path: Path) -> int:
    output_path.mkdir(parents=True, exist_ok=True)
    count = 0

    items: Iterable[Path]
    if input_path.is_dir():
        items = input_path.rglob("*")  # iterable из Path
    else:
        items = [input_path]  # список тоже Iterable[Path]

    for p in items:
        if p.is_file() and p.suffix.lower() in SUPPORTED_EXT:
            dst = output_path / p.name
            if dst.resolve() != p.resolve():
                shutil.copy2(p, dst)
            count += 1

    log.info("prepare_dataset_done", copied=count, output=str(output_path))
    return count
