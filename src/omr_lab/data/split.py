from __future__ import annotations

import json
import random
from pathlib import Path


def _collect(ir_dir: Path) -> list[tuple[Path, bool]]:
    items: list[tuple[Path, bool]] = []
    for p in ir_dir.rglob("*.json"):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            items.append((p, bool(data.get("has_lyrics", False))))
        except Exception:
            continue
    return items


def stratified_split(
    ir_dir: Path,
    out_dir: Path,
    ratios: tuple[float, float, float] = (0.7, 0.15, 0.15),
    seed: int = 42,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    items = _collect(ir_dir)
    with_lyrics = [p for p in items if p[1]]
    without_lyrics = [p for p in items if not p[1]]

    def _split(lst: list[tuple[Path, bool]]) -> tuple[list[Path], list[Path], list[Path]]:
        random.Random(seed).shuffle(lst)
        n = len(lst)
        n_train = int(n * ratios[0])
        n_val = int(n * ratios[1])
        train = [p for p, _ in lst[:n_train]]
        val = [p for p, _ in lst[n_train : n_train + n_val]]
        test = [p for p, _ in lst[n_train + n_val :]]
        return train, val, test

    tr1, va1, te1 = _split(with_lyrics)
    tr2, va2, te2 = _split(without_lyrics)

    train = tr1 + tr2
    val = va1 + va2
    test = te1 + te2

    def _write_list(name: str, lst: list[Path]) -> None:
        (out_dir / f"{name}.txt").write_text(
            "\n".join(p.as_posix() for p in sorted(lst)) + "\n", encoding="utf-8"
        )

    _write_list("train", train)
    _write_list("val", val)
    _write_list("test", test)
