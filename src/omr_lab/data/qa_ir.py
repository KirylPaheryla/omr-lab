from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from omr_lab.common.ir import ScoreIR


@dataclass
class IRSummary:
    files_total: int
    files_with_lyrics: int
    parts: int
    measures: int
    notes: int
    lyrics: int
    empty_lyrics: int
    dangling_lyrics: int
    syllabic_counts: dict[str, int]
    error_txt_files: int
    failed_json: int


def _load_ir(path: Path) -> ScoreIR:
    # Быстрая валидация схемы через Pydantic
    text = path.read_text(encoding="utf-8")
    return ScoreIR.model_validate_json(text)


def qa_ir_dir(ir_dir: Path) -> tuple[IRSummary, list[dict[str, Any]]]:
    json_paths = sorted(ir_dir.rglob("*.json"))
    error_txt_paths = list(ir_dir.rglob("*.error.txt"))

    files_total = 0
    files_with_lyrics = 0
    parts = measures = notes = lyrics = 0
    empty_lyrics = dangling_lyrics = 0
    syllabic_counts: dict[str, int] = {"single": 0, "begin": 0, "middle": 0, "end": 0}
    failed_json = 0

    rows: list[dict[str, Any]] = []

    for jp in json_paths:
        try:
            ir = _load_ir(jp)
        except Exception:
            failed_json += 1
            continue

        files_total += 1
        if ir.has_lyrics:
            files_with_lyrics += 1

        parts += len(ir.parts)

        note_ids_all: set[str] = set()
        file_measures = 0
        file_notes = 0
        file_lyrics = 0
        file_empty_lyrics = 0
        file_dangling_lyrics = 0
        file_syll: dict[str, int] = {"single": 0, "begin": 0, "middle": 0, "end": 0}

        for part in ir.parts:
            for m in part.measures:
                file_measures += 1
                for n in m.notes:
                    note_ids_all.add(n.id)
                    file_notes += 1
                for t in m.lyrics:
                    file_lyrics += 1
                    if not t.text:
                        file_empty_lyrics += 1
                    if t.syllabic in file_syll:
                        file_syll[t.syllabic] += 1
                    else:
                        file_syll[t.syllabic] = 1
                    if t.note_id not in note_ids_all:
                        # Если lyric пришёл раньше, чем соответствующая нота в списке:
                        # проверим ещё раз «лениво» после сбора всех нот.
                        pass

        # Повторная проверка «висячих» лирик после того, как собрали все ноты
        for part in ir.parts:
            for m in part.measures:
                for t in m.lyrics:
                    if t.note_id not in note_ids_all:
                        file_dangling_lyrics += 1

        measures += file_measures
        notes += file_notes
        lyrics += file_lyrics
        empty_lyrics += file_empty_lyrics
        dangling_lyrics += file_dangling_lyrics
        for k, v in file_syll.items():
            syllabic_counts[k] = syllabic_counts.get(k, 0) + v

        rows.append(
            {
                "work_id": jp.stem,
                "has_lyrics": int(ir.has_lyrics),
                "parts": len(ir.parts),
                "measures": file_measures,
                "notes": file_notes,
                "lyrics": file_lyrics,
                "empty_lyrics": file_empty_lyrics,
                "dangling_lyrics": file_dangling_lyrics,
                "single": file_syll.get("single", 0),
                "begin": file_syll.get("begin", 0),
                "middle": file_syll.get("middle", 0),
                "end": file_syll.get("end", 0),
            }
        )

    summary = IRSummary(
        files_total=files_total,
        files_with_lyrics=files_with_lyrics,
        parts=parts,
        measures=measures,
        notes=notes,
        lyrics=lyrics,
        empty_lyrics=empty_lyrics,
        dangling_lyrics=dangling_lyrics,
        syllabic_counts=syllabic_counts,
        error_txt_files=len(error_txt_paths),
        failed_json=failed_json,
    )

    return summary, rows


def write_ir_csv(out_csv: Path, rows: list[dict[str, Any]]) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        out_csv.write_text("", encoding="utf-8")
        return
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
