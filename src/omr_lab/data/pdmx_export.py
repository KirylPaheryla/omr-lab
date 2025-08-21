from __future__ import annotations

import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from music21 import meter, note, pitch, stream
from omr_lab.common.logging import log


@dataclass(frozen=True)
class ExportTask:
    src: Path
    dst: Path
    ext: str  # "musicxml" | "mxl"


def _safe_get(d: dict, key: str, default: Any = None) -> Any:
    try:
        v = d.get(key, default)
    except Exception:
        return default
    return v


def _to_quarters(value: Any, resolution: int) -> float:
    try:
        return float(value) / float(resolution) * 1.0
    except Exception:
        return 0.0


def _build_score_from_pdmx_json(data: dict[str, Any]) -> stream.Score:
    sc = stream.Score()

    ts_num = 4
    ts_den = 4
    time_sigs = _safe_get(data, "time_signatures")
    if isinstance(time_sigs, list) and time_sigs:
        ts0 = time_sigs[0]
        _n = _safe_get(ts0, "numerator")
        _d = _safe_get(ts0, "denominator")
        try:
            ts_num = int(_n) if _n is not None else 4
            ts_den = int(_d) if _d is not None else 4
        except Exception:
            ts_num, ts_den = 4, 4

    resolution = _safe_get(data, "resolution", 480)
    try:
        resolution = int(resolution)
        if resolution <= 0:
            resolution = 480
    except Exception:
        resolution = 480

    tracks = _safe_get(data, "tracks", [])
    if not isinstance(tracks, list):
        tracks = []

    for ti, tr in enumerate(tracks):
        part = stream.Part()
        part.partName = str(_safe_get(tr, "name", f"Track {ti+1}"))

        part.append(meter.TimeSignature(f"{ts_num}/{ts_den}"))

        notes = _safe_get(tr, "notes", [])
        if not isinstance(notes, list):
            notes = []

        for nobj in notes:
            midi = _safe_get(nobj, "pitch")
            start = _safe_get(nobj, "time", 0)
            dur = _safe_get(nobj, "duration", 0)

            if midi is None or start is None or dur is None:
                continue
            try:
                midi_int = int(midi)
            except Exception:
                continue

            p = pitch.Pitch()
            try:
                p.midi = midi_int
            except Exception:
                continue

            n = note.Note()
            n.pitch = p

            ql = max(0.0, _to_quarters(dur, resolution))
            n.duration.quarterLength = ql if ql > 0 else 0.25
            off_q = max(0.0, _to_quarters(start, resolution))
            n.offset = off_q

            lyr = _safe_get(nobj, "lyric")
            if isinstance(lyr, str) and lyr.strip():
                n.addLyric(lyr.strip())
            else:
                lyr_list = _safe_get(nobj, "lyrics", [])
                if isinstance(lyr_list, list):
                    for s in lyr_list:
                        if isinstance(s, str) and s.strip():
                            n.addLyric(s.strip())

            part.insert(off_q, n)

        try:
            part.makeMeasures(inPlace=True)
        except Exception:
            pass

        sc.insert(0, part)

    return sc


def _write_musicxml_from_json(json_path: Path, out_path: Path, ext: str) -> None:
    data = json.loads(json_path.read_text(encoding="utf-8"))
    sc = _build_score_from_pdmx_json(data)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    if ext == "mxl":
        sc.write("mxl", fp=str(out_path))
    else:
        sc.write("musicxml", fp=str(out_path))


def _export_one(task: ExportTask) -> tuple[Path, bool, str | None]:
    try:
        _write_musicxml_from_json(task.src, task.dst, task.ext)
        return task.src, True, None
    except Exception as e:
        return task.src, False, str(e)


def _gather_pdmx_json(root: Path) -> list[Path]:
    base = root / "data"
    if not base.exists():
        base = root
    return [p for p in base.rglob("*.json") if p.is_file()]


def export_pdmx_to_musicxml(
    root: Path,
    out_dir: Path,
    *,
    jobs: int = 1,
    lyrics_only: bool = False,
    no_conflict_only: bool = False,
    ext: str = "musicxml",
    csv_path: Path | None = None,
) -> dict[str, int]:
    _ = lyrics_only, no_conflict_only, csv_path
    out_dir.mkdir(parents=True, exist_ok=True)

    all_json = _gather_pdmx_json(root)
    total = len(all_json)
    if total == 0:
        log.warning("pdmx_export_no_candidates")
        return {"exported": 0, "failed": 0, "total": 0}

    tasks: list[ExportTask] = []
    for j in all_json:
        rel = j.relative_to(root)
        dst_rel = rel.with_suffix("." + ext)
        dst = out_dir / dst_rel
        tasks.append(ExportTask(src=j, dst=dst, ext=ext))

    exported = 0
    failed = 0

    if jobs <= 1:
        for i, t in enumerate(tasks, start=1):
            _, ok, err = _export_one(t)
            exported += int(ok)
            failed += int(not ok)
            if not ok:
                log.warning("pdmx_export_failed", error=str(err))
            if i % 500 == 0:
                log.info("pdmx_export_progress", done=i, total=total)
    else:
        with ProcessPoolExecutor(max_workers=jobs) as ex:
            futs = [ex.submit(_export_one, t) for t in tasks]
            for i, fut in enumerate(as_completed(futs), start=1):
                _, ok, err = fut.result()
                exported += int(ok)
                failed += int(not ok)
                if not ok:
                    log.warning("pdmx_export_failed", error=str(err))
                if i % 500 == 0:
                    log.info("pdmx_export_progress", done=i, total=total)

    summary = {"exported": exported, "failed": failed, "total": total}
    log.info("pdmx_export_done", **summary)
    return summary
