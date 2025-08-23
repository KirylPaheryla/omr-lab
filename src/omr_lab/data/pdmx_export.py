# src/omr_lab/data/pdmx_export.py
from __future__ import annotations

import json
import os
import shutil
import zipfile
from collections.abc import Iterable
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from music21 import (
    chord,
    converter,
    exceptions21,
    instrument,
    key,
    meter,
    note,
    stream,
    tempo,
    metadata,
)

from omr_lab.common.logging import log


# --------------------------------------------------------------------------------------
# Helpers: warnings, duration sanitization, IO
# --------------------------------------------------------------------------------------
def _silence_music21_warnings() -> None:
    """
    Suppress noisy music21 warnings (MIDI channels, MusicXML edge cases, etc.).
    """
    import warnings

    warning_cls = getattr(exceptions21, "Music21Warning", None)
    if warning_cls is not None:
        warnings.filterwarnings("ignore", category=warning_cls)
    warnings.filterwarnings("ignore", module=r"music21\.musicxml\.m21ToXml")
    warnings.filterwarnings("ignore", module=r"music21\.midi")


def _sanitize_for_musicxml(sc: stream.Score, *, min_denominator: int = 1024) -> None:
    """
    Clamp too-short durations so music21 can serialize them to MusicXML.
    E.g. if we encounter 1/2048 quarterLength, raise it to 1/1024 (by default).
    """
    # minimum allowed duration in quarter-length units
    min_ql = 1.0 / float(min_denominator)

    for el in sc.recurse().notesAndRests:
        try:
            ql = float(el.duration.quarterLength or 0.0)
        except Exception:
            continue
        if 0.0 < ql < min_ql:
            el.duration.quarterLength = min_ql


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------------------------------------------
# Reading PDMX JSON (MusicRender-like) and converting to music21 Score
# --------------------------------------------------------------------------------------
@dataclass
class _PDMXTrackNote:
    time: int
    duration: int
    pitch: int
    velocity: int | None = None


@dataclass
class _PDMXLyric:
    time: int
    lyric: str


@dataclass
class _PDMXTrack:
    name: str | None
    program: int | None
    is_drum: bool | None
    notes: list[_PDMXTrackNote]
    lyrics: list[_PDMXLyric]


@dataclass
class _PDMXDocument:
    resolution: int
    tracks: list[_PDMXTrack]
    time_signatures: list[dict[str, Any]]
    key_signatures: list[dict[str, Any]]
    tempos: list[dict[str, Any]]
    title: str | None = None


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _load_pdmx_json(json_path: Path) -> _PDMXDocument:
    """
    Load one PDMX JSON and extract the subset we need to render MusicXML.
    The schema in PDMX can vary; we try to be permissive and fall back to defaults.
    """
    data = json.loads(json_path.read_text(encoding="utf-8", errors="ignore"))

    resolution = _safe_int(data.get("resolution"), 480)  # default to 480 if missing

    # Optional high-level metadata
    title = None
    md = data.get("metadata") or {}
    if isinstance(md, dict):
        title = md.get("title") or md.get("name")

    # Global events
    time_signatures = data.get("time_signatures") or data.get("timeSignatures") or []
    key_signatures = data.get("key_signatures") or data.get("keySignatures") or []
    tempos_ = data.get("tempos") or []

    # Tracks
    raw_tracks = data.get("tracks") or []
    tracks: list[_PDMXTrack] = []
    for t in raw_tracks:
        if not isinstance(t, dict):
            continue

        name = t.get("name")
        program = t.get("program")
        is_drum = t.get("is_drum") or t.get("isDrum")

        # Notes
        raw_notes = t.get("notes") or []
        notes: list[_PDMXTrackNote] = []
        for n in raw_notes:
            if not isinstance(n, dict):
                continue
            pitch = n.get("pitch")
            time = n.get("time")
            dur = n.get("duration")
            if pitch is None or time is None or dur is None:
                continue
            notes.append(
                _PDMXTrackNote(
                    time=_safe_int(time, 0),
                    duration=_safe_int(dur, 0),
                    pitch=_safe_int(pitch, 60),
                    velocity=(
                        _safe_int(n.get("velocity"), None)
                        if n.get("velocity") is not None
                        else None
                    ),
                )
            )

        # Lyrics (if any)
        raw_lyrics = t.get("lyrics") or []
        lyrics: list[_PDMXLyric] = []
        for l in raw_lyrics:
            if not isinstance(l, dict):
                continue
            txt = (l.get("lyric") or "").strip()
            if not txt:
                continue
            lyrics.append(_PDMXLyric(time=_safe_int(l.get("time"), 0), lyric=txt))

        tracks.append(
            _PDMXTrack(
                name=name, program=program, is_drum=is_drum, notes=notes, lyrics=lyrics
            )
        )

    return _PDMXDocument(
        resolution=resolution,
        tracks=tracks,
        time_signatures=time_signatures,
        key_signatures=key_signatures,
        tempos=tempos_,
        title=title,
    )


def _add_global_events(sc: stream.Score, doc: _PDMXDocument) -> None:
    """
    Add first of time/key/tempo if present.
    """
    # Time signature
    ts = None
    if doc.time_signatures:
        ts0 = doc.time_signatures[0]
        num = _safe_int(ts0.get("numerator"), 4)
        den = _safe_int(ts0.get("denominator"), 4)
        ts = meter.TimeSignature(f"{num}/{den}")
    else:
        ts = meter.TimeSignature("4/4")
    sc.insert(0, ts)

    # Tempo (bpm)
    if doc.tempos:
        t0 = doc.tempos[0]
        qpm = float(t0.get("qpm") or t0.get("bpm") or 120.0)
        sc.insert(0, tempo.MetronomeMark(number=qpm))
    else:
        sc.insert(0, tempo.MetronomeMark(number=120))

    # Key signature (optional)
    if doc.key_signatures:
        k0 = doc.key_signatures[0]
        # Common fields in PDMX-like structures
        # prefer explicit fifths if present; else try 'root' + 'mode'
        if "fifths" in k0:
            sc.insert(0, key.KeySignature(_safe_int(k0.get("fifths"), 0)))
        else:
            try:
                root = str(k0.get("root_str") or k0.get("root") or "C")
                mode = str(k0.get("mode") or "major").lower()
                sc.insert(0, key.Key(root + " " + mode))
            except Exception:
                # ignore malformed key description
                pass


def _instrument_for_program(program: int | None) -> instrument.Instrument:
    """
    Return a generic Instrument (no MIDI channel binding) to avoid MIDI-channel warnings.
    """
    inst = instrument.Instrument()
    if program is not None:
        # store as metadata; do not set midiChannel to avoid 'out of midi channels' warnings
        inst.midiProgram = int(program)
    return inst


def _attach_lyrics_to_nearest_notes(
    part: stream.Part, lyrics: list[_PDMXLyric], resolution: int
) -> None:
    """
    Attach lyrics to the nearest note at the same onset time (coarse heuristic).
    """
    if not lyrics:
        return
    # Build simple onset -> notes map (rounded to ticks)
    onset_map: dict[int, list[note.Note]] = {}
    for n in part.recurse().notes:
        onset_tick = round(float(n.offset) * resolution)
        onset_map.setdefault(onset_tick, []).append(n)

    for lyr in lyrics:
        candidates = onset_map.get(int(lyr.time), [])
        if candidates:
            # For now attach as plain lyric (no syllabic splitting)
            candidates[0].lyric = lyr.lyric


def _pdmx_to_score(doc: _PDMXDocument) -> stream.Score:
    """
    Convert a loaded PDMX JSON (subset) into a music21 Score.
    """
    sc = stream.Score()
    if doc.title:
        if sc.metadata is None:
            sc.insert(0, metadata.Metadata())
        sc.metadata.title = doc.title

    _add_global_events(sc, doc)

    for t_idx, tr in enumerate(doc.tracks):
        # Create part
        part = stream.Part(id=f"P{t_idx+1}")
        part.partName = tr.name or f"Track {t_idx+1}"
        part.insert(0, _instrument_for_program(tr.program))

        # Notes
        res = max(1, int(doc.resolution))
        for nt in tr.notes:
            # Ignore weird zero/negative durations
            if nt.duration <= 0:
                continue

            ql = float(nt.duration) / res
            start_ql = float(nt.time) / res

            try:
                n = note.Note()
                n.pitch.midi = int(nt.pitch)
                n.duration.quarterLength = ql
                part.insert(start_ql, n)
            except Exception:
                # if pitch invalid; skip this note
                continue

        # Lyrics (optional)
        _attach_lyrics_to_nearest_notes(part, tr.lyrics, res)

        sc.insert(0, part)

    return sc


# --------------------------------------------------------------------------------------
# Candidate discovery & filters
# --------------------------------------------------------------------------------------
def _iter_json_candidates(
    root: Path,
    lyrics_only: bool,
    no_conflict_only: bool,
    csv_filter: set[str] | None,
) -> Iterable[Path]:
    """
    Find PDMX JSON files under <root>/data/**.json.
    If csv_filter is provided, restrict to those basenames (without extension).
    'lyrics_only' and 'no_conflict_only' are best-effort filters:
      - lyrics_only: quick scan for '"lyric"' strings in file
      - no_conflict_only: if a sidecar 'conflict' marker exists (heuristic), skip
    """
    data_dir = root / "data"
    if not data_dir.exists():
        # Some users place JSON directly under root
        data_dir = root

    for p in data_dir.rglob("*.json"):
        stem = p.stem

        if csv_filter is not None and stem not in csv_filter:
            continue

        if no_conflict_only:
            # Heuristic: if there's a sibling "<stem>.conflict" or json has "is_conflict": true
            conflict_marker = p.with_suffix(".conflict")
            if conflict_marker.exists():
                continue
            try:
                # lightweight: just search substring
                txt = p.read_text(encoding="utf-8", errors="ignore")
                if '"conflict": true' in txt or '"is_conflict": true' in txt:
                    continue
            except Exception:
                # if unreadable, let it pass (we'll fail later)
                pass

        if lyrics_only:
            try:
                # lightweight substring scan to avoid parsing huge JSONs
                txt = p.read_text(encoding="utf-8", errors="ignore")
                if '"lyric"' not in txt and '"lyrics"' not in txt:
                    continue
            except Exception:
                continue

        yield p


def _load_csv_filter(csv_path: Path | None) -> set[str] | None:
    """
    Load a CSV with an ID column named 'id' or 'stem' or 'basename' (without extension).
    If csv_path is None, return None.
    """
    if not csv_path:
        return None
    if not csv_path.exists():
        return None

    ids: set[str] = set()
    try:
        import csv

        with csv_path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            # pick the first available id-like column
            id_col = None
            for cand in ("id", "stem", "basename", "name"):
                if cand in (reader.fieldnames or []):
                    id_col = cand
                    break
            if id_col is None:
                # fallback: assume single-column csv of stems
                fh.seek(0)
                for line in fh:
                    line = line.strip()
                    if line:
                        ids.add(line)
                return ids

            for row in reader:
                v = (row.get(id_col) or "").strip()
                if v:
                    v = Path(v).stem  # normalize if paths present
                    ids.add(v)
    except Exception:
        # silently ignore CSV issues; behave as if no filter
        return None

    return ids


# --------------------------------------------------------------------------------------
# Export worker
# --------------------------------------------------------------------------------------
def _write_score(
    score: stream.Score,
    out_path: Path,
    *,
    ext: str,
    min_denominator: int,
    quiet_warnings: bool,
) -> None:
    """
    Sanitize and write the music21 score to MusicXML or MXL.
    """
    if quiet_warnings:
        _silence_music21_warnings()

    _sanitize_for_musicxml(score, min_denominator=min_denominator)

    wtype = "mxl" if ext.lower() == "mxl" else "musicxml"
    _ensure_parent_dir(out_path)

    # Write MusicXML or MXL
    score.write(wtype, fp=str(out_path))


def _export_one(
    args: tuple[Path, Path, str, int, bool]
) -> tuple[Path, bool, str | None]:
    """
    Worker for one JSON -> MusicXML/MXL export.
    Returns: (json_path, ok, error_msg)
    """
    json_path, out_root, ext, min_denominator, quiet_warnings = args
    try:
        # Keep relative layout under out_root
        # <root>/data/.../file.json -> <out_root>/.../file.musicxml
        # If input is not inside a 'data' folder, just mirror relative to root provided by caller.
        rel = None
        try:
            # Find 'data' anchor in path to preserve shard structure (a/1/..., etc.)
            parts = list(json_path.parts)
            if "data" in parts:
                idx = parts.index("data")
                # keep the 'data' segment so output mirrors input layout
                rel = Path(*parts[idx:])
            else:
                rel = Path(json_path.name)
        except Exception:
            rel = Path(json_path.name)

        out_path = (out_root / rel).with_suffix("." + ext.lower())

        # Skip if exists and newer than source
        if out_path.exists():
            try:
                if out_path.stat().st_mtime >= json_path.stat().st_mtime:
                    return json_path, True, None
            except Exception:
                pass

        # Parse JSON -> music21
        doc = _load_pdmx_json(json_path)
        score = _pdmx_to_score(doc)

        # Write
        _write_score(
            score,
            out_path,
            ext=ext,
            min_denominator=min_denominator,
            quiet_warnings=quiet_warnings,
        )

        return json_path, True, None
    except Exception as e:
        try:
            # Write sidecar error
            err_path = out_root / (json_path.stem + ".error.txt")
            _ensure_parent_dir(err_path)
            err_path.write_text(str(e), encoding="utf-8")
        except Exception:
            pass
        return json_path, False, str(e)


# --------------------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------------------
def export_pdmx_to_musicxml(
    root: Path,
    out_dir: Path,
    *,
    csv_path: Path | None = None,
    jobs: int = 1,
    lyrics_only: bool = False,
    no_conflict_only: bool = False,
    ext: str = "musicxml",
    quiet_warnings: bool = True,
    min_denominator: int = 1024,
) -> dict[str, int]:
    """
    Export PDMX MusicRender JSON into MusicXML/MXL files using music21.

    Args:
        root: path to PDMX root (folder containing 'data' shards or flat JSONs).
        out_dir: where to write MusicXML/MXL.
        csv_path: optional CSV filter (id/stem/basename column).
        jobs: parallel workers.
        lyrics_only: pre-filter by presence of '"lyric"' or '"lyrics"' in JSON text (quick & lossy).
        no_conflict_only: skip JSONs that look conflicted (heuristic).
        ext: 'musicxml' or 'mxl'.
        quiet_warnings: silence music21 warnings during writing.
        min_denominator: clamp durations shorter than 1/<min_denominator>.

    Returns:
        dict with keys: exported, failed, total
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # Optional filter list from CSV
    id_filter = _load_csv_filter(csv_path)

    # Collect candidates
    candidates = list(
        _iter_json_candidates(
            root=root,
            lyrics_only=lyrics_only,
            no_conflict_only=no_conflict_only,
            csv_filter=id_filter,
        )
    )
    total = len(candidates)
    if total == 0:
        log.warning("pdmx_export_no_candidates")
        return {"exported": 0, "failed": 0, "total": 0}

    # Parallel export
    exported = 0
    failed = 0
    tasks = [
        (p, out_dir, ext, int(min_denominator), bool(quiet_warnings))
        for p in candidates
    ]

    if jobs <= 1:
        for i, t in enumerate(tasks, start=1):
            _, ok, err = _export_one(t)
            exported += int(ok)
            if not ok:
                failed += 1
                log.warning("pdmx_export_failed", error=str(err))
            if i % 500 == 0:
                log.info("pdmx_export_progress", done=i, total=total)
    else:
        with ProcessPoolExecutor(max_workers=jobs) as ex:
            futs = [ex.submit(_export_one, t) for t in tasks]
            for i, fut in enumerate(as_completed(futs), start=1):
                _, ok, err = fut.result()
                exported += int(ok)
                if not ok:
                    failed += 1
                    log.warning("pdmx_export_failed", error=str(err))
                if i % 500 == 0:
                    log.info("pdmx_export_progress", done=i, total=total)

    log.info("pdmx_export_done", exported=exported, failed=failed, total=total)
    return {"exported": exported, "failed": failed, "total": total}


# --------------------------------------------------------------------------------------
# Optional: utility to rebuild a single MusicXML from an existing MusicXML/ZIP
# (helpful if later you decide to round-trip or verify)
# --------------------------------------------------------------------------------------
def rebuild_musicxml_if_needed(src: Path, dst: Path) -> None:
    """
    If src is MXL or compressed MusicXML, re-save to plain MusicXML at dst using music21.
    """
    if not src.exists():
        return
    try:
        s = converter.parse(str(src))
        _ensure_parent_dir(dst)
        s.write("musicxml", fp=str(dst))
    except Exception as e:
        log.warning("pdmx_rebuild_musicxml_failed", src=str(src), error=str(e))


# --------------------------------------------------------------------------------------
# Quick test (manual)
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    # Minimal manual test:
    # python -m omr_lab.data.pdmx_export <root> <out> [ext]
    import sys

    if len(sys.argv) < 3:
        print(
            "Usage: python -m omr_lab.data.pdmx_export <pdmx_root> <out_dir> [musicxml|mxl]"
        )
        sys.exit(1)

    _root = Path(sys.argv[1])
    _out = Path(sys.argv[2])
    _ext = sys.argv[3] if len(sys.argv) > 3 else "musicxml"

    log.info(
        "pdmx_export_start",
        root=str(_root),
        out=str(_out),
        jobs=1,
        lyrics_only=False,
        no_conflict_only=False,
        ext=_ext,
        csv=None,
    )
    summary = export_pdmx_to_musicxml(_root, _out, jobs=1, ext=_ext)
    print(summary)
