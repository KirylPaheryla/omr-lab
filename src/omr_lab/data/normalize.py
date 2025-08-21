from __future__ import annotations

import json
import zipfile
from collections.abc import Iterable
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, cast

from music21 import (
    converter,
    exceptions21,
    meter,
    note,
    stream,
)  # ← добавили exceptions21
from omr_lab.common.ir import LyricsToken, MeasureIR, NoteEvent, PartIR, ScoreIR
from omr_lab.common.logging import log


def _silence_music21_warnings() -> None:
    """Suppress noisy music21 warnings to speed up and declutter output."""
    import warnings

    # Filter only categories that actually exist in this music21 build
    for name in ("MusicXMLWarning", "Music21DeprecationWarning"):
        cat = getattr(exceptions21, name, None)
        if isinstance(cat, type) and issubclass(cat, Warning):
            warnings.filterwarnings("ignore", category=cat)


def _coerce_to_score(obj: Any) -> stream.Score:
    """Coerce converter.parse result to a Score (handle Opus/Part)."""
    if isinstance(obj, stream.Score):
        return obj
    if isinstance(obj, stream.Opus):
        if obj.scores:
            return obj.scores[0]
        sc = stream.Score()
        for el in obj.flatten():
            if isinstance(el, stream.Part):
                sc.insert(0, el)
        return sc
    if isinstance(obj, stream.Part):
        sc = stream.Score()
        sc.insert(0, obj)
        return sc
    sc = stream.Score()
    parts = getattr(obj, "parts", None)
    if parts:
        for p in parts:
            if isinstance(p, stream.Part):
                sc.insert(0, p)
    return sc


def _safe_int(v: Any, default: int) -> int:
    try:
        return int(v)  # type: ignore[arg-type]
    except Exception:
        return default


def _coerce_alter(val: Any) -> tuple[int, bool]:
    """
    Return (alter_int, was_microtonal).
    Accepts float-like values (e.g., -0.3). If it's within ~1/3 of a semitone,
    round to nearest int; otherwise clamp to 0 and mark as microtonal.
    """
    try:
        f = float(val)
    except Exception:
        return 0, False
    if f < -2.5 or f > 2.5:
        return 0, True
    rounded = int(round(f))
    if abs(f - rounded) <= 0.34:
        return max(-2, min(2, rounded)), abs(f - rounded) > 1e-6
    # too fractional → drop to 0 but mark it
    return 0, True


def musicxml_to_ir(path: Path, *, analyze_key: bool = True) -> ScoreIR:
    parsed = converter.parse(path.as_posix())
    sc: stream.Score = _coerce_to_score(parsed)

    title = sc.metadata.title if sc.metadata and sc.metadata.title else path.stem
    ts = None
    kf = None
    try:
        m = sc.recurse().getElementsByClass(meter.TimeSignature).first()
        if m is not None:
            ts = m.ratioString
    except Exception:
        ts = None
    if analyze_key:
        try:
            k = sc.analyze("key")
            if k is not None:
                kf = int(k.sharps)
        except Exception:
            kf = None

    parts_ir: list[PartIR] = []
    for p_idx, p in enumerate(sc.parts):
        measures_ir: list[MeasureIR] = []
        flat: stream.Stream = p.flatten()  # .flat is deprecated

        measure_numbers = sorted(
            {
                int(mn)
                for mn in (
                    getattr(n, "measureNumber", None) for n in flat.notesAndRests
                )
                if mn is not None
            }
        )

        local_note_idx = 0
        for mnum in measure_numbers:
            notes_ir: list[NoteEvent] = []
            lyrics_ir: list[LyricsToken] = []

            ms = [
                n
                for n in flat.notesAndRests
                if _safe_int(getattr(n, "measureNumber", None), -1) == mnum
            ]

            for el in ms:
                if isinstance(el, note.Note):
                    nid = f"p{p_idx}_n{local_note_idx}"
                    local_note_idx += 1

                    step = el.pitch.step
                    octv = _safe_int(el.pitch.octave, 4)
                    alter_raw = el.pitch.accidental.alter if el.pitch.accidental else 0
                    alter, was_micro = _coerce_alter(alter_raw)
                    if was_micro:
                        from omr_lab.common.logging import log

                        log.debug(
                            "microtonal_alter_coerced",
                            raw=float(alter_raw or 0),
                            used=alter,
                        )

                    dur_q = float(el.duration.quarterLength)
                    off_q = float(el.offset)
                    voice_val = getattr(el, "voice", None)
                    staff_val = getattr(el, "staffNumber", None)
                    voice = _safe_int(getattr(voice_val, "id", voice_val), 1)
                    staff = _safe_int(staff_val, 1)
                    tie_start = bool(el.tie and el.tie.type in ("start", "continue"))
                    tie_stop = bool(el.tie and el.tie.type in ("stop", "continue"))

                    notes_ir.append(
                        NoteEvent(
                            id=nid,
                            pitch_step=cast(str, step),
                            pitch_octave=octv,
                            pitch_alter=alter,
                            duration_quarter=dur_q,
                            start_quarter=off_q,
                            voice=voice,
                            staff=staff,
                            tie_start=tie_start,
                            tie_stop=tie_stop,
                        )
                    )

                    if el.lyrics:
                        for li, lyr in enumerate(el.lyrics):
                            text = (lyr.text or "").strip()
                            if not text:
                                continue
                            syl = getattr(lyr, "syllabic", None)
                            syl_str = syl.lower() if isinstance(syl, str) else "single"
                            if syl_str not in {"single", "begin", "middle", "end"}:
                                syl_str = "single"
                            lyrics_ir.append(
                                LyricsToken(
                                    text=text,
                                    syllabic=syl_str,  # type: ignore[arg-type]
                                    note_id=nid,
                                    word_index=None,
                                    syll_index=li,
                                )
                            )

            measures_ir.append(
                MeasureIR(number=int(mnum), notes=notes_ir, lyrics=lyrics_ir)
            )

        parts_ir.append(
            PartIR(
                id=f"P{p_idx+1}",
                name=p.partName or f"Part {p_idx+1}",
                measures=measures_ir,
            )
        )

    has_lyrics = any(
        tok for part in parts_ir for m in part.measures for tok in m.lyrics
    )

    return ScoreIR(
        title=title,
        parts=parts_ir,
        time_signature=ts,
        key_fifths=kf,
        has_lyrics=has_lyrics,
    )


def _quick_has_lyrics(path: Path) -> bool:
    """Fast check: does file contain '<lyric' (also inside .mxl zip)."""
    try:
        ext = path.suffix.lower()
        if ext in {".xml", ".musicxml"}:
            # Read small chunks to avoid loading huge files
            text = path.read_text(encoding="utf-8", errors="ignore")
            return "<lyric" in text
        if ext == ".mxl":
            with zipfile.ZipFile(path, "r") as zf:
                for zi in zf.infolist():
                    if zi.filename.lower().endswith(".xml"):
                        with zf.open(zi, "r") as fh:
                            data = fh.read()
                            try:
                                txt = data.decode("utf-8", errors="ignore")
                            except Exception:
                                txt = ""
                            if "<lyric" in txt:
                                return True
            return False
    except Exception:
        return False
    return False


def _should_skip(in_path: Path, out_path: Path, skip_if_exists: bool) -> bool:
    if not out_path.exists():
        return False
    if not skip_if_exists:
        return False
    # incremental: skip if output is newer or same mtime
    try:
        return out_path.stat().st_mtime >= in_path.stat().st_mtime
    except Exception:
        return True


def _process_one(
    args: tuple[Path, Path, bool, bool, bool]
) -> tuple[Path, bool, str | None]:
    """
    Worker for parallel normalization.
    Returns (in_path, ok, error_msg).
    """
    in_path, out_dir, analyze_key, overwrite, quiet = args
    try:
        if quiet:
            _silence_music21_warnings()
        out_path = out_dir / (in_path.stem + ".json")
        if not overwrite and out_path.exists():
            return in_path, True, None
        ir = musicxml_to_ir(in_path, analyze_key=analyze_key)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(ir.model_dump(), indent=2, ensure_ascii=False), encoding="utf-8"
        )
        return in_path, True, None
    except Exception as e:
        err_path = out_dir / (in_path.stem + ".error.txt")
        err_path.write_text(str(e), encoding="utf-8")
        return in_path, False, str(e)


def _gather_files(in_dir: Path) -> list[Path]:
    return [
        p
        for p in in_dir.rglob("*")
        if p.suffix.lower() in {".musicxml", ".xml", ".mxl"}
    ]


def normalize_folder(
    in_dir: Path,
    out_dir: Path,
    *,
    jobs: int = 1,
    skip_if_exists: bool = True,
    lyrics_only: bool = False,
    analyze_key: bool = True,
    quiet_warnings: bool = False,  # ← новый флаг
) -> int:
    """
    Normalize MusicXML/MXL into ScoreIR.

    Args:
        jobs: parallel workers (>=1)
        skip_if_exists: skip files with up-to-date JSON
        lyrics_only: pre-filter by presence of '<lyric' (fast scan)
        analyze_key: run music21 key analysis (slower)
        quiet_warnings: suppress music21 warnings
    """
    if quiet_warnings and jobs <= 1:
        # В однопоточном режиме можно заглушить глобально
        _silence_music21_warnings()

    out_dir.mkdir(parents=True, exist_ok=True)
    files = _gather_files(in_dir)
    if lyrics_only:
        files = [p for p in files if _quick_has_lyrics(p)]

    candidates: list[Path] = []
    for p in files:
        out_path = out_dir / (p.stem + ".json")
        if _should_skip(p, out_path, skip_if_exists):
            continue
        candidates.append(p)

    if not candidates:
        log.info("normalize_no_candidates", in_dir=str(in_dir))
        return 0

    # single-thread
    if jobs <= 1:
        ok = 0
        for p in candidates:
            _, success, _ = _process_one(
                (p, out_dir, analyze_key, True, quiet_warnings)
            )
            ok += int(success)
        return ok

    # parallel
    ok = 0
    args_iter: Iterable[tuple[Path, Path, bool, bool, bool]] = (
        (p, out_dir, analyze_key, True, quiet_warnings) for p in candidates
    )
    with ProcessPoolExecutor(max_workers=jobs) as ex:
        futs = [ex.submit(_process_one, a) for a in args_iter]
        for i, fut in enumerate(as_completed(futs), start=1):
            _p, success, err = fut.result()
            ok += int(success)
            if not success:
                log.warning("normalize_failed", file=str(_p), error=err)
            if i % 50 == 0:
                log.info("normalize_progress", done=i, total=len(futs))
    return ok
