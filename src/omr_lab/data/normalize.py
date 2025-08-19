from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

from music21 import converter, meter, note, stream
from omr_lab.common.ir import LyricsToken, MeasureIR, NoteEvent, PartIR, ScoreIR


def _coerce_to_score(obj: Any) -> stream.Score:
    """Приводим результат converter.parse к Score (обрабатываем Opus/Part)."""
    if isinstance(obj, stream.Score):
        return obj
    if isinstance(obj, stream.Opus):
        # Берём первый Score из Opus (чаще всего один)
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
    # Фоллбек: пытаемся собрать Score из доступных Part
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


def musicxml_to_ir(path: Path) -> ScoreIR:
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
    try:
        k = sc.analyze("key")
        if k is not None:
            kf = int(k.sharps)
    except Exception:
        kf = None

    parts_ir: list[PartIR] = []
    for p_idx, p in enumerate(sc.parts):
        measures_ir: list[MeasureIR] = []
        flat: stream.Stream = p.flat  # type: ignore[assignment]
        # measureNumber может быть None → фильтруем и приводим к int
        measure_numbers = sorted(
            {
                int(mn)
                for mn in (getattr(n, "measureNumber", None) for n in flat.notesAndRests)
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
                    alter = _safe_int(el.pitch.accidental.alter if el.pitch.accidental else 0, 0)
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

            measures_ir.append(MeasureIR(number=int(mnum), notes=notes_ir, lyrics=lyrics_ir))

        parts_ir.append(
            PartIR(id=f"P{p_idx+1}", name=p.partName or f"Part {p_idx+1}", measures=measures_ir)
        )

    has_lyrics = any(tok for part in parts_ir for m in part.measures for tok in m.lyrics)

    return ScoreIR(
        title=title, parts=parts_ir, time_signature=ts, key_fifths=kf, has_lyrics=has_lyrics
    )


def normalize_folder(in_dir: Path, out_dir: Path) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    count = 0
    for p in in_dir.rglob("*"):
        if p.suffix.lower() in {".musicxml", ".xml", ".mxl"}:
            try:
                ir = musicxml_to_ir(p)
                out_path = out_dir / (p.stem + ".json")
                # Pydantic v2: используем json.dumps(c.dict())
                out_path.write_text(
                    json.dumps(ir.model_dump(), indent=2, ensure_ascii=False), encoding="utf-8"
                )
                count += 1
            except Exception as e:
                (out_dir / (p.stem + ".error.txt")).write_text(str(e), encoding="utf-8")
    return count
