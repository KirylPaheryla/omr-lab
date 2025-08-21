from __future__ import annotations

import csv
import json
import math
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from omr_lab.common.logging import log


# ---------- файловые утилиты ----------


def _iter_musicrender_json(root: Path) -> list[Path]:
    # PDMX раскладывает JSON по data/<a..z>/<bucket>/*.json
    return [p for p in (root / "data").rglob("*.json") if p.is_file()]


def _looks_like_has_lyrics_fast(p: Path) -> bool:
    # В PDMX явных лирик почти нет. Если фильтр всё же включён — быстрый эвристический поиск.
    try:
        t = p.read_text(encoding="utf-8", errors="ignore")
        return '"lyric"' in t or '"lyrics"' in t
    except Exception:
        return False


def _passes_conflict_filter(p: Path, no_conflict_only: bool) -> bool:
    # Заглушка: если нет явной метки “conflict” в JSON — считаем, что пропускаем.
    # При желании подключите PDMX.csv и читайте колонку «conflict».
    if not no_conflict_only:
        return True
    try:
        t = p.read_text(encoding="utf-8", errors="ignore")
        # если встречается "conflict": true — исключаем
        return '"conflict"' not in t or '"conflict": false' in t.lower()
    except Exception:
        return False


def _gather_pdmx_json(root: Path) -> list[Path]:
    data_dir = root / "data"
    if data_dir.exists():
        return [p for p in data_dir.rglob("*.json")]
    return [p for p in root.rglob("*.json")]


def _fast_has_lyrics(path: Path) -> bool:
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
        return ('"lyrics"' in text) or ('"lyric"' in text) or ('"text"' in text)
    except Exception:
        return False


def _load_no_conflict_filter(csv_path: Path) -> set[str] | None:
    if not csv_path.exists():
        return None
    allow: set[str] = set()
    try:
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            conflict_cols = ["no_conflict", "license_ok", "has_conflict"]
            id_cols = ["ipfs_hash", "hash", "id", "uid", "stem", "best_path"]
            for row in reader:
                ok: bool | None = None
                for c in conflict_cols:
                    if c in row:
                        v = str(row[c]).strip().lower()
                        if c == "has_conflict":
                            ok = v in ("0", "false", "no", "")
                        else:
                            ok = v in ("1", "true", "yes")
                        break
                if not ok:
                    continue
                stem: str | None = None
                for ic in id_cols:
                    if ic in row and row[ic]:
                        stem = Path(str(row[ic]).strip()).stem
                        break
                if stem:
                    allow.add(stem)
        return allow
    except Exception:
        return None


# ---------- парсинг PDMX JSON (минимально необходимое) ----------


def _as_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)  # type: ignore[arg-type]
    except Exception:
        return default


def _as_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)  # type: ignore[arg-type]
    except Exception:
        return default


def _detect_resolution(obj: dict[str, Any]) -> int:
    """
    Пробуем понять ticks-per-quarter (TPQ).
    Часто встречается 'resolution'/'tpq'/'ticks_per_quarter' либо считаем по НОД шагов времени.
    """
    for k in ("resolution", "tpq", "ticks_per_quarter", "ticksPerQuarter"):
        if k in obj:
            return max(1, _as_int(obj[k], 480))
    # попробуем собрать все time/duration и оценить шаг
    times: list[int] = []
    durs: list[int] = []

    def collect(note_like: dict[str, Any]) -> None:
        t = note_like.get("time", note_like.get("start", note_like.get("onset")))
        d = note_like.get("duration", None)
        if t is not None:
            times.append(_as_int(t))
        if d is not None:
            durs.append(_as_int(d))

    # ищем грубо возможные контейнеры нот
    for k in ("notes", "events"):
        if isinstance(obj.get(k), list):
            for n in obj[k]:
                if isinstance(n, dict):
                    collect(n)
    if isinstance(obj.get("tracks"), list):
        for tr in obj["tracks"]:
            if not isinstance(tr, dict):
                continue
            for k in ("notes", "events"):
                if isinstance(tr.get(k), list):
                    for n in tr[k]:
                        if isinstance(n, dict):
                            collect(n)
    vals = [v for v in times + durs if v > 0]
    if not vals:
        return 480
    # оценка: ближайшая степень 2 * 60…960
    median = sorted(vals)[len(vals) // 2]
    candidates = [240, 256, 360, 384, 480, 512, 720, 768, 960]
    best = min(candidates, key=lambda c: abs(median - c))
    return best


def _extract_tracks(obj: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Возвращает список треков в унифицированном виде:
    [{"name": str|None, "notes":[{pitch:int|str, time:int, duration:int}], "lyrics":[{time:int, text:str}], ...}]
    """
    if isinstance(obj.get("tracks"), list):
        return [t for t in obj["tracks"] if isinstance(t, dict)]

    # некоторые json могут быть "плоскими"
    track: dict[str, Any] = {
        "name": obj.get("name"),
        "notes": obj.get("notes") or obj.get("events") or [],
        "lyrics": obj.get("lyrics") or [],
    }
    return [track]


def _note_to_midi(n: dict[str, Any]) -> int | None:
    """
    Поддержим варианты:
      n["pitch"] -> int MIDI или {"midi":60} или "C4" (тогда попробуем вручную)
    """
    p = n.get("pitch")
    if p is None:
        p = n.get("midi")
    if isinstance(p, (int, float)):
        v = int(round(float(p)))
        return max(0, min(127, v))
    if isinstance(p, dict):
        for k in ("midi", "pitch", "note", "value"):
            if k in p and isinstance(p[k], (int, float)):
                v = int(round(float(p[k])))
                return max(0, min(127, v))
    if isinstance(p, str):
        # очень грубо: C4..B4
        names = {
            "C": 0,
            "C#": 1,
            "Db": 1,
            "D": 2,
            "D#": 3,
            "Eb": 3,
            "E": 4,
            "F": 5,
            "F#": 6,
            "Gb": 6,
            "G": 7,
            "G#": 8,
            "Ab": 8,
            "A": 9,
            "A#": 10,
            "Bb": 10,
            "B": 11,
        }
        s = p.strip()
        base = None
        for name, val in names.items():
            if s.upper().startswith(name):
                base = val
                rest = s[len(name) :]
                try:
                    octv = int(rest)
                except Exception:
                    octv = 4
                if base is not None:
                    return 12 * (octv + 1) + base
        return None
    return None


def _extract_notes_and_lyrics(
    track: dict[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    notes: list[dict[str, Any]] = []
    lyrics: list[dict[str, Any]] = []

    raw_notes = track.get("notes") or track.get("events") or []
    if isinstance(raw_notes, list):
        for n in raw_notes:
            if not isinstance(n, dict):
                continue
            midi = _note_to_midi(n)
            if midi is None:
                continue
            t = n.get("time", n.get("start", n.get("onset", 0)))
            d = n.get("duration", n.get("dur", n.get("length", 0)))
            notes.append(
                {"midi": _as_int(midi), "time": _as_int(t), "duration": _as_int(d)}
            )

    raw_lyrics = track.get("lyrics") or []
    if isinstance(raw_lyrics, list):
        for l in raw_lyrics:
            if not isinstance(l, dict):
                continue
            txt = l.get("lyric", l.get("text", ""))
            if not txt:
                continue
            t = l.get("time", l.get("start", l.get("onset", 0)))
            lyrics.append({"time": _as_int(t), "text": str(txt)})

    return notes, lyrics


# ---------- построение MusicXML через music21 ----------


def _build_m21_score(
    json_obj: dict[str, Any], title: str, *, out_ext: str
) -> "stream.Score":
    from music21 import metadata, meter, note as m21note, stream

    sc = stream.Score()
    sc.metadata = metadata.Metadata()
    sc.metadata.title = title

    tpq = _detect_resolution(json_obj)
    # по умолчанию 4/4
    ts = meter.TimeSignature("4/4")
    sc.insert(0, ts)

    tracks = _extract_tracks(json_obj)
    for tr_idx, tr in enumerate(tracks):
        part = stream.Part()
        name = tr.get("name") or f"Track {tr_idx+1}"
        part.partName = str(name)

        notes, lyrics = _extract_notes_and_lyrics(tr)
        # делаем индексацию нот по времени для привязки лирики
        by_time: dict[int, list["m21note.Note"]] = {}

        for ev in notes:
            midi = ev["midi"]
            t = ev["time"]
            d = ev["duration"]
            # перевод в quarterLength: quarter = tpq
            start_q = float(t) / float(tpq)
            dur_q = max(0.125, float(d) / float(tpq)) if d > 0 else 0.25

            n = m21note.Note()
            n.pitch.midi = int(midi)
            n.duration.quarterLength = dur_q
            part.insert(start_q, n)
            by_time.setdefault(int(t), []).append(n)

        # привязываем лирику к ближайшей ноте во временном окне ±tpq/8
        tol = max(1, tpq // 8)
        for l in lyrics:
            t = int(l["time"])
            txt = l["text"]
            candidates: list[tuple[int, "m21note.Note"]] = []
            for dt in range(-tol, tol + 1, max(1, tol // 4)):
                bucket = by_time.get(t + dt)
                if not bucket:
                    continue
                for n in bucket:
                    # расстояние в тактах
                    candidates.append((abs(dt), n))
            if candidates:
                candidates.sort(key=lambda x: x[0])
                candidates[0][1].addLyric(txt)  # type: ignore[arg-type]

        sc.insert(0, part)

    return sc


def _write_score(sc: "stream.Score", out_path: Path, *, ext: str) -> None:
    # ext: 'musicxml' | 'mxl'
    if ext.lower() == "mxl":
        # сначала в .musicxml, затем упакуем
        tmp_xml = out_path.with_suffix(".musicxml")
        sc.write("musicxml", fp=str(tmp_xml))
        with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(tmp_xml, arcname=tmp_xml.name)
        tmp_xml.unlink(missing_ok=True)
    else:
        sc.write("musicxml", fp=str(out_path))


# ---------- рабочие функции ----------


def _export_one(
    json_path: Path, out_dir: Path, ext: str
) -> tuple[Path, bool, str | None]:
    """
    Простейший экспорт: кладём исходный MusicRender JSON рядом для трассировки
    и выгружаем MusicXML через muspy/pretty_midi недоступно — поэтому здесь
    только заглушка-копия JSON + .musicxml-placeholder.
    Реальный экспорт подключите к внешнему конвертеру, когда будет готов.
    """
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        stem = json_path.stem
        # placeholder: исходный JSON копируем
        (out_dir / f"{stem}.json").write_text(
            json_path.read_text(encoding="utf-8", errors="ignore"),
            encoding="utf-8",
        )
        # placeholder: создаём пустой XML как маркер
        (out_dir / f"{stem}.{ext}").write_text(
            "<!-- TODO: write real MusicXML here -->\n", encoding="utf-8"
        )
        return json_path, True, None
    except Exception as e:
        return json_path, False, str(e)


def export_pdmx_to_musicxml(
    pdmx_root: Path,
    out_dir: Path,
    *,
    csv_path: (
        Path | None
    ) = None,  # сейчас не используется; можно подключить при необходимости
    jobs: int = 1,
    lyrics_only: bool = False,
    no_conflict_only: bool = False,
    ext: str = "musicxml",
) -> dict[str, int]:
    """
    Возвращает summary: {"exported": X, "failed": Y, "total": N}.
    """
    files = _iter_musicrender_json(pdmx_root)
    # фильтры
    if lyrics_only:
        files = [p for p in files if _looks_like_has_lyrics_fast(p)]
    if no_conflict_only:
        files = [p for p in files if _passes_conflict_filter(p, True)]

    total = len(files)
    if total == 0:
        log.warning("pdmx_export_no_candidates")
        return {"exported": 0, "failed": 0, "total": 0}

    exported = 0
    failed = 0

    if jobs <= 1:
        for p in files:
            _, ok, err = _export_one(p, out_dir, ext)
            if ok:
                exported += 1
            else:
                failed += 1
                log.warning("pdmx_export_failed", error=f"{type(err).__name__}: {err}")
    else:
        tasks: Iterable[tuple[Path, Path, str]] = ((p, out_dir, ext) for p in files)
        with ProcessPoolExecutor(max_workers=jobs) as ex:
            futs = [ex.submit(_export_one, *t) for t in tasks]
            for i, fut in enumerate(as_completed(futs), start=1):
                _, ok, err = fut.result()
                if ok:
                    exported += 1
                else:
                    failed += 1
                    log.warning(
                        "pdmx_export_failed", error=f"{type(err).__name__}: {err}"
                    )
                if i % 500 == 0:
                    log.info("pdmx_export_progress", done=i, total=total)

    return {"exported": exported, "failed": failed, "total": total}
