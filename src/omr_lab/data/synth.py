from __future__ import annotations

import random
from collections.abc import Iterable
from pathlib import Path

from music21 import duration, meter, note, stream, tempo
from music21.note import Lyric
from omr_lab.common.ir import ScoreIR
from omr_lab.data.normalize import musicxml_to_ir


def _split_to_syllables(word: str) -> list[tuple[str, str]]:
    if "-" in word:
        parts = [w for w in word.split("-") if w]
        if len(parts) == 1:
            return [(parts[0], "single")]
        res: list[tuple[str, str]] = []
        for i, w in enumerate(parts):
            if i == 0:
                res.append((w, "begin"))
            elif i == len(parts) - 1:
                res.append((w, "end"))
            else:
                res.append((w, "middle"))
        return res
    else:
        return [(word, "single")]


def _attach_lyrics_to_notes(nlist: list[note.Note], words: list[str]) -> None:
    syls: list[tuple[str, str]] = []
    for w in words:
        for s, syl in _split_to_syllables(w):
            syls.append((s, syl))
    for i, n in enumerate(nlist):
        if i < len(syls):
            s, syl = syls[i]
            lyr = Lyric(text=s)
            # allowed values: "single", "begin", "middle", "end"
            lyr.syllabic = syl  # type: ignore[assignment]
            n.lyrics.append(lyr)


def synth_one(out_musicxml: Path, words: list[str] | None = None, measures: int = 4) -> ScoreIR:
    sc = stream.Score()
    sc.insert(0, tempo.MetronomeMark(number=100))
    p = stream.Part()
    p.append(meter.TimeSignature("4/4"))

    notes: list[note.Note] = []
    for _m in range(measures):
        for _q in range(4):
            n = note.Note(random.choice(["C4", "D4", "E4", "F4", "G4", "A4", "B4"]))
            n.duration = duration.Duration(1.0)  # quarter note
            notes.append(n)
            p.append(n)

    if words:
        _attach_lyrics_to_notes(notes, words)

    sc.append(p)
    sc.write("musicxml", fp=out_musicxml.as_posix())

    return musicxml_to_ir(out_musicxml)


def synth_batch(out_dir: Path, n: int = 10, words_bank: Iterable[list[str]] | None = None) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    if words_bank is None:
        words_bank = [
            ["la", "la", "la", "la"],
            ["glo-ri-a", "in", "ex-cel-sis", "De-o"],
            ["do", "re", "mi", "fa", "sol", "la", "si"],
        ]
    count = 0
    for i in range(n):
        ws = random.choice(list(words_bank))
        mx = out_dir / f"synth_{i:03d}.musicxml"
        synth_one(mx, words=ws, measures=max(1, len(ws) // 4 + 1))
        count += 1
    return count
