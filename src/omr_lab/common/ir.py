from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

Syllabic = Literal["single", "begin", "middle", "end"]


class LyricsToken(BaseModel):
    text: str
    syllabic: Syllabic
    note_id: str
    word_index: int | None = None
    syll_index: int | None = None


class NoteEvent(BaseModel):
    id: str
    pitch_step: str
    pitch_octave: int
    pitch_alter: int
    duration_quarter: float
    start_quarter: float
    voice: int
    staff: int
    tie_start: bool = False
    tie_stop: bool = False


class MeasureIR(BaseModel):
    number: int
    notes: list[NoteEvent]
    lyrics: list[LyricsToken]


class PartIR(BaseModel):
    id: str
    name: str
    measures: list[MeasureIR]


class ScoreIR(BaseModel):
    title: str
    parts: list[PartIR]
    time_signature: str | None = None
    key_fifths: int | None = None
    has_lyrics: bool = False
