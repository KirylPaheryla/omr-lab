from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import mido

from omr_lab.common.config import AppConfig
from omr_lab.common.logging import log

SUPPORTED_EXT = {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp"}


def _write_minimal_musicxml(out_xml: Path, title: str) -> None:
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<score-partwise version="3.1">
  <work><work-title>{title}</work-title></work>
  <part-list>
    <score-part id="P1"><part-name>Part 1</part-name></score-part>
  </part-list>
  <part id="P1">
    <measure number="1">
      <attributes>
        <divisions>1</divisions>
        <key><fifths>0</fifths></key>
        <time><beats>4</beats><beat-type>4</beat-type></time>
        <clef><sign>G</sign><line>2</line></clef>
      </attributes>
      <note><rest/><duration>4</duration><type>whole</type></note>
    </measure>
  </part>
</score-partwise>
"""
    out_xml.write_text(xml, encoding="utf-8")


def _write_minimal_midi(out_mid: Path) -> None:
    mid = mido.MidiFile()
    track = mido.MidiTrack()
    mid.tracks.append(track)
    track.append(mido.MetaMessage("set_tempo", tempo=mido.bpm2tempo(120), time=0))
    track.append(mido.Message("note_on", note=60, velocity=64, time=0))
    track.append(mido.Message("note_off", note=60, velocity=64, time=480))
    track.append(mido.MetaMessage("end_of_track", time=0))
    mid.save(out_mid.as_posix())


def _iter_images(paths: Iterable[Path]) -> list[Path]:
    result: list[Path] = []
    for p in paths:
        if p.is_dir():
            for q in p.rglob("*"):
                if q.suffix.lower() in SUPPORTED_EXT:
                    result.append(q)
        elif p.suffix.lower() in SUPPORTED_EXT:
            result.append(p)
    return sorted(result)


def run_rules_pipeline(inputs: Iterable[Path], out_dir: Path, config: AppConfig | None) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    images = _iter_images(inputs)
    log.info("rules_pipeline_input", count=len(images))
    for img in images:
        stem = img.stem
        xml_path = out_dir / f"{stem}.musicxml"
        mid_path = out_dir / f"{stem}.mid"
        _write_minimal_musicxml(xml_path, title=stem)
        _write_minimal_midi(mid_path)
    (out_dir / "metrics.csv").write_text(
        "metric,value\nfiles_processed," + str(len(images)) + "\n",
        encoding="utf-8",
    )
    log.info("rules_pipeline_done", outputs=str(out_dir))
