from __future__ import annotations

import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

from omr_lab.common.logging import log


def render_svg_with_verovio(
    verovio_cmd: str | Path,
    input_xml: Path,
    out_svg: Path,
    all_pages: bool = False,
    scale: int = 40,
    extra_args: list[str] | None = None,
) -> list[Path]:
    """
    Render MusicXML → SVG via Verovio CLI.
    """
    verovio_cmd = Path(verovio_cmd).as_posix()
    out_svg = out_svg.resolve()
    out_svg.parent.mkdir(parents=True, exist_ok=True)

    cmd: list[str] = [verovio_cmd]
    if all_pages:
        cmd += ["-a"]
    cmd += [
        "-f",
        "musicxml",
        "--scale",
        str(scale),
        input_xml.as_posix(),
        "-o",
        out_svg.as_posix(),
    ]
    if extra_args:
        cmd += extra_args

    log.info("verovio_cmd", cmd=" ".join(cmd))
    try:
        subprocess.run(cmd, check=True)
    except FileNotFoundError as err:
        raise RuntimeError(f"Verovio CLI not found: {verovio_cmd}") from err
    except subprocess.CalledProcessError as err:
        raise RuntimeError(f"Verovio render failed: {err}") from err

    produced: list[Path] = []
    if out_svg.exists():
        produced.append(out_svg)
    else:
        produced = sorted(out_svg.parent.glob(f"{out_svg.stem}-*.svg"))
        if not produced:
            log.warning("verovio_no_output_found", expected=str(out_svg))
            raise RuntimeError("Verovio did not produce expected SVG(s).")
    log.info("verovio_render_ok", files=[p.name for p in produced])
    return produced


def extract_lyrics_bboxes_from_svg(svg_path: Path) -> list[dict]:
    """
    Naive SVG parser to extract lyric candidates. Looks for:
      - <rect class="...lyric..."> with x/y/width/height
      - <text class="...lyric..."> with text content (no width/height)
    """
    try:
        tree = ET.parse(svg_path)
        root = tree.getroot()
    except Exception as err:
        log.warning("svg_parse_failed", file=str(svg_path), error=str(err))
        return []

    ns = {"svg": "http://www.w3.org/2000/svg"}
    out: list[dict] = []

    # Rects with lyric class
    for rect in root.findall(".//svg:rect", ns):
        cls = rect.attrib.get("class", "")
        if "lyric" in cls.lower():
            x = float(rect.attrib.get("x", "0") or 0)
            y = float(rect.attrib.get("y", "0") or 0)
            w = float(rect.attrib.get("width", "0") or 0)
            h = float(rect.attrib.get("height", "0") or 0)
            xml_id = rect.attrib.get("{http://www.w3.org/XML/1998/namespace}id")
            out.append({"x": x, "y": y, "w": w, "h": h, "text": None, "xml_id": xml_id})

    # Text nodes with lyric class
    for t in root.findall(".//svg:text", ns):
        cls = t.attrib.get("class", "")
        if "lyric" in cls.lower():
            x = float(t.attrib.get("x", "0") or 0)
            y = float(t.attrib.get("y", "0") or 0)
            txt = "".join(t.itertext()).strip() or None
            xml_id = t.attrib.get("{http://www.w3.org/XML/1998/namespace}id")
            out.append(
                {"x": x, "y": y, "w": 0.0, "h": 0.0, "text": txt, "xml_id": xml_id}
            )

    return out
