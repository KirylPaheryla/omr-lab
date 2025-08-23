from __future__ import annotations

import subprocess
from collections.abc import Sequence
from pathlib import Path

from omr_lab.common.logging import log


def render_png_with_musescore(
    musescore_cmd: str | Path,
    input_xml: Path,
    out_png: Path,
    dpi: int = 300,
    trim_px: int | None = 0,
    extra_args: Sequence[str] | None = None,
) -> list[Path]:
    """
    Render MusicXML to PNG via MuseScore CLI.
    Returns a list of produced PNG files (supports multipage outputs).
    """
    musescore_cmd = Path(musescore_cmd).as_posix()
    out_png = out_png.resolve()
    out_png.parent.mkdir(parents=True, exist_ok=True)

    cmd: list[str] = [musescore_cmd, "-s"]
    if trim_px and trim_px > 0:
        # Optional: not all versions support -T
        cmd += ["-T", str(trim_px)]
    cmd += ["-r", str(dpi), "-o", out_png.as_posix(), input_xml.as_posix()]
    if extra_args:
        cmd += list(extra_args)

    log.info("musescore_cmd", cmd=" ".join(cmd))
    try:
        subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as err:
        raise RuntimeError(f"MuseScore CLI not found: {musescore_cmd}") from err
    except subprocess.CalledProcessError as err:
        raise RuntimeError(
            f"MuseScore render failed (code {err.returncode}).\n"
            f"STDOUT:\n{err.stdout}\n\nSTDERR:\n{err.stderr}"
        ) from err

    produced: list[Path] = []
    if out_png.exists():
        produced.append(out_png)
    else:
        pattern = f"{out_png.stem}-*.png"
        produced = sorted(out_png.parent.glob(pattern))
        if not produced:
            log.warning("musescore_no_output_found", expected=str(out_png))
            raise RuntimeError("MuseScore did not produce expected PNG(s).")
    log.info("musescore_render_ok", files=[p.name for p in produced])
    return produced
