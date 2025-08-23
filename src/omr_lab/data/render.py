from __future__ import annotations

import csv
import os
import time
from pathlib import Path
from typing import Any

import cv2
from omr_lab.common.logging import log
from omr_lab.data.coco import CocoAnnotation, CocoImage, default_categories, write_coco
from omr_lab.data.normalize import musicxml_to_ir
from omr_lab.render.musescore import render_png_with_musescore
from omr_lab.render.verovio import (
    extract_lyrics_bboxes_from_svg,
    render_svg_with_verovio,
)


def _infer_pages_pngs(out_dir: Path, one_png: Path) -> list[Path]:
    if one_png.exists():
        return [one_png]
    cand = sorted(out_dir.glob(f"{one_png.stem}-*.png"))
    if cand:
        return cand
    return []


def _safe_rel(p: Path, base: Path) -> Path:
    p = Path(p)
    base = Path(base)
    try:
        return p.relative_to(base)
    except Exception:
        try:
            return Path(os.path.relpath(p, base))
        except Exception:
            return Path(p.name)


def render_dataset(
    input_dir: Path,
    out_images: Path,
    out_ann_dir: Path,
    musescore_cmd: str | None = None,
    verovio_cmd: str | None = None,
    dpi: int = 300,
) -> tuple[Path, Path]:
    """
    For each *.musicxml|*.xml|*.mxl:
      1) Render PNG via MuseScore (if musescore_cmd is provided).
      2) Render SVG via Verovio and extract lyric bbox candidates (if verovio_cmd is provided).
      3) Write COCO (syllable) and two manifests:
         - pages.csv (page_id, work_id, image_path, width, height, has_lyrics, n_syllables)
         - links.csv (syllable_ann_id, note_id)
    """
    input_dir = Path(input_dir).resolve()
    out_images = Path(out_images).resolve()
    out_ann_dir = Path(out_ann_dir).resolve()
    out_images.mkdir(parents=True, exist_ok=True)
    out_ann_dir.mkdir(parents=True, exist_ok=True)
    pages_csv = out_ann_dir / "pages.csv"
    links_csv = out_ann_dir / "links.csv"
    coco_path = out_ann_dir / "coco_lyrics.json"

    with (
        pages_csv.open("w", newline="", encoding="utf-8") as fp_pages,
        links_csv.open("w", newline="", encoding="utf-8") as fp_links,
    ):
        wp = csv.writer(fp_pages)
        wl = csv.writer(fp_links)
        wp.writerow(
            [
                "page_id",
                "work_id",
                "image_path",
                "width",
                "height",
                "has_lyrics",
                "n_syllables",
            ]
        )
        wl.writerow(["annotation_id", "note_id"])

        coco_images: list[CocoImage] = []
        coco_ann: list[CocoAnnotation] = []
        ann_id = 1
        img_id = 1

        for xml in sorted(
            [
                p
                for p in input_dir.rglob("*")
                if p.suffix.lower() in {".musicxml", ".xml", ".mxl"}
            ]
        ):
            stem = xml.stem
            work_id = stem
            t0 = time.time()
            log.info("render_file_start", file=str(xml))

            # 1) IR (to link syllable -> note_id)
            ir = musicxml_to_ir(xml)
            tokens = [
                (m.number, tok)
                for part in ir.parts
                for m in part.measures
                for tok in m.lyrics
            ]

            # 2) PNG via MuseScore
            produced_pngs: list[Path] = []
            if musescore_cmd:
                out_png = out_images / f"{stem}.png"
                produced_pngs = render_png_with_musescore(
                    musescore_cmd, xml, out_png, dpi=dpi, trim_px=0
                )

            # 3) SVG via Verovio (+ lyric bbox candidates)
            svg_bboxes: list[dict[str, Any]] = []
            if verovio_cmd:
                out_svg = out_ann_dir / f"{stem}.svg"
                svgs = render_svg_with_verovio(
                    verovio_cmd, xml, out_svg, all_pages=False, scale=40
                )
                for svg in svgs:
                    svg_bboxes += extract_lyrics_bboxes_from_svg(svg)

            if not produced_pngs:
                log.warning("no_png", file=str(xml))
                continue

            # 4) Naive matching of syllables to bbox by text (order-preserving)
            texts = [tok.text for _, tok in tokens]
            remaining: list[dict[str, Any]] = svg_bboxes.copy()
            matched_idx: list[int | None] = []
            for t in texts:
                # find first bbox with the same text
                found_i: int | None = None
                for j, b in enumerate(remaining):
                    bx_txt = (b.get("text") or "").strip()
                    if bx_txt == t:
                        found_i = j
                        break
                matched_idx.append(found_i)
                if found_i is not None:
                    remaining[found_i] = {"used": True}

            # 5) COCO + manifests
            for page_no, png in enumerate(produced_pngs, start=1):
                img = cv2.imread(png.as_posix())
                if img is None:
                    log.warning("read_png_failed", file=str(png))
                    continue
                h, w = img.shape[:2]
                coco_images.append(
                    CocoImage(
                        id=img_id,
                        file_name=str(_safe_rel(png, out_images)),
                        width=w,
                        height=h,
                    )
                )

                n_syllables_on_page = 0
                for k, (_, tok) in enumerate(tokens):
                    bbox_vals: list[float] = [0.0, 0.0, 0.0, 0.0]
                    xml_id: str | None = None

                    if k < len(matched_idx):
                        mi = matched_idx[k]
                        if mi is not None and 0 <= mi < len(svg_bboxes):
                            b = svg_bboxes[mi]
                            bbox_vals = [
                                float(b.get("x", 0)),
                                float(b.get("y", 0)),
                                float(b.get("w", 0)),
                                float(b.get("h", 0)),
                            ]
                            xml_id_val = b.get("xml_id")
                            xml_id = xml_id_val if isinstance(xml_id_val, str) else None

                    coco_ann.append(
                        CocoAnnotation(
                            id=ann_id,
                            image_id=img_id,
                            category_id=1,  # syllable
                            bbox=bbox_vals,
                            text=tok.text,
                            syllabic=tok.syllabic,  # type: ignore[arg-type]
                            note_id=tok.note_id,
                            xml_id=xml_id,
                        )
                    )
                    wl.writerow([ann_id, tok.note_id])
                    ann_id += 1
                    n_syllables_on_page += 1

                wp.writerow(
                    [
                        f"{stem}_p{page_no:03d}",
                        work_id,
                        str(_safe_rel(png, out_images)),
                        w,
                        h,
                        int(ir.has_lyrics),
                        n_syllables_on_page,
                    ]
                )
                img_id += 1

            dt = time.time() - t0
            log.info(
                "render_file_done",
                file=str(xml),
                pages=len(produced_pngs),
                secs=f"{dt:.2f}",
            )

        write_coco(coco_path, coco_images, coco_ann, default_categories())

    return coco_path, pages_csv
