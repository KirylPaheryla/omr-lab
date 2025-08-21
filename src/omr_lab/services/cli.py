from __future__ import annotations

from pathlib import Path

import typer

from omr_lab.common.config import AppConfig, load_yaml
from omr_lab.common.logging import log, setup_logging
from omr_lab.common.runctx import RunContext
from omr_lab.data.normalize import normalize_folder
from omr_lab.data.qa import qa_coco
from omr_lab.data.render import render_dataset
from omr_lab.data.split import stratified_split
from omr_lab.data.synth import synth_batch
from omr_lab.eval.compare import compare_runs
from omr_lab.eval.filelevel import eval_filelevel
from omr_lab.eval.report import build_report
from omr_lab.services.pipeline_registry import get_registry
from omr_lab.services.prepare import prepare_dataset

app = typer.Typer(
    help="OMR Lab CLI. Pipelines: rules, hybrid, ai. With unified evaluation."
)

OPT_VERBOSE = typer.Option(False, "--verbose", "-v", help="Verbose JSON logs.")

ARG_INPUT_RAW = typer.Argument(..., help="Input data folder (raw).")
ARG_OUTPUT_PROCESSED = typer.Argument(..., help="Output folder (processed).")
OPT_DPI = typer.Option(300, help="Normalize images to this DPI.")

OPT_IMPL = typer.Option(..., help="Implementation: rules|hybrid|ai|baseline")
OPT_CONFIG = typer.Option(None, help="YAML config for pipeline.")
ARG_INPUT_PATH = typer.Argument(..., help="Input images folder or file.")
OPT_OUT_RUN = typer.Option(
    Path("experiments/runs/run1"), help="Output directory for artifacts."
)

OPT_PRED = typer.Option(..., help="Predictions folder.")
OPT_GT = typer.Option(..., help="Ground truth annotations folder.")
OPT_EVAL_CONFIG = typer.Option(None, help="YAML config for evaluation.")
OPT_OUT_EVAL = typer.Option(
    Path("experiments/reports/eval1"), help="Output directory for report."
)

ARG_RUNS = typer.Argument(..., help="List of run folders to compare.")
OPT_METRICS = typer.Option("f1", help="Comma-separated list of metrics.")
OPT_OUT_SUMMARY = typer.Option(
    Path("experiments/reports/summary"), help="Output folder."
)

ARG_SOURCE = typer.Argument(..., help="Folder with metrics to summarize into a report.")
OPT_OUT_FINAL = typer.Option(Path("experiments/reports/final"), help="Output folder.")

# data-* aliases
ARG_MXL_IN = typer.Argument(..., help="Folder with MusicXML files.")
ARG_IR_OUT = typer.Argument(..., help="Folder to write IR JSON.")
OPT_SYNTH_OUT = typer.Option(
    Path("data/synth"),
    "--out",
    "--out-dir",
    "-o",
    help="Output folder for synthetic MusicXML.",
)
OPT_SYNTH_N = typer.Option(
    10, "--n", "-n", help="How many synthetic scores to generate."
)
OPT_SPLIT_OUT = typer.Option(
    Path("data/splits"),
    "--out",
    "--out-dir",
    "-o",
    help="Output folder for split files.",
)

# data-normalize speed-ups
OPT_NORM_JOBS = typer.Option(
    1, "--jobs", "-j", help="Parallel workers for normalization."
)
OPT_NORM_SKIP = typer.Option(
    True,
    "--skip-if-exists/--no-skip-if-exists",
    help="Skip files with up-to-date outputs.",
)
OPT_NORM_LYR = typer.Option(
    False, "--lyrics-only", help="Normalize only files that contain <lyric>."
)
OPT_NORM_NO_KEY = typer.Option(False, "--no-key", help="Disable key analysis (faster).")
OPT_NORM_QUIET = typer.Option(
    False, "--quiet-warnings", help="Suppress music21 warnings (faster, cleaner logs)."
)

# ---- PDMX export (args & options) ----
ARG_PDMX_ROOT = typer.Argument(
    ..., help="Path to PDMX root (folder with data/, metadata/, PDMX.csv)."
)
ARG_PDMX_OUT = typer.Argument(..., help="Output folder for exported MusicXML/MXL.")
OPT_PDMX_CSV = typer.Option(
    None, "--csv", help="Optional path to PDMX.csv (defaults to <root>/PDMX.csv)."
)
OPT_PDMX_JOBS = typer.Option(8, "--jobs", "-j", help="Parallel workers for export.")
OPT_PDMX_LYRICS = typer.Option(
    True, "--lyrics-only/--all", help="Export only rows with has_lyrics=1."
)
OPT_PDMX_NO_CONFLICT = typer.Option(
    True,
    "--no-conflict-only/--include-conflict",
    help="Exclude rows with license_conflict==1 if present.",
)
OPT_PDMX_EXT = typer.Option(
    "musicxml", "--ext", help="musicxml or mxl (default: musicxml)."
)

# QA params (avoid B008 by hoisting Typer objects)
ARG_COCO_PATH = typer.Argument(..., help="Path to COCO JSON (from data-render).")
OPT_PAGES_CSV = typer.Option(
    None, "--pages", help="Optional pages.csv for has_lyrics stats."
)

# IR QA
ARG_IR_DIR = typer.Argument(..., help="Folder with IR JSON files.")
OPT_IR_QA_OUT = typer.Option(
    None,
    "--out",
    help="Optional CSV path to write per-file metrics (e.g., data/annotations/openscore/ir_summary.csv).",
)

# render/qa
OPT_RENDER_IMG_DIR = typer.Option(
    Path("data/images"), "--images", "-I", help="Where to store rendered page images."
)
OPT_RENDER_ANN_DIR = typer.Option(
    Path("data/annotations_pix"),
    "--ann-out",
    "-A",
    help="Where to store COCO/CSV annotations.",
)
OPT_RENDER_MUSESCORE = typer.Option(
    None,
    "--musescore-cmd",
    help="Path to MuseScore CLI executable (e.g. MuseScore4.exe).",
)
OPT_RENDER_VEROVIO = typer.Option(
    None, "--verovio-cmd", help="Path to Verovio CLI executable (e.g. verovio)."
)
OPT_RENDER_DPI = typer.Option(
    300, "--dpi", "-r", help="PNG DPI for MuseScore renderer."
)
# --------------------------------------------------------------


@app.callback()
def main(verbose: bool = OPT_VERBOSE) -> None:
    setup_logging()
    if verbose:
        log.info("verbose_enabled")


@app.command("prepare-data")
def prepare_data(
    input_path: Path = ARG_INPUT_RAW,
    output_path: Path = ARG_OUTPUT_PROCESSED,
    dpi: int = OPT_DPI,
) -> None:
    """Prepare dataset: copy/convert raw input into processed format."""
    from omr_lab.common.logging import add_file_logging

    add_file_logging(output_path / "logs" / "prepare.jsonl")
    log.info(
        "prepare_data_start", input=str(input_path), output=str(output_path), dpi=dpi
    )
    copied = prepare_dataset(input_path, output_path)
    log.info("prepare_data_done", copied=copied)


@app.command("data-normalize")
def data_normalize(
    musicxml_dir: Path = ARG_MXL_IN,
    ir_out: Path = ARG_IR_OUT,
    jobs: int = OPT_NORM_JOBS,
    skip_if_exists: bool = OPT_NORM_SKIP,
    lyrics_only: bool = OPT_NORM_LYR,
    no_key: bool = OPT_NORM_NO_KEY,
    quiet_warnings: bool = OPT_NORM_QUIET,  # ← добавили
) -> None:
    from omr_lab.common.logging import add_file_logging

    add_file_logging(ir_out / "logs" / "normalize.jsonl")
    log.info(
        "normalize_start",
        input=str(musicxml_dir),
        out=str(ir_out),
        jobs=jobs,
        skip_if_exists=skip_if_exists,
        lyrics_only=lyrics_only,
        analyze_key=not no_key,
    )
    count = normalize_folder(
        musicxml_dir,
        ir_out,
        jobs=jobs,
        skip_if_exists=skip_if_exists,
        lyrics_only=lyrics_only,
        analyze_key=not no_key,
        quiet_warnings=quiet_warnings,  # ← добавили
    )
    log.info("normalize_done", count=count)


@app.command("pdmx-export")
def pdmx_export(
    pdmx_root: Path = ARG_PDMX_ROOT,
    out_dir: Path = ARG_PDMX_OUT,
    csv_path: Path | None = OPT_PDMX_CSV,
    jobs: int = OPT_PDMX_JOBS,
    lyrics_only: bool = OPT_PDMX_LYRICS,
    no_conflict_only: bool = OPT_PDMX_NO_CONFLICT,
    ext: str = OPT_PDMX_EXT,
) -> None:
    """Export PDMX MusicRender JSON to MusicXML/MXL files."""
    from omr_lab.common.logging import add_file_logging
    from omr_lab.data.pdmx_export import export_pdmx_to_musicxml

    add_file_logging(out_dir / "logs" / "pdmx_export.jsonl")
    log.info(
        "pdmx_export_start",
        root=str(pdmx_root),
        out=str(out_dir),
        csv=str(csv_path) if csv_path else None,
        jobs=jobs,
        lyrics_only=lyrics_only,
        no_conflict_only=no_conflict_only,
        ext=ext,
    )
    summary = export_pdmx_to_musicxml(
        pdmx_root,
        out_dir,
        csv_path=csv_path,
        jobs=jobs,
        lyrics_only=lyrics_only,
        no_conflict_only=no_conflict_only,
        ext=ext,
    )

    import typer as _ty

    if isinstance(summary, int):
        _ty.echo(f"exported={summary} failed=0 total={summary}")
    else:
        _ty.echo(
            f"exported={summary['exported']} failed={summary['failed']} total={summary['total']}"
        )


@app.command("data-synth")
def data_synth(
    out_dir: Path = OPT_SYNTH_OUT,
    n: int = OPT_SYNTH_N,
) -> None:
    """Generate synthetic MusicXML scores."""
    from omr_lab.common.logging import add_file_logging

    add_file_logging(out_dir / "logs" / "synth.jsonl")
    log.info("synth_start", out=str(out_dir), n=n)
    count = synth_batch(out_dir, n=n)
    log.info("synth_done", count=count)


@app.command("data-split")
def data_split(
    ir_dir: Path = ARG_IR_OUT,
    out_dir: Path = OPT_SPLIT_OUT,
) -> None:
    """Split dataset into train/val/test subsets using stratification."""
    from omr_lab.common.logging import add_file_logging

    add_file_logging(out_dir / "logs" / "split.jsonl")
    log.info("split_start", ir=str(ir_dir), out=str(out_dir))
    stratified_split(ir_dir, out_dir)
    log.info("split_done", out=str(out_dir))


@app.command("data-render")
def data_render(
    musicxml_dir: Path = ARG_MXL_IN,
    images_out: Path = OPT_RENDER_IMG_DIR,
    ann_out: Path = OPT_RENDER_ANN_DIR,
    musescore_cmd: str | None = OPT_RENDER_MUSESCORE,
    verovio_cmd: str | None = OPT_RENDER_VEROVIO,
    dpi: int = OPT_RENDER_DPI,
) -> None:
    """
    Render page images (MuseScore) and SVG (Verovio), collect COCO + pages.csv + links.csv.
    Requires CLI tools installed:
      - MuseScore CLI: see official docs (exporting PNG options).
      - Verovio CLI: see Verovio Reference Book (command-line / toolkit options).
    """
    from omr_lab.common.logging import add_file_logging

    add_file_logging(ann_out / "logs" / "render.jsonl")
    coco_path, pages_csv = render_dataset(
        input_dir=musicxml_dir,
        out_images=images_out,
        out_ann_dir=ann_out,
        musescore_cmd=musescore_cmd,
        verovio_cmd=verovio_cmd,
        dpi=dpi,
    )
    log.info("render_done", coco=str(coco_path), pages=str(pages_csv))


@app.command("data-qa")
def data_qa(
    coco_path: Path = ARG_COCO_PATH,
    pages_csv: Path | None = OPT_PAGES_CSV,
) -> None:
    """Run QA checks on dataset (COCO + optional pages.csv)."""
    from omr_lab.common.logging import add_file_logging

    add_file_logging(coco_path.parent / "logs" / "qa.jsonl")
    rep = qa_coco(coco_path, pages_csv)
    typer.echo(
        f"images={rep.images} annotations={rep.annotations} "
        f"bbox_coverage={rep.bbox_coverage_pct:.1f}% images_with_lyrics={rep.images_with_lyrics}"
    )


@app.command("ir-qa")
def ir_qa(
    ir_dir: Path = ARG_IR_DIR,
    out: Path | None = OPT_IR_QA_OUT,
) -> None:
    """Quality checks on IR (ScoreIR): coverage, syllabic distribution, dangling links."""
    from omr_lab.common.logging import add_file_logging
    from omr_lab.data.qa_ir import qa_ir_dir, write_ir_csv

    add_file_logging(ir_dir / "logs" / "ir_qa.jsonl")
    summary, rows = qa_ir_dir(ir_dir)

    # Краткая сводка в консоль
    pct = (
        (summary.files_with_lyrics / summary.files_total * 100.0)
        if summary.files_total
        else 0.0
    )
    typer.echo(
        f"files={summary.files_total} with_lyrics={summary.files_with_lyrics} ({pct:.1f}%) "
        f"parts={summary.parts} measures={summary.measures} notes={summary.notes} lyrics={summary.lyrics}"
    )
    typer.echo(
        f"empty_lyrics={summary.empty_lyrics} dangling_lyrics={summary.dangling_lyrics} "
        f"errors_txt={summary.error_txt_files} failed_json={summary.failed_json}"
    )
    typer.echo(
        "syllabic="
        + ", ".join(f"{k}={v}" for k, v in sorted(summary.syllabic_counts.items()))
    )

    if out:
        write_ir_csv(out, rows)
        typer.echo(f"Wrote per-file CSV → {out}")


@app.command("run-pipeline")
def run_pipeline(
    impl: str = OPT_IMPL,
    config: Path | None = OPT_CONFIG,
    input_path: Path = ARG_INPUT_PATH,
    out: Path = OPT_OUT_RUN,
) -> None:
    """Run a recognition pipeline (rules, hybrid, ai, baseline)."""
    registry = get_registry()
    if impl not in registry:
        raise typer.BadParameter(
            f"Unknown implementation: {impl}. Available: {', '.join(registry)}"
        )

    cfg_obj: AppConfig | None = load_yaml(config) if config else None
    cfg_effective: dict | None = cfg_obj.model_dump() if cfg_obj else None

    ctx = RunContext.create(impl=impl, run_dir=out)
    log.bind(run_id=ctx.run_id, impl=impl)
    ctx.write_manifest(inputs=[str(input_path)])
    ctx.save_configs(cfg_effective, config)

    log.info(
        "run_pipeline_start",
        impl=impl,
        config=str(config) if config else None,
        input=str(input_path),
        out=str(out),
    )
    out.mkdir(parents=True, exist_ok=True)
    inputs: list[Path] = [input_path]
    registry[impl](inputs, out, cfg_obj)
    ctx.finalize(status="ok")


@app.command("eval-run")
def eval_run(
    pred: Path = OPT_PRED,
    gt: Path = OPT_GT,
    config: Path | None = OPT_EVAL_CONFIG,
    out: Path = OPT_OUT_EVAL,
) -> None:
    """Evaluate run predictions vs ground truth annotations."""
    from omr_lab.common.logging import add_file_logging

    add_file_logging(out / "logs" / "eval.jsonl")
    log.info(
        "eval_start", pred=str(pred), gt=str(gt), config=str(config) if config else None
    )
    out.mkdir(parents=True, exist_ok=True)
    eval_filelevel(pred, gt, out / "metrics.csv")
    log.info("eval_done", out=str(out))


@app.command("compare")
def compare(
    runs: list[Path] = ARG_RUNS,
    metrics: str = OPT_METRICS,
    out: Path = OPT_OUT_SUMMARY,
) -> None:
    """Compare multiple runs by chosen metrics."""
    from omr_lab.common.logging import add_file_logging

    add_file_logging(out / "logs" / "compare.jsonl")
    log.info("compare_start", runs=[str(r) for r in runs], metrics=metrics)
    out.mkdir(parents=True, exist_ok=True)
    compare_runs(runs, out / "summary.csv")
    log.info("compare_done", out=str(out))


@app.command("report")
def report(
    source: Path = ARG_SOURCE,
    out: Path = OPT_OUT_FINAL,
) -> None:
    """Build a final evaluation report from metrics summary."""
    from omr_lab.common.logging import add_file_logging

    add_file_logging(out / "logs" / "report.jsonl")
    log.info("report_start", source=str(source))
    out.mkdir(parents=True, exist_ok=True)
    build_report(source, out / "report.txt")
    log.info("report_done", out=str(out))


if __name__ == "__main__":
    app()
