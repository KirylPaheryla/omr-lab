from __future__ import annotations

from pathlib import Path

import typer

from omr_lab.common.config import AppConfig, load_yaml
from omr_lab.common.logging import log, setup_logging
from omr_lab.eval.compare import compare_runs
from omr_lab.eval.filelevel import eval_filelevel
from omr_lab.eval.report import build_report
from omr_lab.services.pipeline_registry import get_registry
from omr_lab.services.prepare import prepare_dataset

app = typer.Typer(help="OMR Lab CLI. Pipelines: rules, hybrid, ai. With unified evaluation.")

# ---- Typer param objects (вынесены, чтобы избежать B008) ----
OPT_VERBOSE = typer.Option(False, "--verbose", "-v", help="Verbose JSON logs.")

ARG_INPUT_RAW = typer.Argument(..., help="Input data folder (raw).")
ARG_OUTPUT_PROCESSED = typer.Argument(..., help="Output folder (processed).")
OPT_DPI = typer.Option(300, help="Normalize images to this DPI.")

OPT_IMPL = typer.Option(..., help="Implementation: rules|hybrid|ai|baseline")
OPT_CONFIG = typer.Option(None, help="YAML config for pipeline.")
ARG_INPUT_PATH = typer.Argument(..., help="Input images folder or file.")
OPT_OUT_RUN = typer.Option(Path("experiments/runs/run1"), help="Output directory for artifacts.")

OPT_PRED = typer.Option(..., help="Predictions folder.")
OPT_GT = typer.Option(..., help="Ground truth annotations folder.")
OPT_EVAL_CONFIG = typer.Option(None, help="YAML config for evaluation.")
OPT_OUT_EVAL = typer.Option(Path("experiments/reports/eval1"), help="Output directory for report.")

ARG_RUNS = typer.Argument(..., help="List of run folders to compare.")
OPT_METRICS = typer.Option("f1", help="Comma-separated list of metrics.")
OPT_OUT_SUMMARY = typer.Option(Path("experiments/reports/summary"), help="Output folder.")

ARG_SOURCE = typer.Argument(..., help="Folder with metrics to summarize into a report.")
OPT_OUT_FINAL = typer.Option(Path("experiments/reports/final"), help="Output folder.")
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
    log.info("prepare_data_start", input=str(input_path), output=str(output_path), dpi=dpi)
    copied = prepare_dataset(input_path, output_path)
    log.info("prepare_data_done", copied=copied)


@app.command("run-pipeline")
def run_pipeline(
    impl: str = OPT_IMPL,
    config: Path | None = OPT_CONFIG,
    input_path: Path = ARG_INPUT_PATH,
    out: Path = OPT_OUT_RUN,
) -> None:
    registry = get_registry()
    if impl not in registry:
        raise typer.BadParameter(
            f"Unknown implementation: {impl}. Available: {', '.join(registry)}"
        )
    cfg: AppConfig | None = load_yaml(config) if config else None
    log.info(
        "run_pipeline_start",
        impl=impl,
        config=str(config) if config else None,
        input=str(input_path),
        out=str(out),
    )
    out.mkdir(parents=True, exist_ok=True)
    inputs: list[Path] = [input_path]
    registry[impl](inputs, out, cfg)
    log.info("run_pipeline_done", out=str(out))


@app.command("eval-run")
def eval_run(
    pred: Path = OPT_PRED,
    gt: Path = OPT_GT,
    config: Path | None = OPT_EVAL_CONFIG,
    out: Path = OPT_OUT_EVAL,
) -> None:
    log.info("eval_start", pred=str(pred), gt=str(gt), config=str(config) if config else None)
    out.mkdir(parents=True, exist_ok=True)
    eval_filelevel(pred, gt, out / "metrics.csv")
    log.info("eval_done", out=str(out))


@app.command("compare")
def compare(
    runs: list[Path] = ARG_RUNS,
    metrics: str = OPT_METRICS,
    out: Path = OPT_OUT_SUMMARY,
) -> None:
    log.info("compare_start", runs=[str(r) for r in runs], metrics=metrics)
    out.mkdir(parents=True, exist_ok=True)
    compare_runs(runs, out / "summary.csv")
    log.info("compare_done", out=str(out))


@app.command("report")
def report(
    source: Path = ARG_SOURCE,
    out: Path = OPT_OUT_FINAL,
) -> None:
    log.info("report_start", source=str(source))
    out.mkdir(parents=True, exist_ok=True)
    build_report(source, out / "report.txt")
    log.info("report_done", out=str(out))


if __name__ == "__main__":
    app()
