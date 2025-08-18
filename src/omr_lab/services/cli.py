from __future__ import annotations
import typer
from pathlib import Path
from typing import Optional

from omr_lab.common.logging import setup_logging, log
from omr_lab.common.config import load_yaml, AppConfig

app = typer.Typer(help="OMR Lab CLI. Pipelines: rules, hybrid, ai. With unified evaluation.")

@app.callback()
def main(verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose JSON logs.")):
    setup_logging()
    if verbose:
        log.info("verbose_enabled")

@app.command("prepare-data")
def prepare_data(
    input_path: Path = typer.Argument(..., help="Input data folder (raw)."),
    output_path: Path = typer.Argument(..., help="Output folder (processed)."),
    dpi: int = typer.Option(300, help="Normalize images to this DPI."),
):
    log.info("prepare_data_start", input=str(input_path), output=str(output_path), dpi=dpi)
    # TODO: implement PDF->image, normalization, deskew, etc.
    output_path.mkdir(parents=True, exist_ok=True)
    log.info("prepare_data_done")

@app.command("run-pipeline")
def run_pipeline(
    impl: str = typer.Option(..., help="Implementation: rules|hybrid|ai|baseline"),
    config: Optional[Path] = typer.Option(None, help="YAML config for pipeline."),
    input_path: Path = typer.Argument(..., help="Input images folder or file."),
    out: Path = typer.Option(Path("experiments/runs/run1"), help="Output directory for artifacts."),
):
    cfg: AppConfig | None = load_yaml(config) if config else None
    log.info("run_pipeline_start", impl=impl, config=str(config) if config else None, input=str(input_path), out=str(out))
    out.mkdir(parents=True, exist_ok=True)
    # TODO: dispatch to selected pipeline and produce MusicXML/MIDI and logs.
    (out / "placeholder.txt").write_text(f"Pipeline {impl} would run here. Config: {cfg.model_dump() if cfg else '{}'}\n")
    log.info("run_pipeline_done", out=str(out))

@app.command("eval-run")
def eval_run(
    pred: Path = typer.Option(..., help="Predictions folder."),
    gt: Path = typer.Option(..., help="Ground truth annotations folder."),
    config: Optional[Path] = typer.Option(None, help="YAML config for evaluation."),
    out: Path = typer.Option(Path("experiments/reports/eval1"), help="Output directory for report."),
):
    log.info("eval_start", pred=str(pred), gt=str(gt), config=str(config) if config else None)
    out.mkdir(parents=True, exist_ok=True)
    # TODO: compute metrics and write CSV/HTML report.
    (out / "metrics.csv").write_text("metric,value\nprecision,0.0\nrecall,0.0\nf1,0.0\n")
    log.info("eval_done", out=str(out))

@app.command("compare")
def compare(
    runs: list[Path] = typer.Argument(..., help="List of run folders to compare."),
    metrics: str = typer.Option("f1", help="Comma-separated list of metrics."),
    out: Path = typer.Option(Path("experiments/reports/summary"), help="Output folder."),
):
    log.info("compare_start", runs=[str(r) for r in runs], metrics=metrics)
    out.mkdir(parents=True, exist_ok=True)
    # TODO: aggregate metrics across runs, produce a summary.
    (out / "summary.csv").write_text("run,metric,value\n" + "\n".join(f"{r.name},f1,0.0" for r in runs) + "\n")
    log.info("compare_done", out=str(out))

@app.command("report")
def report(
    source: Path = typer.Argument(..., help="Folder with metrics to summarize into a report."),
    out: Path = typer.Option(Path("experiments/reports/final"), help="Output folder."),
):
    log.info("report_start", source=str(source))
    out.mkdir(parents=True, exist_ok=True)
    # TODO: compile HTML/PDF report.
    (out / "report.txt").write_text("Final report would be generated here.\n")
    log.info("report_done", out=str(out))

if __name__ == "__main__":
    app()
