from collections.abc import Callable, Iterable
from pathlib import Path

from omr_lab.common.config import AppConfig
from omr_lab.omr_rules.pipeline import run_rules_pipeline

PipelineFn = Callable[[Iterable[Path], Path, AppConfig | None], None]


def get_registry() -> dict[str, PipelineFn]:
    return {
        "rules": run_rules_pipeline,
        # "hybrid": run_hybrid_pipeline, # добавим позже
        # "ai": run_ai_pipeline, # добавим позже
        # "baseline": run_baseline_pipeline
    }
