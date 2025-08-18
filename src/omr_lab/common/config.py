from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel


class AppConfig(BaseModel):
    impl: str | None = None
    params: dict[str, Any] = {}


def load_yaml(path: str | Path) -> AppConfig:
    p = Path(path)
    data = yaml.safe_load(p.read_text())
    return AppConfig(**data)
