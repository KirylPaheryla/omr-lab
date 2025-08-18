from __future__ import annotations
from pydantic import BaseModel
from typing import Any
import yaml
from pathlib import Path

class AppConfig(BaseModel):
    impl: str | None = None
    params: dict[str, Any] = {}

def load_yaml(path: str | Path) -> AppConfig:
    p = Path(path)
    data = yaml.safe_load(p.read_text())
    return AppConfig(**data)
