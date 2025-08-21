from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from types import ModuleType


def _ensure_external_on_path() -> Path | None:
    """
    Add external/PDMX (or OMR_PDMX_PATH) to sys.path so we can import its modules.
    Returns the chosen root, or None if not found.
    """
    # 1) Env var has priority
    env = os.environ.get("OMR_PDMX_PATH")
    candidates: list[Path] = []
    if env:
        candidates.append(Path(env))

    # 2) Project-relative fallback: <repo_root>/external/PDMX
    here = Path(__file__).resolve()
    repo_root = here.parents[2]  # …/src/pdmx/__init__.py -> …/src -> …/
    candidates.append(repo_root / "external" / "PDMX")

    for root in candidates:
        if root.exists():
            if str(root) not in sys.path:
                sys.path.insert(0, str(root))
            return root
    return None


_ext = _ensure_external_on_path()


# Make top-level 'reading' and 'writing' packages visible under 'pdmx.reading', 'pdmx.writing'
def _alias_package(alias: str, target: str) -> None:
    try:
        mod = importlib.import_module(target)
    except Exception:
        return
    # Ensure parent ('pdmx') exists in sys.modules (we are it)
    pkg = sys.modules.get("pdmx")
    if not isinstance(pkg, ModuleType):
        pkg = ModuleType("pdmx")
        pkg.__path__ = []  # type: ignore[attr-defined]
        sys.modules["pdmx"] = pkg
    sys.modules[alias] = mod


_alias_package("pdmx.reading", "reading")
_alias_package("pdmx.writing", "writing")

# Optional convenience – expose 'load' if present as pdmx.load
try:
    _music = importlib.import_module("reading.music")
    if hasattr(_music, "load"):
        load = _music.load  # type: ignore[assignment]
except Exception:
    pass
