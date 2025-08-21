from __future__ import annotations

import platform
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Any

from omr_lab.common.logging import add_file_logging, log


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _git_info(root: Path) -> dict[str, Any]:
    def _cmd(args: list[str]) -> str | None:
        try:
            r = subprocess.run(
                args, cwd=root, capture_output=True, text=True, check=True
            )
            return r.stdout.strip()
        except Exception:
            return None

    return {
        "commit": _cmd(["git", "rev-parse", "HEAD"]),
        "branch": _cmd(["git", "rev-parse", "--abbrev-ref", "HEAD"]),
        "is_dirty": bool(_cmd(["git", "status", "--porcelain"])),
    }


def _env_info() -> dict[str, Any]:
    pkgs: dict[str, str] = {}
    try:
        for dist in importlib_metadata.distributions():
            # PackageMetadata of a distribution behaves like a mapping, but mypy types are conservative.
            # Use try/except for reliability instead of .get().
            try:
                name = dist.metadata["Name"]  # type: ignore[index]
            except Exception:
                continue
            if not isinstance(name, str) or not name:
                continue
            version = dist.version or ""
            pkgs[name] = version
    except Exception:
        # If collecting metadata fails, just return whatever was gathered.
        pass

    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "packages": pkgs,
    }


@dataclass
class RunContext:
    run_dir: Path
    logs_dir: Path
    configs_dir: Path
    outputs_dir: Path
    manifest_path: Path
    log_path: Path
    run_id: str
    impl: str

    @staticmethod
    def create(impl: str, run_dir: Path) -> RunContext:
        run_dir.mkdir(parents=True, exist_ok=True)
        logs_dir = run_dir / "logs"
        configs_dir = run_dir / "configs"
        outputs_dir = run_dir
        manifest_path = run_dir / "manifest.json"
        log_path = logs_dir / "run.jsonl"
        ctx = RunContext(
            run_dir=run_dir,
            logs_dir=logs_dir,
            configs_dir=configs_dir,
            outputs_dir=outputs_dir,
            manifest_path=manifest_path,
            log_path=log_path,
            run_id=run_dir.name,
            impl=impl,
        )
        add_file_logging(log_path)
        log.info("run_start", run_id=ctx.run_id, impl=impl, run_dir=str(run_dir))
        return ctx

    def save_configs(
        self, effective_cfg: dict | None, source_cfg_path: Path | None
    ) -> None:
        self.configs_dir.mkdir(parents=True, exist_ok=True)
        if source_cfg_path:
            dst = self.configs_dir / "used.yaml"
            try:
                dst.write_text(
                    Path(source_cfg_path).read_text(encoding="utf-8"), encoding="utf-8"
                )
            except Exception as e:
                log.warning("config_copy_failed", error=str(e))
        if effective_cfg is not None:
            eff = self.configs_dir / "effective.json"
            import json

            eff.write_text(
                json.dumps(effective_cfg, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    def write_manifest(self, inputs: list[str]) -> None:
        import json

        data = {
            "run_id": self.run_id,
            "impl": self.impl,
            "created_at": _now_iso(),
            "inputs": inputs,
            "git": _git_info(self.run_dir),
            "env": _env_info(),
        }
        self.manifest_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def finalize(self, status: str = "ok") -> None:
        try:
            import json

            data: dict[str, Any] = {}
            if self.manifest_path.exists():
                data = json.loads(self.manifest_path.read_text(encoding="utf-8"))
            data["finished_at"] = _now_iso()
            data["status"] = status
            self.manifest_path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        finally:
            log.info("run_end", run_id=self.run_id, status=status)
