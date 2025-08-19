from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

import structlog


def _get_json_processors() -> list:
    return [
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ]


def setup_logging(level: int = logging.INFO) -> None:
    """
    Консольное JSON-логирование (идемпотентно).
    Для логов в файл вызови add_file_logging().
    """
    logging.basicConfig(level=level, format="%(message)s")
    structlog.configure(
        processors=_get_json_processors(),
        wrapper_class=structlog.make_filtering_bound_logger(level),
        cache_logger_on_first_use=True,
    )


def add_file_logging(log_file: Path, level: int = logging.INFO) -> None:
    """
    Подключает RotatingFileHandler, который пишет JSON-строки.
    Избегаем дублирования по совпадению baseFilename.
    """
    log_file.parent.mkdir(parents=True, exist_ok=True)
    root_logger = logging.getLogger()

    target = log_file.as_posix()
    for h in root_logger.handlers:
        if isinstance(h, RotatingFileHandler) and getattr(h, "baseFilename", "") == target:
            return

    fh = RotatingFileHandler(target, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter("%(message)s"))
    root_logger.addHandler(fh)


log = structlog.get_logger()
