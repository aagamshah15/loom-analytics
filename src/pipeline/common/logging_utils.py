from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Optional

try:
    from loguru import logger as loguru_logger
except ImportError:  # pragma: no cover - optional fallback
    loguru_logger = None


def configure_logger(run_id: str, output_dir: Optional[Path] = None) -> Any:
    log_format = os.getenv("PIPELINE_LOG_FORMAT", "human").lower()
    log_file = None
    if output_dir is not None:
        log_dir = output_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{run_id}.json"

    if loguru_logger is not None:
        loguru_logger.remove()
        serialize = log_format == "json"
        loguru_logger.add(
            sys.stdout,
            serialize=serialize,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        )
        if log_file is not None:
            loguru_logger.add(log_file, serialize=True)
        return loguru_logger.bind(run_id=run_id)

    logger = logging.getLogger(f"pipeline.{run_id}")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
    if log_file is not None:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(file_handler)
    logger.propagate = False
    return _StdLoggerAdapter(logger, run_id=run_id)


class _StdLoggerAdapter:
    def __init__(self, logger: logging.Logger, *, run_id: str) -> None:
        self._logger = logger
        self._run_id = run_id

    def info(self, message: str, **kwargs: Any) -> None:
        self._emit("info", message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        self._emit("warning", message, **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        self._emit("error", message, **kwargs)

    def debug(self, message: str, **kwargs: Any) -> None:
        self._emit("debug", message, **kwargs)

    def _emit(self, level: str, message: str, **kwargs: Any) -> None:
        payload = {"run_id": self._run_id, "message": message, **kwargs}
        if any(isinstance(handler, logging.FileHandler) for handler in self._logger.handlers):
            self._logger.log(_level_value(level), json.dumps(payload, default=str))
        else:
            self._logger.log(_level_value(level), f"{message} | {json.dumps(kwargs, default=str)}")


def _level_value(level: str) -> int:
    return getattr(logging, level.upper(), logging.INFO)
