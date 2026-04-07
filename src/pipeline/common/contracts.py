from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

import pandas as pd


@dataclass
class PipelineContext:
    run_id: str = field(default_factory=lambda: str(uuid4()))
    input_path: Optional[Path] = None
    output_dir: Optional[Path] = None
    raw_df: Optional[pd.DataFrame] = None
    clean_df: Optional[pd.DataFrame] = None
    schema: dict[str, dict[str, Any]] = field(default_factory=dict)
    quality_report: dict[str, Any] = field(default_factory=dict)
    transform_log: list[dict[str, Any]] = field(default_factory=list)
    analysis_results: dict[str, Any] = field(default_factory=dict)
    charts: list[str] = field(default_factory=list)
    insights: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def add_warning(self, message: str) -> None:
        if message not in self.warnings:
            self.warnings.append(message)

    def add_error(
        self,
        *,
        stage: str,
        message: str,
        error_type: str = "pipeline_error",
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        self.errors.append(
            {
                "stage": stage,
                "type": error_type,
                "message": message,
                "details": details or {},
            }
        )

    def log_transform(
        self,
        *,
        action: str,
        column: Optional[str] = None,
        rows_affected: int = 0,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        entry = {
            "action": action,
            "rows_affected": rows_affected,
            "details": details or {},
        }
        if column is not None:
            entry["column"] = column
        self.transform_log.append(entry)


class PipelineExecutionError(Exception):
    """Structured, user-safe pipeline exception."""

    def __init__(
        self,
        *,
        stage: str,
        message: str,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.stage = stage
        self.message = message
        self.details = details or {}
