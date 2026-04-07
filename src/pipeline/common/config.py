from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional, Union

import yaml


@dataclass
class CleaningConfig:
    numeric_missing_strategy: str = "median"
    categorical_missing_strategy: str = "mode"
    date_missing_strategy: str = "forward_fill"
    duplicate_strategy: str = "drop_duplicates"
    case_strategy: str = "lower"
    categorical_fill_value: str = "UNKNOWN"
    auto_convert_numeric_strings: bool = True
    numeric_parse_threshold: float = 0.8
    always_drop_fully_missing_columns: bool = True
    always_drop_zero_variance_columns: bool = True


@dataclass
class AnalysisConfig:
    correlation_threshold: float = 0.7
    scatter_threshold: float = 0.5
    outlier_zscore_threshold: float = 3.0
    anomaly_zscore_threshold: float = 3.5
    skew_flag_threshold: float = 1.0
    severe_skew_threshold: float = 2.0
    top_n_segments: int = 5


@dataclass
class VisualizationConfig:
    max_charts: int = 8


@dataclass
class PipelineConfig:
    cleaning: CleaningConfig = field(default_factory=CleaningConfig)
    analysis: AnalysisConfig = field(default_factory=AnalysisConfig)
    visualization: VisualizationConfig = field(default_factory=VisualizationConfig)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def load_pipeline_config(config_path: Optional[Union[str, Path]] = None) -> PipelineConfig:
    config = PipelineConfig()
    if config_path is None:
        return config

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    data = yaml.safe_load(path.read_text()) or {}
    _merge_dataclass(config.cleaning, data.get("cleaning", {}))
    _merge_dataclass(config.analysis, data.get("analysis", {}))
    _merge_dataclass(config.visualization, data.get("visualization", {}))
    return config


def _merge_dataclass(target: Any, values: dict[str, Any]) -> None:
    for key, value in values.items():
        if hasattr(target, key):
            setattr(target, key, value)
