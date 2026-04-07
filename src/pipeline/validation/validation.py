from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from pipeline.common.config import PipelineConfig
from pipeline.common.contracts import PipelineContext, PipelineExecutionError


def run_validation(context: PipelineContext, _: PipelineConfig) -> None:
    if context.raw_df is None:
        raise PipelineExecutionError(stage="validation", message="Validation requires an ingested dataframe.")

    df = context.raw_df
    schema = {column: _infer_column_schema(df[column], len(df)) for column in df.columns}
    missing = {
        column: {
            "count": int(df[column].isna().sum()),
            "pct": round(float(df[column].isna().mean() * 100), 2),
        }
        for column in df.columns
    }

    duplicates_mask = df.duplicated()
    duplicates_count = int(duplicates_mask.sum())
    duplicate_samples = df[duplicates_mask].head(3).to_dict(orient="records")

    outliers = {}
    numeric_columns = [col for col, meta in schema.items() if meta["semantic_type"] == "numeric"]
    for column in numeric_columns:
        series = pd.to_numeric(df[column], errors="coerce").dropna()
        if series.empty:
            continue
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            count = 0
            lower = float(q1)
            upper = float(q3)
        else:
            lower = float(q1 - 1.5 * iqr)
            upper = float(q3 + 1.5 * iqr)
            mask = (pd.to_numeric(df[column], errors="coerce") < lower) | (
                pd.to_numeric(df[column], errors="coerce") > upper
            )
            count = int(mask.fillna(False).sum())
        outliers[column] = {
            "count": count,
            "pct": round((count / len(df)) * 100, 2) if len(df) else 0.0,
            "bounds": {"lower": lower, "upper": upper},
        }

    constant_columns = [
        column
        for column in df.columns
        if df[column].dropna().nunique() <= 1 and len(df[column].dropna()) > 0
    ]
    high_cardinality_columns = [
        column for column in df.columns if df[column].nunique(dropna=True) > 0.95 * len(df)
    ]
    mixed_type_columns = [
        column for column, meta in schema.items() if meta.get("mixed_types_detected", False)
    ]

    quality_score = _compute_quality_score(
        missing_rate=float(df.isna().mean().mean()) if len(df.columns) else 0.0,
        duplicate_rate=(duplicates_count / len(df)) if len(df) else 0.0,
        outlier_density=(
            np.mean([item["count"] / len(df) for item in outliers.values()]) if outliers and len(df) else 0.0
        ),
    )

    warnings = []
    if quality_score < 60:
        warnings.append("Data quality score is below 60.")
    if not numeric_columns:
        warnings.append("No numeric columns found for analysis.")
    if not any(meta["semantic_type"] == "date" for meta in schema.values()):
        warnings.append("No date column detected - time series skipped.")
    if len(df.columns) == 1:
        warnings.append("Single-column file: limited insights available.")

    for warning in warnings:
        context.add_warning(warning)

    context.schema = schema
    context.quality_report = {
        "score": quality_score,
        "row_count": int(df.shape[0]),
        "column_count": int(df.shape[1]),
        "missing_values": missing,
        "duplicates": {
            "count": duplicates_count,
            "pct": round((duplicates_count / len(df)) * 100, 2) if len(df) else 0.0,
            "sample_rows": duplicate_samples,
        },
        "outliers": outliers,
        "constant_columns": constant_columns,
        "high_cardinality_columns": high_cardinality_columns,
        "mixed_type_columns": mixed_type_columns,
        "warnings": warnings,
    }


def _infer_column_schema(series: pd.Series, row_count: int) -> dict[str, Any]:
    non_null = series.dropna()
    semantic_type = "categorical"
    mixed_types = False

    if pd.api.types.is_numeric_dtype(series):
        semantic_type = "numeric"
    elif pd.api.types.is_datetime64_any_dtype(series):
        semantic_type = "date"
    elif not non_null.empty:
        sample = non_null.astype(str).head(50)
        inferred_kinds = {_infer_value_kind(value) for value in sample if str(value).strip()}
        mixed_types = len(inferred_kinds) > 1
        datetime_ratio = _datetime_ratio(sample)
        numeric_ratio = pd.to_numeric(sample, errors="coerce").notna().mean()
        if datetime_ratio >= 0.8:
            semantic_type = "date"
        elif numeric_ratio >= 0.8:
            semantic_type = "numeric"
        elif inferred_kinds == {"boolean"}:
            semantic_type = "boolean"

    return {
        "dtype": str(series.dtype),
        "semantic_type": semantic_type,
        "null_count": int(series.isna().sum()),
        "null_pct": round(float(series.isna().mean() * 100), 2),
        "unique_count": int(series.nunique(dropna=True)),
        "mixed_types_detected": mixed_types,
        "likely_id": bool(series.nunique(dropna=True) > 0.95 * row_count) if row_count else False,
    }


def _infer_value_kind(value: str) -> str:
    lowered = value.strip().lower()
    if lowered in {"true", "false", "yes", "no"}:
        return "boolean"
    if pd.to_numeric(pd.Series([value]), errors="coerce").notna().iloc[0]:
        return "numeric"
    if pd.to_datetime(pd.Series([value]), errors="coerce").notna().iloc[0]:
        return "date"
    return "string"


def _datetime_ratio(sample: pd.Series) -> float:
    sample_strings = sample.astype(str).str.strip()
    if not sample_strings.str.contains(r"[-/:]").any():
        return 0.0
    return pd.to_datetime(sample_strings, errors="coerce").notna().mean()


def _compute_quality_score(*, missing_rate: float, duplicate_rate: float, outlier_density: float) -> float:
    penalty = ((0.5 * missing_rate) + (0.3 * duplicate_rate) + (0.2 * outlier_density)) * 100
    return round(max(0.0, 100.0 - penalty), 2)
