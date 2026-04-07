from __future__ import annotations

from itertools import combinations

import numpy as np
import pandas as pd

from pipeline.common.config import PipelineConfig
from pipeline.common.contracts import PipelineContext, PipelineExecutionError

try:
    from scipy import stats
except ImportError:  # pragma: no cover - optional fallback
    stats = None


def run_analysis(context: PipelineContext, config: PipelineConfig) -> None:
    df = context.clean_df
    if df is None:
        raise PipelineExecutionError(stage="analysis", message="Analysis requires a cleaned dataframe.")

    numeric_columns = df.select_dtypes(include=["number"]).columns.tolist()
    date_columns = [
        column for column in df.columns if pd.api.types.is_datetime64_any_dtype(df[column])
    ]
    categorical_columns = [
        column
        for column in df.columns
        if column not in numeric_columns and column not in date_columns
    ]

    results: dict[str, object] = {
        "numeric_columns": numeric_columns,
        "categorical_columns": categorical_columns,
        "date_columns": date_columns,
        "summary_statistics": {},
        "correlations": {},
        "value_counts": {},
        "time_series": {},
        "anomalies": {},
        "distributions": {},
        "top_segments": {},
    }

    if numeric_columns:
        summary = df[numeric_columns].describe().transpose()
        summary["skew"] = df[numeric_columns].skew(numeric_only=True)
        summary["kurtosis"] = df[numeric_columns].kurtosis(numeric_only=True)
        results["summary_statistics"] = summary.round(4).to_dict(orient="index")

        if len(numeric_columns) >= 2:
            corr = df[numeric_columns].corr(method="pearson").round(4)
            results["correlations"] = corr.to_dict()

        results["anomalies"] = _detect_anomalies(df, numeric_columns, config.analysis.outlier_zscore_threshold)
        results["distributions"] = _distribution_summary(df, numeric_columns)
    else:
        context.add_warning("No numeric columns found for analysis.")

    for column in categorical_columns:
        if df[column].nunique(dropna=True) <= 25:
            results["value_counts"][column] = df[column].value_counts(dropna=False).head(10).to_dict()

    if date_columns and numeric_columns:
        results["time_series"] = _time_series_summary(df, date_columns[0], numeric_columns, context)
    else:
        context.add_warning("No date column detected - time series skipped.")

    if numeric_columns and categorical_columns:
        results["top_segments"] = _top_segments(df, categorical_columns, numeric_columns, config.analysis.top_n_segments)

    context.analysis_results = results


def _detect_anomalies(
    df: pd.DataFrame,
    numeric_columns: list[str],
    threshold: float,
) -> dict[str, dict[str, object]]:
    anomalies = {}
    for column in numeric_columns:
        series = pd.to_numeric(df[column], errors="coerce")
        if series.dropna().empty:
            continue
        z_scores = _zscore(series)
        mask = z_scores.abs() > threshold
        anomalies[column] = {
            "count": int(mask.fillna(False).sum()),
            "pct": round(float(mask.fillna(False).mean() * 100), 2),
            "indices": series.index[mask.fillna(False)].tolist()[:10],
            "threshold": threshold,
        }
    return anomalies


def _distribution_summary(df: pd.DataFrame, numeric_columns: list[str]) -> dict[str, dict[str, object]]:
    distributions = {}
    for column in numeric_columns:
        skew_value = float(df[column].skew())
        distributions[column] = {
            "skew": round(skew_value, 4),
            "is_skewed": abs(skew_value) > 1.0,
        }
    return distributions


def _time_series_summary(
    df: pd.DataFrame,
    date_column: str,
    numeric_columns: list[str],
    context: PipelineContext,
) -> dict[str, object]:
    target_column = _choose_time_series_target(numeric_columns, context.schema)
    working = df[[date_column, target_column]].dropna().sort_values(date_column)
    if working.empty:
        return {}

    rolling_window = min(30, max(3, len(working)))
    rolling = working[target_column].rolling(window=rolling_window, min_periods=max(2, rolling_window // 3)).mean()
    slope = 0.0
    monotonic_fraction = 0.0
    direction = "stable"
    if rolling.dropna().shape[0] >= 2:
        x = np.arange(len(rolling.dropna()))
        y = rolling.dropna().to_numpy()
        slope = float(np.polyfit(x, y, 1)[0])
        diffs = np.diff(y)
        if len(diffs):
            positive_fraction = float(np.mean(diffs >= 0))
            negative_fraction = float(np.mean(diffs <= 0))
            monotonic_fraction = max(positive_fraction, negative_fraction)
            if positive_fraction >= 0.9:
                direction = "increasing"
            elif negative_fraction >= 0.9:
                direction = "decreasing"

    z_scores = _zscore(working[target_column])
    anomaly_mask = z_scores.abs() > 3.5

    return {
        "date_column": date_column,
        "target_column": target_column,
        "points": int(len(working)),
        "rolling_window": rolling_window,
        "rolling_mean_slope": round(slope, 4),
        "trend_direction": direction,
        "monotonic_fraction": round(monotonic_fraction, 4),
        "anomaly_indices": working.index[anomaly_mask.fillna(False)].tolist()[:10],
    }


def _choose_time_series_target(numeric_columns: list[str], schema: dict[str, dict[str, object]]) -> str:
    for column in numeric_columns:
        if column.lower() == "close":
            return column
    for column in numeric_columns:
        if not column.lower().endswith("id") and not schema.get(column, {}).get("likely_id", False):
            return column
    return numeric_columns[0]


def _top_segments(
    df: pd.DataFrame,
    categorical_columns: list[str],
    numeric_columns: list[str],
    top_n: int,
) -> dict[str, dict[str, list[dict[str, object]]]]:
    segments = {}
    for cat_col in categorical_columns:
        if df[cat_col].nunique(dropna=True) > 20:
            continue
        segments[cat_col] = {}
        for num_col in numeric_columns:
            grouped = (
                df.groupby(cat_col, dropna=False)[num_col]
                .agg(["count", "mean", "sum"])
                .sort_values("sum", ascending=False)
                .head(top_n)
                .round(4)
                .reset_index()
            )
            segments[cat_col][num_col] = grouped.to_dict(orient="records")
    return segments


def _zscore(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if stats is not None:
        values = stats.zscore(numeric, nan_policy="omit")
        return pd.Series(values, index=series.index)

    std = numeric.std(ddof=0)
    if std == 0 or pd.isna(std):
        return pd.Series([0.0] * len(series), index=series.index)
    return (numeric - numeric.mean()) / std
