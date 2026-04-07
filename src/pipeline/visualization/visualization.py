from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Optional, Tuple

os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "matplotlib-cache"))

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from pipeline.common.config import PipelineConfig
from pipeline.common.contracts import PipelineContext, PipelineExecutionError

try:
    import missingno as msno
except ImportError:  # pragma: no cover - optional fallback
    msno = None


def generate_visualizations(context: PipelineContext, config: PipelineConfig) -> None:
    df = context.clean_df
    if df is None:
        raise PipelineExecutionError(stage="visualization", message="Visualization requires a cleaned dataframe.")
    if context.output_dir is None:
        raise PipelineExecutionError(stage="visualization", message="Visualization requires an output directory.")
    if df.empty:
        context.add_warning("Visualization skipped because the cleaned dataframe is empty.")
        return

    charts_dir = context.output_dir / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)
    source_name = context.input_path.name if context.input_path else "input.csv"
    max_charts = config.visualization.max_charts
    generated: list[str] = []

    numeric_columns = df.select_dtypes(include=["number"]).columns.tolist()
    date_columns = [column for column in df.columns if pd.api.types.is_datetime64_any_dtype(df[column])]
    categorical_columns = [
        column
        for column in df.columns
        if column not in numeric_columns and column not in date_columns
    ]

    if any(item["pct"] > 5 for item in context.quality_report.get("missing_values", {}).values()):
        path = _missing_heatmap(df, charts_dir, source_name)
        if path:
            generated.append(path)

    if len(numeric_columns) >= 3 and len(generated) < max_charts:
        generated.append(_correlation_heatmap(df, numeric_columns, charts_dir, source_name))

    correlations = context.analysis_results.get("correlations", {})
    if correlations and len(generated) < max_charts:
        pair = _first_correlated_pair(correlations, threshold=config.analysis.scatter_threshold)
        if pair:
            generated.append(_scatter_plot(df, pair[0], pair[1], charts_dir, source_name))

    if date_columns and numeric_columns and len(generated) < max_charts:
        target_column = context.analysis_results.get("time_series", {}).get("target_column", numeric_columns[0])
        generated.append(_line_chart(df, date_columns[0], target_column, charts_dir, source_name))

    for column in numeric_columns:
        if len(generated) >= max_charts:
            break
        skew_info = context.analysis_results.get("distributions", {}).get(column, {})
        if not skew_info.get("is_skewed", False):
            generated.append(_histogram(df, column, charts_dir, source_name))

    for column, meta in context.quality_report.get("outliers", {}).items():
        if len(generated) >= max_charts:
            break
        if meta.get("count", 0) > 0:
            generated.append(_box_plot(df, column, charts_dir, source_name))

    for column in categorical_columns:
        if len(generated) >= max_charts:
            break
        if df[column].nunique(dropna=True) < 20:
            generated.append(_bar_chart(df, column, charts_dir, source_name))

    context.charts = [chart for chart in generated if chart]


def _histogram(df: pd.DataFrame, column: str, charts_dir: Path, source_name: str) -> str:
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.histplot(df[column].dropna(), kde=True, ax=ax)
    ax.set_title(f"Distribution of {column}")
    ax.set_xlabel(column)
    ax.set_ylabel("Count")
    return _save_figure(fig, charts_dir / f"hist_{column}.png", source_name)


def _box_plot(df: pd.DataFrame, column: str, charts_dir: Path, source_name: str) -> str:
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.boxplot(y=df[column], ax=ax)
    ax.set_title(f"Outliers in {column}")
    ax.set_ylabel(column)
    return _save_figure(fig, charts_dir / f"box_{column}.png", source_name)


def _bar_chart(df: pd.DataFrame, column: str, charts_dir: Path, source_name: str) -> str:
    counts = df[column].fillna("missing").value_counts().head(10)
    fig, ax = plt.subplots(figsize=(9, 5))
    sns.barplot(x=counts.index.astype(str), y=counts.values, ax=ax)
    ax.set_title(f"Top categories for {column}")
    ax.set_xlabel(column)
    ax.set_ylabel("Count")
    ax.tick_params(axis="x", rotation=30)
    return _save_figure(fig, charts_dir / f"bar_{column}.png", source_name)


def _line_chart(
    df: pd.DataFrame,
    date_column: str,
    numeric_column: str,
    charts_dir: Path,
    source_name: str,
) -> str:
    plot_df = df[[date_column, numeric_column]].dropna().sort_values(date_column)
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(plot_df[date_column], plot_df[numeric_column], linewidth=2)
    ax.set_title(f"{numeric_column} over time")
    ax.set_xlabel(date_column)
    ax.set_ylabel(numeric_column)
    fig.autofmt_xdate()
    return _save_figure(fig, charts_dir / f"line_{date_column}_{numeric_column}.png", source_name)


def _correlation_heatmap(
    df: pd.DataFrame,
    numeric_columns: list[str],
    charts_dir: Path,
    source_name: str,
) -> str:
    fig, ax = plt.subplots(figsize=(7, 6))
    sns.heatmap(df[numeric_columns].corr(), annot=True, cmap="Blues", ax=ax)
    ax.set_title("Correlation heatmap")
    return _save_figure(fig, charts_dir / "correlation_heatmap.png", source_name)


def _scatter_plot(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    charts_dir: Path,
    source_name: str,
) -> str:
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.scatterplot(data=df, x=x_col, y=y_col, ax=ax)
    ax.set_title(f"{x_col} vs {y_col}")
    return _save_figure(fig, charts_dir / f"scatter_{x_col}_{y_col}.png", source_name)


def _missing_heatmap(df: pd.DataFrame, charts_dir: Path, source_name: str) -> str:
    fig = plt.figure(figsize=(9, 5))
    if msno is not None:
        msno.matrix(df, sparkline=False)
    else:
        ax = fig.add_subplot(111)
        sns.heatmap(df.isna(), cbar=False, ax=ax)
        ax.set_title("Missing value heatmap")
    return _save_figure(fig, charts_dir / "missing_values.png", source_name)


def _first_correlated_pair(
    correlations: dict[str, dict[str, float]], threshold: float
) -> Optional[Tuple[str, str]]:
    for left, row in correlations.items():
        for right, value in row.items():
            if left != right and abs(value) > threshold:
                return left, right
    return None


def _save_figure(fig: plt.Figure, path: Path, source_name: str) -> str:
    fig.text(0.99, 0.01, f"Source: {source_name}", ha="right", va="bottom", fontsize=8, color="#555555")
    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return str(path)
