from __future__ import annotations

from pipeline.common.config import PipelineConfig
from pipeline.common.contracts import PipelineContext


def generate_insights(context: PipelineContext, config: PipelineConfig) -> None:
    insights: list[dict[str, str]] = []
    recommendations: list[str] = []

    quality_score = context.quality_report.get("score", 100)
    if quality_score < 60:
        insights.append(
            _insight(
                severity="high",
                message=f"Data quality score is {quality_score}, which is below the MVP warning threshold.",
                recommended_action="Review missing values, duplicates, and outliers before sharing downstream conclusions.",
            )
        )

    for column, meta in context.quality_report.get("missing_values", {}).items():
        if meta.get("pct", 0) > 20:
            insights.append(
                _insight(
                    severity="medium",
                    message=f"{column} has {meta['pct']}% missing values.",
                    recommended_action=f"Audit upstream collection for {column} or choose a more explicit imputation rule.",
                )
            )

    correlations = context.analysis_results.get("correlations", {})
    for left, row in correlations.items():
        for right, value in row.items():
            if left < right and abs(value) > config.analysis.correlation_threshold:
                insights.append(
                    _insight(
                        severity="medium",
                        message=f"Strong correlation detected between {left} and {right} (r={value}).",
                        recommended_action=f"Check whether {left} and {right} are duplicate signals or part of the same business driver.",
                    )
                )

    for column, meta in context.quality_report.get("outliers", {}).items():
        if meta.get("pct", 0) > 5:
            insights.append(
                _insight(
                    severity="medium",
                    message=f"{column} contains {meta['pct']}% outliers by IQR rules.",
                    recommended_action=f"Inspect extreme values in {column} for data entry issues or real edge events.",
                )
            )

    for column, meta in context.analysis_results.get("distributions", {}).items():
        if abs(meta.get("skew", 0)) > config.analysis.severe_skew_threshold:
            insights.append(
                _insight(
                    severity="low",
                    message=f"{column} shows a heavily skewed distribution (skew={meta['skew']}).",
                    recommended_action=f"Consider a log transform or robust summary metrics for {column}.",
                )
            )

    time_series = context.analysis_results.get("time_series", {})
    if time_series.get("monotonic_fraction", 0) >= 0.9 and time_series.get("trend_direction") in {"increasing", "decreasing"}:
        insights.append(
            _insight(
                severity="medium",
                message=(
                    f"{time_series['target_column']} shows a mostly {time_series['trend_direction']} rolling trend "
                    f"over time."
                ),
                recommended_action="Validate whether the trend is seasonal, structural, or driven by a short time window.",
            )
        )
    if time_series.get("anomaly_indices"):
        insights.append(
            _insight(
                severity="medium",
                message=f"Time-series anomaly spike detected in {time_series['target_column']}.",
                recommended_action="Inspect the flagged dates and compare them with known external events or incidents.",
            )
        )

    deduped_recommendations = []
    for item in insights:
        action = item["recommended_action"]
        if action not in deduped_recommendations:
            deduped_recommendations.append(action)

    if not insights:
        recommendations.append("Dataset looks healthy enough for exploratory analysis with the current MVP rules.")

    context.insights = {
        "items": insights,
        "recommendations": deduped_recommendations or recommendations,
    }


def _insight(*, severity: str, message: str, recommended_action: str) -> dict[str, str]:
    return {
        "severity": severity,
        "message": message,
        "recommended_action": recommended_action,
    }
