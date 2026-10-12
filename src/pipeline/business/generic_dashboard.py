from __future__ import annotations

import math
import re
from typing import Any, Optional

import numpy as np
import pandas as pd

from pipeline.business.common import (
    collect_insights,
    is_label_column_name,
    extract_prompt_terms,
    format_number,
    insight_card,
    metric_card,
    render_blueprint_html,
    score_insights,
    unfilled_frame,
)
from pipeline.common.contracts import PipelineContext


# The generic template makes no assumption about what the dataset is. It describes
# whatever columns are there: how values are distributed, how segments differ, how
# things move over time, which measures move together, and how clean the data is.

# Below these sizes a statistic says more about chance than about the data.
MIN_ROWS_FOR_TREND = 5
MIN_ROWS_FOR_CORRELATION = 8
MAX_CATEGORY_LEVELS = 20
MAX_CHART_POINTS = 60
MAX_CORRELATION_COLUMNS = 12

SECTION_CONFIG = {
    "overview": "KPI cards and key insights",
    "trends": "Charts: trends over time",
    "segments": "Charts: segments",
    "distributions": "Charts: distributions",
    "relationships": "Charts: relationships",
    "quality": "Data quality",
    "notes": "Insight notes",
}

FOCUS_KEYWORDS = {
    "trends": ["trend", "time", "growth", "season", "month", "year", "over time"],
    "segments": ["segment", "group", "category", "compare", "breakdown", "mix"],
    "distributions": ["distribution", "spread", "outlier", "skew", "range", "typical"],
    "relationships": ["relationship", "correlat", "driver", "depend", "predict"],
    "quality": ["quality", "missing", "duplicate", "clean", "complete"],
}

PROMPT_STOP_WORDS = {
    "about", "across", "after", "before", "build", "dashboard", "data", "focus", "from", "into", "look",
    "more", "need", "please", "show", "story", "that", "them", "these", "this", "those", "want", "with",
}

DATE_NAME = re.compile(r"date|time|timestamp|day|month|period|created|updated|when")

# Earlier keywords are better candidates for the dataset's headline measure.
MEASURE_KEYWORDS = [
    "revenue", "sales", "amount", "total", "profit", "price", "cost", "spend", "income", "salary", "wage",
    "value", "charge", "fare", "score", "rating", "quantity", "count", "volume", "rate",
]


def analyze_generic_context(context: PipelineContext) -> Optional[dict[str, Any]]:
    df = unfilled_frame(context)
    if df is None or df.empty or not len(df.columns):
        return None

    # A file with nothing to aggregate still gets a dashboard; it says so and reports data quality.
    roles = _column_roles(df)

    measure = _primary_measure(roles["numeric"])
    numeric_summary = _numeric_summary(df, roles["numeric"])
    segment_charts = _segment_means(df, roles["categorical"], measure)
    trend = _trend(df, roles["dates"], measure)
    correlations = _correlations(df, roles["numeric"])
    quality = _quality(context, df, measures={_short(column) for column in roles["numeric"]})

    signals = {
        "distributions": [_histogram(df[column], column) for column in roles["numeric"][:3]],
        "category_counts": [_category_counts(df[column], column) for column in roles["categorical"][:3]],
        "segment_means": segment_charts,
        "trend": trend,
        "correlations": correlations,
        "numeric_summary": numeric_summary,
        "missing": quality["missing"],
    }

    return {
        "kind": "generic",
        "dataset": {
            "row_count": int(len(df)),
            "column_count": int(len(df.columns)),
            "input_columns": [str(column) for column in df.columns],
            "numeric_columns": [str(column) for column in roles["numeric"]],
            "categorical_columns": [str(column) for column in roles["categorical"]],
            "date_columns": [str(column) for column in roles["dates"]],
        },
        "summary": {
            "row_count": int(len(df)),
            "column_count": int(len(df.columns)),
            "quality_score": quality["score"],
            "duplicate_pct": quality["duplicate_pct"],
            "measure_count": len(roles["numeric"]),
            "grouping_count": len(roles["categorical"]),
            "primary_measure": None if measure is None else _short(measure),
            "primary": next((item for item in numeric_summary if measure is not None and item["column"] == _short(measure)), None),
            "top_segment_gap": _top_segment_gap(segment_charts),
            "top_concentration": _top_concentration(signals["category_counts"]),
            "top_correlation": correlations[0] if correlations else None,
            "trend": _trend_summary(trend),
            "most_skewed": _most_skewed(numeric_summary),
            "outlier_column": quality["outlier_column"],
            "missing_column": quality["missing"][0] if quality["missing"] else None,
        },
        "signals": signals,
    }


def build_generic_insight_candidates(analysis: dict[str, Any], user_prompt: str = "") -> dict[str, Any]:
    summary = analysis["summary"]
    focus_tags = extract_focus_tags(user_prompt)
    gap = summary["top_segment_gap"]
    concentration = summary["top_concentration"]
    correlation = summary["top_correlation"]
    trend = summary["trend"]
    skewed = summary["most_skewed"]
    outliers = summary["outlier_column"]
    missing = summary["missing_column"]
    primary = summary["primary"]

    insights = collect_insights([
        lambda: {
            "id": "segment_gap",
            "title": f"Average {gap['measure']} differs most across {gap['column']}",
            "category": "segments",
            "severity": "high",
            "summary": (
                f"Where {gap['column']} is {gap['high_label']}, {gap['measure']} averages {format_number(gap['high_value'])}, "
                f"versus {format_number(gap['low_value'])} where it is {gap['low_label']}."
            ),
            "detail": (
                f"That is the widest gap between groups in the dataset. Check that {gap['column']} groups are comparable "
                "before reading it as a performance difference."
            ),
            "metric_label": "Largest segment gap",
            "metric_value": f"{gap['ratio']:.1f}x" if gap["ratio"] is not None else format_number(gap["high_value"] - gap["low_value"]),
            "metric_sub": f"{gap['high_label']} vs {gap['low_label']} by {gap['column']}",
            "tags": ["segments"],
            "section": "segments",
            "priority": 96,
            "condition": gap["spread_pct"] >= 20,
        },
        lambda: {
            "id": "trend_shift",
            "title": f"{trend['subject']} {'rose' if trend['change_pct'] > 0 else 'fell'} {abs(trend['change_pct']):.0f}% over the period",
            "category": "trends",
            "severity": "high",
            "summary": (
                f"{trend['subject']} moved from {format_number(trend['first_value'])} in {trend['first_label']} "
                f"to {format_number(trend['last_value'])} in {trend['last_label']}."
            ),
            "detail": f"The peak was {format_number(trend['peak_value'])} in {trend['peak_label']}, across {trend['periods']} periods.",
            "metric_label": "Change over the period",
            "metric_value": f"{trend['change_pct']:+.0f}%",
            "metric_sub": f"{trend['first_label']} to {trend['last_label']}",
            "tags": ["trends"],
            "section": "trends",
            "priority": 94,
            "condition": trend["periods"] >= 3 and abs(trend["change_pct"]) >= 10,
        },
        lambda: {
            "id": "strong_relationship",
            "title": f"{correlation['a']} and {correlation['b']} move together",
            "category": "relationships",
            "severity": "medium",
            "summary": (
                f"{correlation['a']} and {correlation['b']} have a {'strong' if abs(correlation['r']) >= 0.8 else 'moderate'} "
                f"{'positive' if correlation['r'] > 0 else 'negative'} relationship (r = {correlation['r']:.2f})."
            ),
            "detail": "They may measure the same thing, or one may drive the other. Either way, do not treat them as independent signals.",
            "metric_label": "Strongest correlation",
            "metric_value": f"{correlation['r']:+.2f}",
            "metric_sub": f"{correlation['a']} vs {correlation['b']}",
            "tags": ["relationships"],
            "section": "relationships",
            "priority": 92 if abs(correlation["r"]) >= 0.8 else 88,
            "condition": abs(correlation["r"]) >= 0.5,
        },
        lambda: {
            "id": "concentration",
            "title": f"{concentration['label']} makes up {concentration['share']:.0f}% of {concentration['column']}",
            "category": "segments",
            "severity": "medium",
            "summary": f"One value accounts for {concentration['share']:.0f}% of the rows that have a {concentration['column']}, across {concentration['levels']} groups.",
            "detail": "Totals and averages for the whole dataset mostly describe this one group. Smaller groups deserve a separate look.",
            "metric_label": "Largest group share",
            "metric_value": f"{concentration['share']:.0f}%",
            "metric_sub": f"{concentration['label']} in {concentration['column']}",
            "tags": ["segments"],
            "section": "segments",
            "priority": 87,
            "condition": concentration["share"] >= 50 and concentration["levels"] >= 3,
        },
        lambda: {
            "id": "top_category",
            "title": f"{concentration['label']} is the most common {concentration['column']}",
            "category": "segments",
            "severity": "low",
            "summary": (
                f"{concentration['label']} accounts for {concentration['share']:.0f}% of the rows that have a "
                f"{concentration['column']}, the largest of {concentration['levels']} groups."
            ),
            "detail": "No single group dominates, so comparisons between groups rest on reasonably sized samples.",
            "metric_label": "Largest group share",
            "metric_value": f"{concentration['share']:.0f}%",
            "metric_sub": f"{concentration['label']} in {concentration['column']}",
            "tags": ["segments"],
            "section": "segments",
            "priority": 74,
            "condition": not (concentration["share"] >= 50 and concentration["levels"] >= 3),
        },
        lambda: {
            "id": "skewed_measure",
            "title": f"A few large values dominate {skewed['column']}",
            "category": "distributions",
            "severity": "medium",
            "summary": f"The typical {skewed['column']} is {format_number(skewed['median'])}, but the average is {format_number(skewed['mean'])}.",
            "detail": "The distribution has a long tail, so the average overstates a typical row. Prefer the median when summarizing it.",
            "metric_label": "Median vs mean",
            "metric_value": f"{format_number(skewed['median'])} vs {format_number(skewed['mean'])}",
            "metric_sub": skewed["column"],
            "tags": ["distributions"],
            "section": "distributions",
            "priority": 86,
            "condition": abs(skewed["skew"]) >= 2,
        },
        lambda: {
            "id": "outliers",
            "title": f"{outliers['pct']:.1f}% of {outliers['column']} values are outliers",
            "category": "distributions",
            "severity": "medium",
            "summary": f"{outliers['pct']:.1f}% of {outliers['column']} values sit far outside the usual range.",
            "detail": "Confirm whether they are data-entry errors or real extreme cases before they shape any conclusion.",
            "metric_label": "Outlier share",
            "metric_value": f"{outliers['pct']:.1f}%",
            "metric_sub": outliers["column"],
            "tags": ["distributions", "quality"],
            "section": "distributions",
            "priority": 85,
            "condition": outliers["pct"] >= 5,
        },
        lambda: {
            "id": "missing_data",
            "title": f"{missing['column']} is empty in {missing['pct']:.0f}% of rows",
            "category": "quality",
            "severity": "high" if missing["pct"] >= 30 else "medium",
            "summary": f"{missing['column']} has no value in {missing['pct']:.0f}% of rows.",
            "detail": "Anything computed from this column describes only the rows that have it. Check whether the gaps are random.",
            "metric_label": "Most incomplete column",
            "metric_value": f"{missing['pct']:.0f}%",
            "metric_sub": missing["column"],
            "tags": ["quality"],
            "section": "quality",
            "priority": 88 if missing["pct"] >= 30 else 80,
            "condition": missing["pct"] >= 10,
        },
        lambda: {
            "id": "duplicates",
            "title": f"{summary['duplicate_pct']:.1f}% of rows are exact duplicates",
            "category": "quality",
            "severity": "medium",
            "summary": f"{summary['duplicate_pct']:.1f}% of rows repeat another row exactly.",
            "detail": "Duplicates were removed before analysis. If they were legitimate repeat records, counts here are understated.",
            "metric_label": "Duplicate rows",
            "metric_value": f"{summary['duplicate_pct']:.1f}%",
            "metric_sub": "exact repeats in the upload",
            "tags": ["quality"],
            "section": "quality",
            "priority": 82,
            "condition": summary["duplicate_pct"] >= 2,
        },
        lambda: {
            "id": "primary_measure",
            "title": f"A typical {primary['column']} is {format_number(primary['median'])}",
            "category": "overview",
            "severity": "low",
            "summary": (
                f"{primary['column']} ranges from {format_number(primary['min'])} to {format_number(primary['max'])}, "
                f"with a median of {format_number(primary['median'])}."
            ),
            "detail": f"The average is {format_number(primary['mean'])} across {summary['row_count']:,} rows.",
            "metric_label": f"Median {primary['column']}",
            "metric_value": format_number(primary["median"]),
            "metric_sub": f"average {format_number(primary['mean'])}",
            "tags": ["distributions"],
            "section": "distributions",
            "priority": 72,
        },
        lambda: {
            "id": "nothing_to_aggregate",
            "title": "No measures or groupings found in this file",
            "category": "overview",
            "severity": "medium",
            "summary": "Every column looks like an identifier, free text, or a single repeated value, so there is nothing to total, average, or compare.",
            "detail": "If the file has amounts or categories, check that they are not stored as text with symbols, or spread across too few rows.",
            "metric_label": "Usable columns",
            "metric_value": "0",
            "metric_sub": f"of {summary['column_count']} columns",
            "tags": ["quality"],
            "section": "quality",
            "priority": 90,
            "condition": summary["measure_count"] == 0 and summary["grouping_count"] == 0,
        },
        lambda: {
            "id": "dataset_shape",
            "title": f"{summary['row_count']:,} rows across {summary['column_count']} columns",
            "category": "overview",
            "severity": "low",
            "summary": f"The dataset has {summary['row_count']:,} distinct rows and {summary['column_count']} columns.",
            "detail": f"Data quality score: {format_number(summary['quality_score'])} out of 100.",
            "metric_label": "Rows analyzed",
            "metric_value": f"{summary['row_count']:,}",
            "metric_sub": f"{summary['column_count']} columns",
            "tags": ["quality"],
            "section": "quality",
            "priority": 70,
        },
    ])

    prompt_terms = extract_prompt_terms(user_prompt, PROMPT_STOP_WORDS)
    return {"insights": score_insights(insights, focus_tags, prompt_terms, minimum_recommended=3), "focus_tags": focus_tags}


def build_business_dashboard(
    context: Optional[PipelineContext] = None,
    analysis: Optional[dict[str, Any]] = None,
    approved_insight_ids: Optional[list[str]] = None,
    user_prompt: str = "",
    settings: Optional[dict[str, Any]] = None,
) -> Optional[dict[str, Any]]:
    if analysis is None:
        if context is None:
            return None
        analysis = analyze_generic_context(context)
    if analysis is None:
        return None

    insight_bundle = build_generic_insight_candidates(analysis, user_prompt)
    insights = insight_bundle["insights"]
    approved_set = set(approved_insight_ids or [item["id"] for item in insights if item.get("recommended", True)])
    approved = [item for item in insights if item["id"] in approved_set] or insights[:4]

    settings = settings or {}
    available = dashboard_section_options(analysis)
    included_sections = settings.get("included_sections") or list(available)
    title = settings.get("title") or "Dataset Overview"
    subtitle = settings.get("subtitle") or "What stands out in this dataset"
    metric_count = int(settings.get("metric_count", 4))
    show_notes = bool(settings.get("show_notes", True))

    payload = {
        "kind": "generic",
        "title": title,
        "subtitle": subtitle,
        "headline": {"title": title, "subtitle": subtitle},
        "approved_insights": approved,
        "metric_cards": approved[:metric_count],
        "sections": included_sections,
        "layout_sections": _build_layout_sections(approved, approved[:metric_count], analysis, included_sections, show_notes),
        "all_layout_sections": _build_layout_sections(approved, approved, analysis, list(available), True),
        "signals": analysis["signals"],
        "summary": analysis["summary"],
        "dataset": analysis["dataset"],
        "show_notes": show_notes,
        "focus_tags": insight_bundle["focus_tags"],
    }
    return {
        "kind": "generic",
        "title": title,
        "html": _render_dashboard_html(payload),
        "height": 1120 + (150 * len(included_sections)),
        "payload": payload,
        "blueprint": payload,
        "download_name": "dataset_overview_dashboard.html",
    }


def dashboard_section_options(analysis: Optional[dict[str, Any]] = None) -> dict[str, str]:
    """Sections this dataset can fill. Without an analysis, every section the template knows."""
    if not analysis:
        return SECTION_CONFIG
    signals = analysis["signals"]
    has_data = {
        "overview": True,
        "trends": bool(signals["trend"]),
        "segments": bool(signals["segment_means"] or signals["category_counts"]),
        "distributions": bool(signals["distributions"]),
        "relationships": bool(signals["correlations"]),
        "quality": True,
        "notes": True,
    }
    return {section: label for section, label in SECTION_CONFIG.items() if has_data[section]}


def extract_focus_tags(prompt: str) -> list[str]:
    lowered = prompt.strip().lower()
    if not lowered:
        return []
    return [tag for tag, keywords in FOCUS_KEYWORDS.items() if any(keyword in lowered for keyword in keywords)]


def _column_roles(df: pd.DataFrame) -> dict[str, list[Any]]:
    """Sort columns into measures, grouping columns and dates; identifiers and free text are left out."""
    numeric: list[Any] = []
    categorical: list[Any] = []
    dates: list[Any] = []
    rows = len(df)
    for column in df.columns:
        series = df[column]
        distinct = int(series.nunique(dropna=True))
        if distinct == 0:
            continue
        if pd.api.types.is_datetime64_any_dtype(series):
            dates.append(column)
        elif distinct == 1:
            continue
        elif pd.api.types.is_bool_dtype(series) or (pd.api.types.is_numeric_dtype(series) and distinct == 2):
            categorical.append(column)
        elif pd.api.types.is_numeric_dtype(series):
            if not _looks_like_identifier(str(column), series, distinct, rows):
                numeric.append(column)
        elif _parses_as_dates(str(column), series):
            dates.append(column)
        elif 2 <= distinct <= MAX_CATEGORY_LEVELS and distinct <= max(2, rows // 2):
            categorical.append(column)

    # Lead with the columns most likely to matter: named measures first, then compact groupings.
    numeric.sort(key=lambda column: _measure_rank(str(column)))
    categorical.sort(key=lambda column: abs(int(df[column].nunique(dropna=True)) - 5))
    return {"numeric": numeric, "categorical": categorical, "dates": dates}


def _parses_as_dates(name: str, series: pd.Series) -> bool:
    """Text columns named like dates whose values parse as dates, e.g. "Jan 1 2000"."""
    if not DATE_NAME.search(name.lower()) or pd.api.types.is_numeric_dtype(series):
        return False
    sample = series.dropna().astype(str).head(200)
    if sample.empty or sample.str.fullmatch(r"[\d.]+").all():
        return False
    return bool(pd.to_datetime(sample, errors="coerce").notna().mean() >= 0.9)


def _year_column(df: pd.DataFrame) -> Optional[Any]:
    """A whole-number column called year, usable as a time axis when there are no dates."""
    for column in df.columns:
        if str(column).strip().lower() not in {"year", "yr", "fiscal_year", "fiscal year"}:
            continue
        values = pd.to_numeric(df[column], errors="coerce").dropna()
        if not values.empty and (values % 1 == 0).all() and values.between(1800, 2200).all() and values.nunique() >= 3:
            return column
    return None


def _is_non_measure_name(name: str) -> bool:
    return is_label_column_name(name)


def _looks_like_identifier(name: str, series: pd.Series, distinct: int, rows: int) -> bool:
    if _is_non_measure_name(name):
        return True
    values = series.dropna()
    whole_numbers = bool((values % 1 == 0).all())
    return whole_numbers and distinct == rows and bool(values.is_monotonic_increasing)


def _measure_rank(name: str) -> int:
    lowered = name.lower()
    return next((index for index, keyword in enumerate(MEASURE_KEYWORDS) if keyword in lowered), len(MEASURE_KEYWORDS))


def _primary_measure(numeric: list[Any]) -> Optional[Any]:
    return numeric[0] if numeric else None


def _numeric_summary(df: pd.DataFrame, numeric: list[Any]) -> list[dict[str, Any]]:
    summary = []
    for column in numeric[:8]:
        values = pd.to_numeric(df[column], errors="coerce").dropna()
        if values.empty:
            continue
        summary.append(
            {
                "column": _short(column),
                "mean": _num(values.mean()),
                "median": _num(values.median()),
                "min": _num(values.min()),
                "max": _num(values.max()),
                "skew": _num(values.skew()) if len(values) >= 3 else None,
            }
        )
    return summary


def _histogram(series: pd.Series, column: Any) -> dict[str, Any]:
    values = pd.to_numeric(series, errors="coerce").dropna()
    values = values[np.isfinite(values)]
    bins = int(min(10, max(1, values.nunique())))
    counts, edges = np.histogram(values, bins=bins)
    labels = [f"{format_number(float(edges[index]))} to {format_number(float(edges[index + 1]))}" for index in range(len(counts))]
    return {"column": _short(column), "labels": labels, "values": [int(count) for count in counts]}


def _category_counts(series: pd.Series, column: Any) -> dict[str, Any]:
    counts = series.dropna().astype(str).value_counts().head(8)
    return {
        "column": _short(column),
        "labels": [_short(label, 40) for label in counts.index.tolist()],
        "values": [int(value) for value in counts.tolist()],
        "levels": int(series.nunique(dropna=True)),
        "answered": int(series.notna().sum()),
    }


def _segment_means(df: pd.DataFrame, categorical: list[Any], measure: Optional[Any]) -> list[dict[str, Any]]:
    """Average of the headline measure per group, for the groupings where the groups differ most."""
    if measure is None:
        return []
    values = pd.to_numeric(df[measure], errors="coerce")
    overall = values.mean()
    if pd.isna(overall) or overall == 0:
        return []
    minimum_group = max(3, len(df) // 100)

    charts = []
    for column in categorical:
        labeled = df[column].notna()
        grouped = values[labeled].groupby(df.loc[labeled, column].astype(str)).agg(["mean", "count"])
        grouped = grouped[grouped["count"] >= minimum_group].dropna(subset=["mean"])
        if len(grouped) < 2:
            continue
        grouped = grouped.sort_values("count", ascending=False).head(8).sort_values("mean", ascending=False)
        high, low = float(grouped["mean"].iloc[0]), float(grouped["mean"].iloc[-1])
        charts.append(
            {
                "column": _short(column),
                "measure": _short(measure),
                "labels": [_short(label, 40) for label in grouped.index.tolist()],
                "values": [_num(value) for value in grouped["mean"].tolist()],
                "spread_pct": _num((high - low) / abs(float(overall)) * 100),
            }
        )
    charts.sort(key=lambda chart: -(chart["spread_pct"] or 0))
    return charts[:3]


def _top_segment_gap(segment_charts: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not segment_charts:
        return None
    chart = segment_charts[0]
    high, low = chart["values"][0], chart["values"][-1]
    if high is None or low is None:
        return None
    return {
        "column": chart["column"],
        "measure": chart["measure"],
        "high_label": chart["labels"][0],
        "high_value": high,
        "low_label": chart["labels"][-1],
        "low_value": low,
        "ratio": _num(high / low) if low > 0 else None,
        "spread_pct": chart["spread_pct"],
    }


def _top_concentration(category_counts: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    best = None
    for chart in category_counts:
        if not chart["values"] or not chart["answered"]:
            continue
        # Share of the rows that have a value; blanks are reported separately under data quality.
        share = chart["values"][0] / chart["answered"] * 100
        if best is None or share > best["share"]:
            best = {"column": chart["column"], "label": chart["labels"][0], "share": _num(share), "levels": chart["levels"]}
    return best


def _trend(df: pd.DataFrame, dates: list[Any], measure: Optional[Any]) -> Optional[dict[str, Any]]:
    year_column = None if dates else _year_column(df)
    if dates:
        date_column = max(dates, key=lambda column: df[column].nunique(dropna=True))
        stamps = pd.to_datetime(df[date_column], errors="coerce")
        valid = stamps.notna()
        if valid.sum() < MIN_ROWS_FOR_TREND or (stamps[valid].max() - stamps[valid].min()).days < 1:
            return None
        for period_name, frequency in (("day", "D"), ("week", "W"), ("month", "M"), ("quarter", "Q"), ("year", "Y")):
            periods = stamps[valid].dt.to_period(frequency)
            if periods.nunique() <= MAX_CHART_POINTS:
                break
        label = lambda period: str(period.start_time.date()) if frequency == "W" else str(period)
    elif year_column is not None:
        date_column, period_name = year_column, "year"
        years = pd.to_numeric(df[year_column], errors="coerce")
        valid = years.notna()
        periods = years[valid].astype(int)
        label = str
    else:
        return None
    if not 2 <= periods.nunique() <= MAX_CHART_POINTS * 4:
        return None

    counts = periods.value_counts().sort_index()
    if measure is not None and measure != year_column:
        counts = _trim_sparse_ends(counts)
    trend = {
        "date_column": _short(date_column),
        "period": period_name,
        "labels": [label(period) for period in counts.index],
        "counts": [int(value) for value in counts.tolist()],
        "measure": None,
        "values": [],
    }
    if measure is not None and measure != year_column:
        means = pd.to_numeric(df.loc[valid, measure], errors="coerce").groupby(periods).mean().reindex(counts.index)
        trend["measure"] = _short(measure)
        trend["values"] = [_num(value) for value in means.tolist()]
    return trend


def _trim_sparse_ends(counts: pd.Series) -> pd.Series:
    """Drop leading and trailing periods holding only a few rows.

    Exports often start or stop mid-period, and the average of one stray row would
    draw a spike at the edge of the chart.
    """
    enough_rows = max(3, float(counts.median()) * 0.25)
    populated = np.flatnonzero(counts.to_numpy() >= enough_rows)
    if len(populated) < 2:
        return counts
    return counts.iloc[populated[0] : populated[-1] + 1]


def _trend_summary(trend: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if trend is None:
        return None
    by_measure = trend["measure"] is not None
    series = trend["values"] if by_measure else trend["counts"]
    # A period holding a handful of rows gives a noisy average; compare only well-populated periods.
    enough_rows = max(3, float(np.median(trend["counts"])) * 0.25) if by_measure else 0
    points = [
        (label, value)
        for label, value, count in zip(trend["labels"], series, trend["counts"])
        if value is not None and count >= enough_rows
    ]
    if len(points) < 2 or not points[0][1]:
        return None
    (first_label, first_value), (last_label, last_value) = points[0], points[-1]
    peak_label, peak_value = max(points, key=lambda point: point[1])
    return {
        "subject": f"Average {trend['measure']}" if by_measure else "Record volume",
        "first_label": first_label,
        "first_value": first_value,
        "last_label": last_label,
        "last_value": last_value,
        "peak_label": peak_label,
        "peak_value": peak_value,
        "change_pct": _num((last_value - first_value) / abs(first_value) * 100),
        "periods": len(points),
    }


def _correlations(df: pd.DataFrame, numeric: list[Any]) -> list[dict[str, Any]]:
    columns = numeric[:MAX_CORRELATION_COLUMNS]
    if len(columns) < 2:
        return []
    matrix = df[columns].apply(pd.to_numeric, errors="coerce").corr(method="pearson", min_periods=MIN_ROWS_FOR_CORRELATION)
    pairs = []
    for left_index, left in enumerate(columns):
        for right in columns[left_index + 1 :]:
            value = _num(matrix.loc[left, right])
            if value is not None:
                pairs.append({"a": _short(left, 32), "b": _short(right, 32), "r": value})
    pairs.sort(key=lambda pair: -abs(pair["r"]))
    return pairs[:6]


def _most_skewed(numeric_summary: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    candidates = [item for item in numeric_summary if item["skew"] is not None]
    return max(candidates, key=lambda item: abs(item["skew"])) if candidates else None


def _quality(context: PipelineContext, df: pd.DataFrame, measures: set[str]) -> dict[str, Any]:
    """Quality facts from the pipeline's validation of the raw upload, or from the frame if that did not run."""
    report = context.quality_report or {}
    if report.get("missing_values"):
        missing = [(_short(column), float(meta.get("pct", 0))) for column, meta in report["missing_values"].items()]
    else:
        missing = [(_short(column), float(df[column].isna().mean() * 100)) for column in df.columns]
    missing = sorted((item for item in missing if item[1] > 0), key=lambda item: -item[1])[:6]

    outliers = [(_short(column), float(meta.get("pct", 0))) for column, meta in (report.get("outliers") or {}).items()]
    outliers = [item for item in outliers if item[0] in measures]
    top_outlier = max(outliers, key=lambda item: item[1]) if outliers else None

    return {
        "score": _num(report.get("score", 100.0)),
        "duplicate_pct": _num((report.get("duplicates") or {}).get("pct", 0.0)) or 0.0,
        "missing": [{"column": column, "pct": _num(pct)} for column, pct in missing],
        "outlier_column": None if top_outlier is None else {"column": top_outlier[0], "pct": _num(top_outlier[1])},
    }


def _short(value: Any, limit: int = 48) -> str:
    """Column names and labels as shown in titles and axes; survey files use whole questions as headers."""
    text = " ".join(str(value).split())
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "\u2026"


def _num(value: Any) -> Optional[float]:
    """A plain rounded float, or None for NaN and infinity, which JSON cannot carry."""
    if value is None:
        return None
    number = float(value)
    return round(number, 4) if math.isfinite(number) else None


def _chart(
    chart_id: str,
    title: str,
    subtitle: str,
    labels: list[str],
    values: list[Any],
    *,
    kind: str = "bar",
    color: str = "#c2410c",
    name: str = "Value",
    number_format: str = "number",
) -> dict[str, Any]:
    return {
        "id": chart_id,
        "kind": "chart",
        "chart": {
            "id": chart_id,
            "title": title,
            "subtitle": subtitle,
            "type": kind,
            "labels": labels,
            "series": [{"name": name, "values": values, "color": color}],
            "format": number_format,
        },
    }


def _build_layout_sections(
    approved_insights: list[dict[str, Any]],
    metric_cards: list[dict[str, Any]],
    analysis: dict[str, Any],
    included_sections: list[str],
    show_notes: bool,
) -> list[dict[str, Any]]:
    signals = analysis["signals"]
    summary = analysis["summary"]
    dataset = analysis["dataset"]
    trend = signals["trend"]
    section_map: dict[str, dict[str, Any]] = {
        "overview": {
            "id": "overview",
            "title": "Dataset Overview",
            "description": "The fastest read on what stands out in this dataset.",
            "blocks": [
                {"id": "overview-metrics", "kind": "metric_grid", "cards": [metric_card(card) for card in metric_cards]},
                {"id": "overview-insights", "kind": "insight_grid", "insights": [insight_card(item) for item in approved_insights[:4]]},
            ],
        },
        "quality": {
            "id": "quality",
            "title": "Data Quality",
            "description": "How complete and consistent the upload is. Read the rest of the dashboard with this in mind.",
            "blocks": [
                {
                    "id": "quality-stats",
                    "kind": "stat_list",
                    "title": "At a glance",
                    "items": [
                        {"label": "Quality score", "value": format_number(summary["quality_score"])},
                        {"label": "Rows analyzed", "value": f"{dataset['row_count']:,}"},
                        {"label": "Columns", "value": str(dataset["column_count"])},
                        {"label": "Duplicate rows", "value": f"{summary['duplicate_pct']:.1f}%"},
                        {"label": "Numeric columns", "value": str(len(dataset["numeric_columns"]))},
                        {"label": "Grouping columns", "value": str(len(dataset["categorical_columns"]))},
                    ],
                },
                *(
                    [
                        _chart(
                            "quality-missing",
                            "Missing values by column",
                            "Share of rows with no value, for the least complete columns.",
                            [item["column"] for item in signals["missing"]],
                            [item["pct"] for item in signals["missing"]],
                            color="#57534e",
                            name="Missing",
                            number_format="percent",
                        )
                    ]
                    if signals["missing"]
                    else []
                ),
            ],
        },
    }

    if trend:
        blocks = []
        if trend["measure"]:
            blocks.append(
                _chart(
                    "trend-measure",
                    f"Average {trend['measure']} by {trend['period']}",
                    f"Grouped on {trend['date_column']}. Periods at either end with very few rows are left out.",
                    trend["labels"],
                    trend["values"],
                    kind="line",
                    name=f"Average {trend['measure']}",
                )
            )
        blocks.append(
            _chart(
                "trend-volume",
                f"Rows per {trend['period']}",
                "How much data each period contributes; thin periods make averages jumpy.",
                trend["labels"],
                trend["counts"],
                color="#292524",
                name="Rows",
            )
        )
        section_map["trends"] = {
            "id": "trends",
            "title": "Trends Over Time",
            "description": "Movement across the dataset's date range.",
            "blocks": blocks,
        }

    segment_blocks = [
        _chart(
            f"segment-mean-{index}",
            f"Average {chart['measure']} by {chart['column']}",
            "Groups with very few rows are left out.",
            chart["labels"],
            chart["values"],
            name=f"Average {chart['measure']}",
        )
        for index, chart in enumerate(signals["segment_means"])
    ] + [
        _chart(
            f"segment-count-{index}",
            f"Rows by {chart['column']}",
            "The largest groups in this column.",
            chart["labels"],
            chart["values"],
            color="#166534",
            name="Rows",
        )
        for index, chart in enumerate(signals["category_counts"][:2])
    ]
    if segment_blocks:
        section_map["segments"] = {
            "id": "segments",
            "title": "Segments",
            "description": "How groups in the data compare with each other.",
            "blocks": segment_blocks,
        }

    if signals["distributions"]:
        section_map["distributions"] = {
            "id": "distributions",
            "title": "Distributions",
            "description": "Where values cluster and how far the extremes reach.",
            "blocks": [
                _chart(
                    f"distribution-{index}",
                    f"Distribution of {chart['column']}",
                    "Number of rows in each value range.",
                    chart["labels"],
                    chart["values"],
                    color="#b45309",
                    name="Rows",
                )
                for index, chart in enumerate(signals["distributions"])
            ]
            + [
                {
                    "id": "distribution-stats",
                    "kind": "stat_list",
                    "title": "Typical values (median)",
                    "items": [
                        {"label": item["column"], "value": format_number(item["median"])} for item in signals["numeric_summary"][:6]
                    ],
                }
            ],
        }

    if signals["correlations"]:
        section_map["relationships"] = {
            "id": "relationships",
            "title": "Relationships",
            "description": "Pairs of measures that rise and fall together. Correlation is not causation.",
            "blocks": [
                _chart(
                    "relationship-correlations",
                    "Strongest correlations",
                    "Correlation coefficient from -1 to 1; values near zero mean no linear relationship.",
                    [f"{pair['a']} vs {pair['b']}" for pair in signals["correlations"]],
                    [pair["r"] for pair in signals["correlations"]],
                    color="#0f766e",
                    name="Correlation",
                )
            ],
        }

    if show_notes:
        section_map["notes"] = {
            "id": "notes",
            "title": "Approved Insight Notes",
            "description": "Narrative notes kept alongside the dashboard for review and export.",
            "blocks": [{"id": "notes-list", "kind": "note_list", "insights": [insight_card(item) for item in approved_insights]}],
        }
    return [section_map[section] for section in included_sections if section in section_map]


def _render_dashboard_html(payload: dict[str, Any]) -> str:
    return render_blueprint_html(payload, eyebrow="Loom dataset overview")
