from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from pipeline.business.common import (
    collect_insights,
    extract_prompt_terms,
    format_number,
    insight_card,
    is_identifier_column_name,
    metric_card,
    prune_empty_blocks,
    score_insights,
)
from pipeline.business.matching import normalize_name


# The other shape that recurs across domains: a table of performance numbers (clicks
# and spend per campaign, visits and bounce rate per day) with columns to slice them
# by. A template names its metrics; this module compares each across every grouping
# column and over time.

MIN_ROWS = 20
MAX_LEVELS = 12
MAX_PERIODS = 60

SECTION_CONFIG = {
    "overview": "KPI cards and key insights",
    "segments": "Charts: metrics by group",
    "trends": "Charts: metrics over time",
    "notes": "Insight notes",
}

FOCUS_KEYWORDS = {
    "segments": ["segment", "group", "channel", "compare", "breakdown", "which", "by "],
    "trends": ["trend", "time", "growth", "month", "week", "over time"],
}

PROMPT_STOP_WORDS = {
    "about", "across", "after", "before", "build", "dashboard", "data", "focus", "from", "into", "look",
    "more", "need", "please", "show", "story", "that", "them", "these", "this", "those", "want", "with",
}

CHART_COLORS = ["#c2410c", "#0f766e", "#b45309", "#292524", "#166534"]


def to_number(series: pd.Series) -> pd.Series:
    """Numbers as exports write them: "$16,174.00", "2,146", "4.5%"."""
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")
    return pd.to_numeric(series.astype(str).str.replace(r"[^0-9.\-]", "", regex=True), errors="coerce")


def find_columns(df: pd.DataFrame, aliases: dict[str, list[str]]) -> dict[str, Any]:
    """Map field names to the dataset's numeric columns, by alias. Fields with no usable column are left out."""
    names = {normalize_name(column): column for column in df.columns}
    found = {}
    for field, options in aliases.items():
        column = next((names[key] for key in map(normalize_name, options) if key in names), None)
        if column is not None and to_number(df[column]).notna().mean() >= 0.9:
            found[field] = column
    return found


def analyze_metrics(
    df: pd.DataFrame,
    *,
    kind: str,
    profile: str,
    metrics: list[dict[str, Any]],
    unit: str,
) -> Optional[dict[str, Any]]:
    """Compare each metric across the dataset's grouping columns and over time.

    A metric is ``{"name", "column"}`` (averaged per row) or ``{"name", "numerator",
    "denominator"}`` (summed, then divided, so big rows count for more), with an
    optional ``"format"`` and ``"scale"`` (100 turns a ratio into a percentage).
    ``unit`` is what a row is ("campaigns", "days").
    """
    if len(df) < MIN_ROWS or not metrics:
        return None

    used = {metric.get(key) for metric in metrics for key in ("column", "numerator", "denominator")}
    values = {column: to_number(df[column]) for column in used if column is not None}

    def measure(metric: dict[str, Any], rows: pd.Series) -> Optional[float]:
        if "column" in metric:
            result = values[metric["column"]][rows].mean()
        else:
            denominator = values[metric["denominator"]][rows].sum()
            result = values[metric["numerator"]][rows].sum() / denominator if denominator else float("nan")
        result = result * metric.get("scale", 1)
        return None if pd.isna(result) else round(float(result), 4)

    everything = pd.Series(True, index=df.index)
    overall = {metric["name"]: measure(metric, everything) for metric in metrics}
    metrics = [metric for metric in metrics if overall[metric["name"]] is not None]
    if not metrics:
        return None

    minimum_group = min(max(10, len(df) // 100), 50)
    dimensions = [
        column
        for column in df.columns
        if column not in values and not is_identifier_column_name(column) and 2 <= df[column].nunique(dropna=True) <= MAX_LEVELS
    ]
    group_labels = {column: df[column].astype(str).where(df[column].notna()) for column in dimensions}
    tables = []
    for metric in metrics:
        best: Optional[dict[str, Any]] = None
        for column in dimensions:
            sizes = group_labels[column].value_counts()
            rows = []
            for label in sizes[sizes >= minimum_group].index:
                value = measure(metric, group_labels[column] == label)
                if value is not None:
                    rows.append((label, value))
            if len(rows) < 2:
                continue
            rows.sort(key=lambda row: -row[1])
            reference = abs(overall[metric["name"]]) or 1.0
            table = {
                "metric": metric["name"],
                "format": metric.get("format", "number"),
                "column": str(column),
                "labels": [label for label, _ in rows[:8]],
                "values": [value for _, value in rows[:8]],
                "spread": round((rows[0][1] - rows[-1][1]) / reference * 100, 2),
                "high_label": rows[0][0],
                "high_value": rows[0][1],
                "low_label": rows[-1][0],
                "low_value": rows[-1][1],
            }
            if best is None or table["spread"] > best["spread"]:
                best = table
        if best is not None:
            tables.append(best)
    tables.sort(key=lambda table: -table["spread"])

    trends = _trends(df, metrics, measure)
    if not tables and not trends:
        return None

    return {
        "kind": kind,
        "profile": profile,
        "dataset": {
            "row_count": int(len(df)),
            "column_count": int(len(df.columns)),
            "input_columns": [str(column) for column in df.columns],
            "shape": "metrics",
            "unit": unit,
        },
        "summary": {
            "row_count": int(len(df)),
            "metrics": [
                {"name": metric["name"], "value": overall[metric["name"]], "format": metric.get("format", "number")}
                for metric in metrics
            ],
            "gaps": tables[:3],
            "trend": trends[0]["change"] if trends else None,
        },
        "signals": {"metric_by_dimension": tables[:5], "metric_trends": trends[:3]},
    }


def build_metric_insight_candidates(analysis: dict[str, Any], user_prompt: str = "") -> dict[str, Any]:
    summary = analysis["summary"]
    unit = analysis["dataset"]["unit"]
    trend = summary["trend"]
    top_gap = summary["gaps"][0] if summary["gaps"] else None

    def gap_insight(index: int, insight_id: str, priority: int) -> dict[str, Any]:
        gap = summary["gaps"][index]
        high, low = format_number(gap["high_value"], gap["format"]), format_number(gap["low_value"], gap["format"])
        return {
            "id": insight_id,
            "title": f"{gap['metric']} varies most by {gap['column']}",
            "category": "segments",
            "severity": "high" if index == 0 else "medium",
            "summary": f"{gap['metric']} runs from {low} for {gap['low_label']} to {high} for {gap['high_label']}.",
            "detail": f"No other grouping separates {gap['metric'].lower()} as widely. Check group sizes before acting on the extremes.",
            "metric_label": f"{gap['metric']} range",
            "metric_value": f"{low} to {high}",
            "metric_sub": f"{gap['low_label']} vs {gap['high_label']}",
            "tags": ["segments"],
            "section": "segments",
            "priority": priority,
            "condition": gap["spread"] >= 10,
        }

    def overview_insight(index: int, priority: int) -> dict[str, Any]:
        metric = summary["metrics"][index]
        value = format_number(metric["value"], metric["format"])
        return {
            "id": f"metric_overview_{index + 1}",
            "title": f"{metric['name']} is {value} overall",
            "category": "overview",
            "severity": "low",
            "summary": f"Across {summary['row_count']:,} {unit}, {metric['name'].lower()} comes to {value}.",
            "detail": "The baseline the group comparisons are measured against.",
            "metric_label": metric["name"],
            "metric_value": value,
            "metric_sub": f"{summary['row_count']:,} {unit}",
            "tags": ["segments"],
            "section": "segments",
            "priority": priority,
        }

    builders = [
        *(
            (lambda index=index, spec=spec: gap_insight(index, *spec))
            for index, spec in enumerate([("metric_gap", 95), ("metric_second_gap", 91), ("metric_third_gap", 88)])
            if index < len(summary["gaps"])
        ),
        lambda: {
            "id": "no_metric_gap",
            "title": f"No grouping separates {top_gap['metric'].lower()} much",
            "category": "segments",
            "severity": "medium",
            "summary": (
                f"The widest range is by {top_gap['column']}: {format_number(top_gap['low_value'], top_gap['format'])} "
                f"for {top_gap['low_label']} to {format_number(top_gap['high_value'], top_gap['format'])} for {top_gap['high_label']}."
            ),
            "detail": "Performance is even across every grouping in the file, so the levers are probably not in how it is segmented.",
            "metric_label": "Widest range",
            "metric_value": f"{top_gap['spread']:.1f}%",
            "metric_sub": f"of the overall figure, by {top_gap['column']}",
            "tags": ["segments"],
            "section": "segments",
            "priority": 87,
            "condition": top_gap["spread"] < 10,
        },
        lambda: {
            "id": "metric_trend",
            "title": f"{trend['metric']} {'rose' if trend['change_pct'] > 0 else 'fell'} {abs(trend['change_pct']):.0f}% over the period",
            "category": "trends",
            "severity": "high",
            "summary": (
                f"{trend['metric']} moved from {format_number(trend['first'], trend['format'])} in {trend['first_label']} "
                f"to {format_number(trend['last'], trend['format'])} in {trend['last_label']}."
            ),
            "detail": "Compare like periods before reading this as a lasting change: seasonality can produce the same shape.",
            "metric_label": f"{trend['metric']} change",
            "metric_value": f"{trend['change_pct']:+.0f}%",
            "metric_sub": f"{trend['first_label']} to {trend['last_label']}",
            "tags": ["trends"],
            "section": "trends",
            "priority": 93,
            "condition": abs(trend["change_pct"]) >= 10,
        },
        *(
            (lambda index=index, priority=priority: overview_insight(index, priority))
            for index, priority in enumerate([86, 85])
            if index < len(summary["metrics"])
        ),
    ]
    focus_tags = extract_focus_tags(user_prompt)
    prompt_terms = extract_prompt_terms(user_prompt, PROMPT_STOP_WORDS)
    insights = score_insights(collect_insights(builders), focus_tags, prompt_terms, minimum_recommended=3)
    return {"insights": insights, "focus_tags": focus_tags}


def build_metric_layout(
    *,
    approved_insights: list[dict[str, Any]],
    metric_cards: list[dict[str, Any]],
    analysis: dict[str, Any],
    included_sections: list[str],
    show_notes: bool,
) -> list[dict[str, Any]]:
    signals = analysis["signals"]
    summary = analysis["summary"]
    section_map = {
        "overview": {
            "id": "overview",
            "title": "Performance Overview",
            "description": "The headline numbers and where they differ most.",
            "blocks": [
                {"id": "overview-metrics", "kind": "metric_grid", "cards": [metric_card(card) for card in metric_cards]},
                {"id": "overview-insights", "kind": "insight_grid", "insights": [insight_card(item) for item in approved_insights[:4]]},
                {
                    "id": "overview-totals",
                    "kind": "stat_list",
                    "title": "Overall",
                    "items": [
                        {"label": metric["name"], "value": format_number(metric["value"], metric["format"])}
                        for metric in summary["metrics"]
                    ],
                },
            ],
        },
        "segments": {
            "id": "segments",
            "title": "Metrics by Group",
            "description": "Each metric across the grouping that separates it most. Small groups are left out.",
            "blocks": [
                {
                    "id": f"metric-by-dimension-{index}",
                    "kind": "chart",
                    "chart": {
                        "id": f"metric-by-dimension-{index}",
                        "title": f"{table['metric']} by {table['column']}",
                        "subtitle": "Highest first.",
                        "type": "bar",
                        "labels": table["labels"],
                        "series": [{"name": table["metric"], "values": table["values"], "color": CHART_COLORS[index % len(CHART_COLORS)]}],
                        "format": table["format"],
                    },
                }
                for index, table in enumerate(signals["metric_by_dimension"])
            ],
        },
        "trends": {
            "id": "trends",
            "title": "Metrics Over Time",
            "description": "How the main metrics moved across the dataset's date range.",
            "blocks": [
                {
                    "id": f"metric-trend-{index}",
                    "kind": "chart",
                    "chart": {
                        "id": f"metric-trend-{index}",
                        "title": f"{trend['metric']} by {trend['period']}",
                        "subtitle": "Periods at either end with very few rows are left out.",
                        "type": "line",
                        "labels": trend["labels"],
                        "series": [{"name": trend["metric"], "values": trend["values"], "color": CHART_COLORS[index % len(CHART_COLORS)]}],
                        "format": trend["format"],
                    },
                }
                for index, trend in enumerate(signals["metric_trends"])
            ],
        },
    }
    if show_notes:
        section_map["notes"] = {
            "id": "notes",
            "title": "Approved Insight Notes",
            "description": "Narrative notes kept alongside the dashboard for review and export.",
            "blocks": [{"id": "notes-list", "kind": "note_list", "insights": [insight_card(item) for item in approved_insights]}],
        }
    return prune_empty_blocks([section_map[section] for section in included_sections if section in section_map])


def metric_section_options(analysis: dict[str, Any]) -> dict[str, str]:
    """The sections this dataset can fill."""
    filled = build_metric_layout(
        approved_insights=[],
        metric_cards=[],
        analysis=analysis,
        included_sections=list(SECTION_CONFIG),
        show_notes=True,
    )
    available = {section["id"] for section in filled} | {"overview", "notes"}
    return {section: label for section, label in SECTION_CONFIG.items() if section in available}


def is_metric_analysis(analysis: Optional[dict[str, Any]]) -> bool:
    return bool(analysis) and analysis.get("dataset", {}).get("shape") == "metrics"


def extract_focus_tags(prompt: str) -> list[str]:
    lowered = prompt.strip().lower()
    if not lowered:
        return []
    return [tag for tag, keywords in FOCUS_KEYWORDS.items() if any(keyword in lowered for keyword in keywords)]


def _trends(df: pd.DataFrame, metrics: list[dict[str, Any]], measure: Any) -> list[dict[str, Any]]:
    """Each metric per period, on the dataset's date column if it has one."""
    # A real date column, or one named like a date whose values parse as dates ("Day" may hold weekday names).
    stamps = None
    for column in df.columns:
        named_like_a_date = normalize_name(column) in {"date", "day", "week", "month", "reportdate", "timestamp"}
        if not (pd.api.types.is_datetime64_any_dtype(df[column]) or named_like_a_date):
            continue
        parsed = pd.to_datetime(df[column], errors="coerce")
        if parsed.notna().sum() >= 0.9 * len(df):
            stamps = parsed
            break
    if stamps is None:
        return []
    valid = stamps.notna()
    if (stamps[valid].max() - stamps[valid].min()).days < 14:
        return []
    for period_name, frequency in (("day", "D"), ("week", "W"), ("month", "M"), ("quarter", "Q"), ("year", "Y")):
        periods = stamps[valid].dt.to_period(frequency)
        if periods.nunique() <= MAX_PERIODS:
            break
    # Drop thinly populated periods at either end: an export that starts mid-month would open with a dip.
    counts = periods.value_counts().sort_index()
    populated = [period for period in counts.index if counts[period] >= max(1.0, float(counts.median()) * 0.25)]
    if len(populated) < 3:
        return []
    kept = [period for period in counts.index if populated[0] <= period <= populated[-1]]
    period_of_row = periods.reindex(df.index)

    trends = []
    for metric in metrics[:3]:
        points = [
            (str(period.start_time.date()) if frequency == "W" else str(period), measure(metric, valid & (period_of_row == period)))
            for period in kept
        ]
        points = [(label, value) for label, value in points if value is not None]
        if len(points) < 3 or not points[0][1]:
            continue
        third = max(1, len(points) // 3)
        first = sum(value for _, value in points[:third]) / third
        last = sum(value for _, value in points[-third:]) / third
        trends.append(
            {
                "metric": metric["name"],
                "format": metric.get("format", "number"),
                "period": period_name,
                "labels": [label for label, _ in points],
                "values": [value for _, value in points],
                "change": {
                    "metric": metric["name"],
                    "format": metric.get("format", "number"),
                    "first": round(first, 4),
                    "last": round(last, 4),
                    "first_label": points[0][0],
                    "last_label": points[-1][0],
                    "change_pct": round((last / first - 1) * 100, 2) if first else 0.0,
                },
            }
        )
    return trends
