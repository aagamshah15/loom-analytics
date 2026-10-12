from __future__ import annotations

import re
from typing import Any, Iterable, Optional

import pandas as pd

from pipeline.business.common import (
    collect_insights,
    extract_prompt_terms,
    format_number,
    insight_card,
    is_label_column_name,
    metric_card,
    parse_flag,
    prune_empty_blocks,
    rate_by_dimensions,
    score_insights,
)
from pipeline.business.matching import normalize_name


# Many real exports share one shape whatever the domain: one row per case, a yes/no
# outcome (readmitted, converted, purchased), and columns that might explain it. A
# template supplies the outcome column and its vocabulary; this module supplies the
# analysis, insights and layout, so each domain gets the same quality of dashboard.

MIN_ROWS = 20

SECTION_CONFIG = {
    "overview": "KPI cards and key insights",
    "drivers": "Charts: where the rate differs",
    "measures": "Charts: how the two groups differ",
    "notes": "Insight notes",
}

FOCUS_KEYWORDS = {
    "drivers": ["driver", "segment", "group", "where", "which", "breakdown", "by "],
    "measures": ["measure", "average", "compare", "difference", "profile"],
}

PROMPT_STOP_WORDS = {
    "about", "across", "after", "before", "build", "dashboard", "data", "focus", "from", "into", "look",
    "more", "need", "please", "show", "story", "that", "them", "these", "this", "those", "want", "with",
}

DIMENSION_COLORS = ["#c2410c", "#0f766e", "#b45309", "#292524", "#166534"]


def context_support(columns: Iterable[Any], words: Iterable[str], exclude: Any = None) -> int:
    """How many other columns carry one of the domain's words.

    An outcome column alone says little: "converted" could be a unit conversion.
    A word counts when it is a whole token of the column name ("ad" in "total ads"
    does not count, "ads" does), or, for words of six letters or more, anywhere in it.
    """
    words = list(words)
    supported = 0
    for column in columns:
        if column == exclude:
            continue
        tokens = {token.lower() for token in re.findall(r"[A-Za-z][a-z]+|[A-Z]+(?![a-z])", str(column))}
        name = normalize_name(column)
        if any(word in tokens or (len(word) >= 6 and word in name) for word in words):
            supported += 1
    return supported


def detect_outcome(
    df: pd.DataFrame,
    outcomes: list[dict[str, Any]],
    context_words: Iterable[str],
) -> Optional[tuple[Any, pd.Series, dict[str, str]]]:
    """Find the first listed outcome this dataset records as a yes/no column.

    Each outcome gives its ``aliases``, optional ``positives``/``negatives`` value
    words, a ``vocabulary``, and ``support``: how many other columns must carry the
    domain's words (default 2) before the column is trusted.
    """
    names = {normalize_name(column): column for column in df.columns}
    for outcome in outcomes:
        column = next((names[key] for key in map(normalize_name, outcome["aliases"]) if key in names), None)
        if column is None or context_support(df.columns, context_words, exclude=column) < outcome.get("support", 2):
            continue
        flag = parse_flag(df[column], positives=outcome.get("positives", ()), negatives=outcome.get("negatives", ()))
        if flag is not None:
            return column, flag, outcome["vocabulary"]
    return None


def analyze_outcome(
    df: pd.DataFrame,
    flag: pd.Series,
    *,
    kind: str,
    profile: str,
    outcome_column: Any,
    vocabulary: dict[str, str],
    exclude: tuple[Any, ...] = (),
) -> Optional[dict[str, Any]]:
    """Describe a yes/no outcome: how common it is, where its rate differs, and what differs with it.

    ``vocabulary`` names things for the reader: ``rate_label`` ("Readmission rate"),
    ``positive`` (an adjective for cases with the outcome, "readmitted") and ``unit``
    (what a row is, "encounters"). Returns None when the data gives nothing to compare.
    """
    known = flag.notna()
    if known.sum() < MIN_ROWS:
        return None
    frame = df.loc[known]
    flag = flag[known]

    candidates = [column for column in frame.columns if column != outcome_column and column not in exclude]
    rate_tables = rate_by_dimensions(frame, flag, candidates, limit=5)
    measure_gaps = _measure_gaps(frame, flag, candidates)
    if not rate_tables and not measure_gaps:
        return None

    gaps = [
        {
            "column": table["column"],
            "label": table["labels"][0],
            "rate": table["values"][0],
            "count": table["counts"][0],
            "low_label": table["labels"][-1],
            "low_rate": table["values"][-1],
        }
        for table in rate_tables[:3]
    ]
    return {
        "kind": kind,
        "profile": profile,
        "outcome": dict(vocabulary, column=str(outcome_column)),
        "dataset": {
            "row_count": int(len(frame)),
            "column_count": int(len(df.columns)),
            "input_columns": [str(column) for column in df.columns],
            "shape": "outcome",
        },
        "summary": {
            "row_count": int(len(frame)),
            "outcome_count": int(flag.sum()),
            "outcome_rate": float(flag.mean() * 100),
            "gaps": gaps,
            "measure_gaps": measure_gaps[:3],
        },
        "signals": {
            "rate_by_dimension": rate_tables,
            "measure_by_outcome": measure_gaps[:4],
        },
    }


def build_outcome_insight_candidates(analysis: dict[str, Any], user_prompt: str = "") -> dict[str, Any]:
    summary = analysis["summary"]
    words = analysis["outcome"]
    rate_label, positive, unit = words["rate_label"], words["positive"], words["unit"]
    overall = summary["outcome_rate"]
    top_gap = summary["gaps"][0] if summary["gaps"] else None

    def gap_insight(index: int, insight_id: str, priority: int) -> dict[str, Any]:
        gap = summary["gaps"][index]
        return {
            "id": insight_id,
            "title": (
                f"{rate_label} is highest where {gap['column']} is {gap['label']}"
                if index == 0
                else f"{rate_label} also differs by {gap['column']}"
            ),
            "category": "drivers",
            "severity": "high" if index == 0 else "medium",
            "summary": (
                f"{rate_label} is {gap['rate']:.1f}% across the {gap['count']:,} {unit} where {gap['column']} is {gap['label']}, "
                f"versus {overall:.1f}% overall and {gap['low_rate']:.1f}% where it is {gap['low_label']}."
            ),
            "detail": "A gap between groups is a place to look for a cause, not proof of one: the groups may differ in other ways too.",
            "metric_label": "Widest gap" if index == 0 else f"Gap by {gap['column']}",
            "metric_value": f"{gap['rate'] - gap['low_rate']:.1f} pts",
            "metric_sub": f"{gap['column']}: {gap['label']} vs {gap['low_label']}",
            "tags": ["drivers"],
            "section": "drivers",
            "priority": priority,
            "condition": _is_notable_gap(gap["rate"], gap["low_rate"]),
        }

    def measure_insight(index: int, insight_id: str, priority: int) -> dict[str, Any]:
        gap = summary["measure_gaps"][index]
        direction = "higher" if gap["yes_mean"] > gap["no_mean"] else "lower"
        return {
            "id": insight_id,
            "title": f"{positive.capitalize()} {unit} have {direction} {gap['column']}",
            "category": "measures",
            "severity": "medium",
            "summary": (
                f"{gap['column']} averages {format_number(gap['yes_mean'])} for {positive} {unit}, "
                f"versus {format_number(gap['no_mean'])} for the rest."
            ),
            "detail": "This is the size of the difference, not its direction of cause: the measure may follow the outcome rather than lead it.",
            "metric_label": f"{gap['column']} gap",
            "metric_value": f"{gap['pct_diff']:+.0f}%",
            "metric_sub": f"{positive} vs the rest",
            "tags": ["measures"],
            "section": "measures",
            "priority": priority,
            "condition": abs(gap["pct_diff"]) >= 10,
        }

    builders = [
        *(
            (lambda index=index, spec=spec: gap_insight(index, *spec))
            for index, spec in enumerate([("outcome_hotspot", 95), ("outcome_second_gap", 90), ("outcome_third_gap", 87)])
            if index < len(summary["gaps"])
        ),
        *(
            (lambda index=index, spec=spec: measure_insight(index, *spec))
            for index, spec in enumerate([("measure_gap", 89), ("measure_second_gap", 86)])
            if index < len(summary["measure_gaps"])
        ),
        lambda: {
            "id": "no_notable_gap",
            "title": f"No grouping moves {rate_label.lower()} much",
            "category": "drivers",
            "severity": "medium",
            "summary": (
                f"The widest gap is {top_gap['rate'] - top_gap['low_rate']:.2f} points: {top_gap['rate']:.1f}% where "
                f"{top_gap['column']} is {top_gap['label']}, {top_gap['low_rate']:.1f}% where it is {top_gap['low_label']}."
            ),
            "detail": "For a test, that is the result: the variants perform about the same. Check the sample is large enough before calling it final.",
            "metric_label": "Widest gap",
            "metric_value": f"{top_gap['rate'] - top_gap['low_rate']:.2f} pts",
            "metric_sub": f"{top_gap['column']}: {top_gap['label']} vs {top_gap['low_label']}",
            "tags": ["drivers"],
            "section": "drivers",
            "priority": 88,
            "condition": not _is_notable_gap(top_gap["rate"], top_gap["low_rate"]),
        },
        lambda: {
            "id": "overall_rate",
            "title": f"{rate_label} is {overall:.1f}% overall",
            "category": "overview",
            "severity": "low",
            "summary": f"{summary['outcome_count']:,} of {summary['row_count']:,} {unit} are {positive}.",
            "detail": "This is the baseline every group comparison on this dashboard is measured against.",
            "metric_label": rate_label,
            "metric_value": f"{overall:.1f}%",
            "metric_sub": f"{summary['outcome_count']:,} of {summary['row_count']:,} {unit}",
            "tags": ["drivers"],
            "section": "drivers",
            "priority": 85,
        },
    ]

    focus_tags = extract_focus_tags(user_prompt)
    prompt_terms = extract_prompt_terms(user_prompt, PROMPT_STOP_WORDS)
    insights = score_insights(collect_insights(builders), focus_tags, prompt_terms, minimum_recommended=3)
    return {"insights": insights, "focus_tags": focus_tags}


def build_outcome_layout(
    *,
    approved_insights: list[dict[str, Any]],
    metric_cards: list[dict[str, Any]],
    analysis: dict[str, Any],
    included_sections: list[str],
    show_notes: bool,
) -> list[dict[str, Any]]:
    signals = analysis["signals"]
    words = analysis["outcome"]
    section_map = {
        "overview": {
            "id": "overview",
            "title": f"{words['rate_label']} Overview",
            "description": f"How often {words['unit']} are {words['positive']}, and where that differs most.",
            "blocks": [
                {"id": "overview-metrics", "kind": "metric_grid", "cards": [metric_card(card) for card in metric_cards]},
                {"id": "overview-insights", "kind": "insight_grid", "insights": [insight_card(item) for item in approved_insights[:4]]},
            ],
        },
        "drivers": {
            "id": "drivers",
            "title": f"Where {words['rate_label']} Differs",
            "description": "The grouping columns with the widest gaps. Very small groups are left out.",
            "blocks": [
                {
                    "id": f"rate-by-dimension-{index}",
                    "kind": "chart",
                    "chart": {
                        "id": f"rate-by-dimension-{index}",
                        "title": f"{words['rate_label']} by {table['column']}",
                        "subtitle": f"Share of each group's {words['unit']} that are {words['positive']}.",
                        "type": "bar",
                        "labels": table["labels"],
                        "series": [{"name": words["rate_label"], "values": table["values"], "color": DIMENSION_COLORS[index % len(DIMENSION_COLORS)]}],
                        "format": "percent",
                    },
                }
                for index, table in enumerate(signals["rate_by_dimension"])
            ],
        },
        "measures": {
            "id": "measures",
            "title": "How the Two Groups Differ",
            "description": f"Averages for {words['positive']} {words['unit']} against the rest, for the measures that differ most.",
            "blocks": [
                {
                    "id": f"measure-by-outcome-{index}",
                    "kind": "chart",
                    "chart": {
                        "id": f"measure-by-outcome-{index}",
                        "title": f"Average {gap['column']}",
                        "subtitle": f"{words['positive'].capitalize()} {words['unit']} against the rest.",
                        "type": "bar",
                        "labels": [words["positive"].capitalize(), "The rest"],
                        "series": [{"name": f"Average {gap['column']}", "values": [gap["yes_mean"], gap["no_mean"]], "color": "#292524"}],
                        "format": "number",
                    },
                }
                for index, gap in enumerate(signals["measure_by_outcome"])
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


def outcome_section_options(analysis: dict[str, Any]) -> dict[str, str]:
    """The sections this dataset can fill."""
    filled = build_outcome_layout(
        approved_insights=[],
        metric_cards=[],
        analysis=analysis,
        included_sections=list(SECTION_CONFIG),
        show_notes=True,
    )
    available = {section["id"] for section in filled} | {"overview", "notes"}
    return {section: label for section, label in SECTION_CONFIG.items() if section in available}


def is_outcome_analysis(analysis: Optional[dict[str, Any]]) -> bool:
    return bool(analysis) and analysis.get("dataset", {}).get("shape") == "outcome"


def extract_focus_tags(prompt: str) -> list[str]:
    lowered = prompt.strip().lower()
    if not lowered:
        return []
    return [tag for tag, keywords in FOCUS_KEYWORDS.items() if any(keyword in lowered for keyword in keywords)]


def _is_notable_gap(high_rate: float, low_rate: float) -> bool:
    """Three points apart, or a quarter higher in relative terms.

    The relative test matters for rare outcomes: 2.6% against 1.8% is under a point
    apart yet a 44% lift, which is the whole result of an A/B test.
    """
    gap = high_rate - low_rate
    return gap >= 3 or (gap >= 0.3 and low_rate > 0 and high_rate / low_rate >= 1.25)


def _measure_gaps(frame: pd.DataFrame, flag: pd.Series, candidates: list[Any]) -> list[dict[str, Any]]:
    """Numeric columns whose average differs between the two outcome groups, largest standardized gap first."""
    gaps = []
    for column in candidates:
        if is_label_column_name(column) or not pd.api.types.is_numeric_dtype(frame[column]):
            continue
        values = pd.to_numeric(frame[column], errors="coerce")
        if values.nunique(dropna=True) <= 2:
            continue
        yes, no = values[flag == 1.0].dropna(), values[flag == 0.0].dropna()
        spread = float(values.std())
        if len(yes) < 10 or len(no) < 10 or not spread or pd.isna(spread):
            continue
        yes_mean, no_mean = float(yes.mean()), float(no.mean())
        effect = (yes_mean - no_mean) / spread
        if abs(effect) < 0.1 or not no_mean:
            continue
        gaps.append(
            {
                "column": str(column),
                "yes_mean": round(yes_mean, 4),
                "no_mean": round(no_mean, 4),
                "pct_diff": round((yes_mean - no_mean) / abs(no_mean) * 100, 2),
                "effect": round(effect, 4),
            }
        )
    gaps.sort(key=lambda gap: -abs(gap["effect"]))
    return gaps
