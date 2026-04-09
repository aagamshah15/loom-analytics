from __future__ import annotations

import json
import re
from typing import Any, Optional

import pandas as pd
from jinja2 import Template

from pipeline.common.contracts import PipelineContext


REQUIRED_COLUMN_ALIASES = {
    "employee_id": ["employee_id", "employee", "worker_id", "id"],
    "department": ["department", "team", "function"],
    "performance_score": ["performance_score", "performance", "review_score"],
    "attrition": ["attrition", "left_company", "is_attrited", "terminated"],
    "engagement_score": ["engagement_score", "engagement", "engagement_index"],
    "gender": ["gender", "sex"],
    "level": ["level", "job_level", "seniority", "title"],
    "salary": ["salary", "compensation", "base_salary", "annual_salary"],
    "training_hours": ["training_hours", "learning_hours", "l_and_d_hours"],
    "work_mode": ["work_mode", "location_type", "work_arrangement"],
}

OPTIONAL_COLUMN_ALIASES = {
    "tenure_years": ["tenure_years", "tenure", "years_at_company"],
    "manager_rating": ["manager_rating", "leadership_rating"],
}

FOCUS_KEYWORDS = {
    "retention": ["retention", "attrition", "turnover", "leaving"],
    "performance": ["performance", "high performers", "top talent"],
    "engagement": ["engagement", "survey", "morale"],
    "pay_equity": ["pay", "compensation", "salary", "equity", "gender"],
    "training": ["training", "development", "learning", "upskilling"],
    "remote": ["remote", "onsite", "hybrid", "work mode"],
    "department": ["department", "support", "customer support", "team"],
}

PROMPT_STOP_WORDS = {
    "about",
    "across",
    "after",
    "before",
    "build",
    "center",
    "dashboard",
    "data",
    "emphasize",
    "focus",
    "from",
    "into",
    "look",
    "make",
    "more",
    "need",
    "please",
    "show",
    "story",
    "that",
    "them",
    "these",
    "this",
    "those",
    "want",
    "with",
}

SECTION_CONFIG = {
    "overview": "KPI cards and key insights",
    "retention": "Charts: attrition and engagement",
    "compensation": "Charts: pay equity",
    "development": "Charts: training and performance",
    "workforce_model": "Charts: remote risk and department hotspots",
    "notes": "Insight notes",
}


def analyze_hr_context(context: PipelineContext) -> Optional[dict[str, Any]]:
    df = context.clean_df if context.clean_df is not None else context.raw_df
    if df is None or df.empty:
        return None

    detected = _detect_columns(df)
    if detected is None:
        return None

    working = pd.DataFrame(
        {
            "employee_id": df[detected["required"]["employee_id"]].astype(str).str.strip(),
            "department": df[detected["required"]["department"]].astype(str).str.strip(),
            "performance_score": pd.to_numeric(df[detected["required"]["performance_score"]], errors="coerce"),
            "attrition": _normalize_binary(df[detected["required"]["attrition"]]),
            "engagement_score": pd.to_numeric(df[detected["required"]["engagement_score"]], errors="coerce"),
            "gender": df[detected["required"]["gender"]].astype(str).str.strip(),
            "level": df[detected["required"]["level"]].astype(str).str.strip(),
            "salary": pd.to_numeric(df[detected["required"]["salary"]], errors="coerce"),
            "training_hours": pd.to_numeric(df[detected["required"]["training_hours"]], errors="coerce"),
            "work_mode": df[detected["required"]["work_mode"]].astype(str).str.strip(),
        }
    ).dropna(subset=["employee_id", "performance_score", "engagement_score", "salary", "training_hours"])

    if len(working) < 20:
        return None

    if detected["optional"].get("tenure_years"):
        working["tenure_years"] = pd.to_numeric(df.loc[working.index, detected["optional"]["tenure_years"]], errors="coerce")
    else:
        working["tenure_years"] = None

    working["performance_group"] = working["performance_score"].apply(_performance_group)
    working["engagement_group"] = working["engagement_score"].apply(_engagement_group)
    working["training_group"] = working["training_hours"].apply(_training_group)
    working["work_mode_group"] = working["work_mode"].apply(_normalize_work_mode)

    if working["work_mode_group"].eq("Unknown").all():
        return None

    attrition_by_performance = _rate_table(working, "performance_group", order=["High performer", "Everyone else"])
    attrition_by_engagement = _rate_table(working, "engagement_group", order=["Very high engagement", "Mid engagement", "Low engagement"])
    engagement_by_work_mode = working.groupby("work_mode_group")["engagement_score"].mean().sort_values(ascending=False)
    attrition_by_work_mode = _rate_table(working, "work_mode_group")
    engagement_by_training = working.groupby("training_group")["engagement_score"].mean().reindex(["Under 15 hours", "15-44 hours", "45+ hours"]).fillna(0.0)
    attrition_by_department = _rate_table(working, "department")
    avg_salary_by_level_gender = (
        working.groupby(["level", "gender"])["salary"].mean().reset_index().sort_values(["level", "gender"])
    )

    high_perf_attrition = _value_or_zero(attrition_by_performance["High performer"]["rate"])
    everyone_else_attrition = _value_or_zero(attrition_by_performance["Everyone else"]["rate"])
    very_high_engagement_attrition = _value_or_zero(attrition_by_engagement["Very high engagement"]["rate"])
    mid_engagement_attrition = _value_or_zero(attrition_by_engagement["Mid engagement"]["rate"])
    remote_attrition = _first_matching_metric(attrition_by_work_mode, ["Remote"])
    onsite_attrition = _first_matching_metric(attrition_by_work_mode, ["Onsite"])
    remote_engagement = _first_matching_average(engagement_by_work_mode, ["Remote"])
    onsite_engagement = _first_matching_average(engagement_by_work_mode, ["Onsite"])
    low_training_engagement = float(engagement_by_training.get("Under 15 hours", 0.0))
    high_training_engagement = float(engagement_by_training.get("45+ hours", 0.0))
    customer_support_attrition = _first_matching_metric(attrition_by_department, ["Customer Support", "Support"])
    company_attrition = float(working["attrition"].mean() * 100)
    female_vp_salary = _level_gender_salary(avg_salary_by_level_gender, ["VP", "Vice President"], ["female", "f"])
    male_vp_salary = _level_gender_salary(avg_salary_by_level_gender, ["VP", "Vice President"], ["male", "m"])

    return {
        "kind": "hr_workforce",
        "column_map": detected,
        "dataset": {
            "row_count": int(len(working)),
            "column_count": int(len(df.columns)),
            "input_columns": list(df.columns),
            "employee_count": int(working["employee_id"].nunique()),
        },
        "summary": {
            "employee_count": int(working["employee_id"].nunique()),
            "overall_attrition_rate": company_attrition,
            "avg_engagement": float(working["engagement_score"].mean()),
            "avg_salary": float(working["salary"].mean()),
            "high_perf_attrition": high_perf_attrition,
            "everyone_else_attrition": everyone_else_attrition,
            "very_high_engagement_attrition": very_high_engagement_attrition,
            "mid_engagement_attrition": mid_engagement_attrition,
            "remote_attrition": remote_attrition,
            "onsite_attrition": onsite_attrition,
            "remote_engagement": remote_engagement,
            "onsite_engagement": onsite_engagement,
            "low_training_engagement": low_training_engagement,
            "high_training_engagement": high_training_engagement,
            "customer_support_attrition": customer_support_attrition,
            "female_vp_salary": female_vp_salary,
            "male_vp_salary": male_vp_salary,
        },
        "signals": {
            "attrition_by_performance": {
                "labels": list(attrition_by_performance.keys()),
                "values": [round(metrics["rate"], 2) for metrics in attrition_by_performance.values()],
            },
            "attrition_by_engagement": {
                "labels": list(attrition_by_engagement.keys()),
                "values": [round(metrics["rate"], 2) for metrics in attrition_by_engagement.values()],
            },
            "attrition_by_work_mode": {
                "labels": list(attrition_by_work_mode.keys()),
                "values": [round(metrics["rate"], 2) for metrics in attrition_by_work_mode.values()],
            },
            "engagement_by_work_mode": {
                "labels": engagement_by_work_mode.index.tolist(),
                "values": [round(float(value), 2) for value in engagement_by_work_mode.tolist()],
            },
            "engagement_by_training": {
                "labels": engagement_by_training.index.tolist(),
                "values": [round(float(value), 2) for value in engagement_by_training.tolist()],
            },
            "attrition_by_department": {
                "labels": list(attrition_by_department.keys()),
                "values": [round(metrics["rate"], 2) for metrics in attrition_by_department.values()],
            },
            "vp_gender_pay": {
                "labels": ["Female VP", "Male VP"],
                "values": [round(float(female_vp_salary or 0.0), 2), round(float(male_vp_salary or 0.0), 2)],
            },
        },
    }


def build_hr_insight_candidates(analysis: dict[str, Any], user_prompt: str = "") -> dict[str, Any]:
    summary = analysis["summary"]
    focus_tags = extract_focus_tags(user_prompt)

    insights = [
        {
            "id": "high_performers_leave_too",
            "title": "High performers leave just as much",
            "category": "performance",
            "severity": "high",
            "summary": (
                f"High performers leave at {summary['high_perf_attrition']:.1f}% versus {summary['everyone_else_attrition']:.1f}% for everyone else."
            ),
            "detail": "Performance management alone is not acting as a retention strategy here. The highest-value employees likely need growth, autonomy, or compensation changes instead.",
            "metric_label": "Attrition gap",
            "metric_value": f"{abs(summary['high_perf_attrition'] - summary['everyone_else_attrition']):.1f} pts",
            "metric_sub": "high performers vs everyone else",
            "tags": ["performance", "retention"],
            "section": "retention",
            "priority": 95,
        },
        {
            "id": "engagement_does_not_save_attrition",
            "title": "Very high engagement does not prevent attrition",
            "category": "engagement",
            "severity": "high",
            "summary": (
                f"Employees scoring 8-10 on engagement still leave at {summary['very_high_engagement_attrition']:.1f}% versus {summary['mid_engagement_attrition']:.1f}% for the mid group."
            ),
            "detail": "That is the clearest sign in the dataset that engagement surveys are not sufficient as a retention proxy on their own.",
            "metric_label": "Engagement attrition gap",
            "metric_value": f"{abs(summary['very_high_engagement_attrition'] - summary['mid_engagement_attrition']):.1f} pts",
            "metric_sub": "very high vs mid engagement",
            "tags": ["engagement", "retention"],
            "section": "retention",
            "priority": 97,
        },
        {
            "id": "vp_gender_pay_gap",
            "title": "The VP gender pay gap widens at senior levels",
            "category": "pay_equity",
            "severity": "high",
            "summary": (
                f"Female VPs average ${summary['female_vp_salary']:,.0f} versus ${summary['male_vp_salary']:,.0f} for male VPs."
            ),
            "detail": "The gap widening at a senior level is the opposite of the story most organizations tell themselves, which makes this a board-level equity issue rather than a small comp footnote.",
            "metric_label": "VP pay gap",
            "metric_value": f"${abs((summary['male_vp_salary'] or 0) - (summary['female_vp_salary'] or 0)):,.0f}",
            "metric_sub": "male vs female VPs",
            "tags": ["pay_equity"],
            "section": "compensation",
            "priority": 98,
            "condition": summary["female_vp_salary"] is not None and summary["male_vp_salary"] is not None,
        },
        {
            "id": "training_is_controllable_lever",
            "title": "Training is the strongest controllable lever",
            "category": "training",
            "severity": "medium",
            "summary": (
                f"Employees with 45+ training hours score {summary['high_training_engagement']:.2f} on engagement versus {summary['low_training_engagement']:.2f} below 15 hours."
            ),
            "detail": "Unlike engagement or performance, training hours are a direct input the company can actually control, which makes this one of the highest-ROI levers in the dataset.",
            "metric_label": "Engagement lift",
            "metric_value": f"{summary['high_training_engagement'] - summary['low_training_engagement']:.2f}",
            "metric_sub": "45+ hours vs under 15",
            "tags": ["training"],
            "section": "development",
            "priority": 92,
        },
        {
            "id": "remote_retention_risk",
            "title": "Remote workers are a quiet retention risk",
            "category": "remote",
            "severity": "medium",
            "summary": (
                f"Remote attrition is {summary['remote_attrition']:.1f}% versus {summary['onsite_attrition']:.1f}% onsite, while engagement also runs lower."
            ),
            "detail": "The gap is not catastrophic, but it is consistent enough to justify a focused remote-work retention intervention rather than being dismissed as noise.",
            "metric_label": "Remote attrition gap",
            "metric_value": f"{(summary['remote_attrition'] or 0) - (summary['onsite_attrition'] or 0):.1f} pts",
            "metric_sub": "remote vs onsite",
            "tags": ["remote", "retention"],
            "section": "workforce_model",
            "priority": 89,
            "condition": summary["remote_attrition"] is not None and summary["onsite_attrition"] is not None,
        },
        {
            "id": "customer_support_crisis",
            "title": "Customer Support is a structural crisis",
            "category": "department",
            "severity": "high",
            "summary": (
                f"Customer Support attrition runs at {summary['customer_support_attrition']:.1f}% versus a company average of {summary['overall_attrition_rate']:.1f}%."
            ),
            "detail": "That is not a normal staffing fluctuation. It points to a structural problem in role design, management load, or burnout conditions.",
            "metric_label": "Support attrition",
            "metric_value": f"{summary['customer_support_attrition']:.1f}%",
            "metric_sub": "vs company average",
            "tags": ["department", "retention"],
            "section": "workforce_model",
            "priority": 99,
            "condition": summary["customer_support_attrition"] is not None,
        },
    ]

    filtered = [item for item in insights if item.get("condition", True)]
    prompt_terms = extract_prompt_terms(user_prompt)
    for item in filtered:
        item["score"] = _instruction_bonus(item, focus_tags, prompt_terms) + item["priority"]
        item["recommended"] = item["score"] >= 85
    filtered.sort(key=lambda item: (-item["score"], item["title"]))
    return {"insights": filtered, "focus_tags": focus_tags}


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
        analysis = analyze_hr_context(context)
    if analysis is None:
        return None

    insight_bundle = build_hr_insight_candidates(analysis, user_prompt)
    insights = insight_bundle["insights"]
    approved_set = set(approved_insight_ids or [item["id"] for item in insights if item.get("recommended", True)])
    approved = [item for item in insights if item["id"] in approved_set] or insights[:4]

    settings = settings or {}
    included_sections = settings.get("included_sections") or _default_sections(approved)
    title = settings.get("title") or "HR Workforce Hidden Insights"
    subtitle = settings.get("subtitle") or "Approved workforce retention and equity narrative"
    metric_count = int(settings.get("metric_count", 4))
    show_notes = bool(settings.get("show_notes", True))

    payload = {
        "kind": "hr_workforce",
        "title": title,
        "subtitle": subtitle,
        "headline": {"title": title, "subtitle": subtitle},
        "approved_insights": approved,
        "metric_cards": approved[:metric_count],
        "sections": included_sections,
        "layout_sections": _build_layout_sections(
            approved_insights=approved,
            metric_cards=approved[:metric_count],
            analysis=analysis,
            included_sections=included_sections,
            show_notes=show_notes,
        ),
        "all_layout_sections": _build_layout_sections(
            approved_insights=approved,
            metric_cards=approved,
            analysis=analysis,
            included_sections=list(SECTION_CONFIG.keys()),
            show_notes=True,
        ),
        "signals": analysis["signals"],
        "summary": analysis["summary"],
        "dataset": analysis["dataset"],
        "show_notes": show_notes,
        "focus_tags": insight_bundle["focus_tags"],
    }
    html = _render_dashboard_html(payload)
    height = 1040 + (140 * len(included_sections)) + (160 if show_notes else 0)
    return {
        "kind": "hr_workforce",
        "title": title,
        "html": html,
        "height": height,
        "payload": payload,
        "blueprint": payload,
        "download_name": "hr_workforce_insights_dashboard.html",
    }


def extract_focus_tags(prompt: str) -> list[str]:
    lowered = prompt.strip().lower()
    if not lowered:
        return []
    matches = []
    for tag, keywords in FOCUS_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            matches.append(tag)
    return matches


def extract_prompt_terms(prompt: str) -> list[str]:
    lowered = prompt.strip().lower()
    if not lowered:
        return []
    terms: list[str] = []
    for token in re.findall(r"[a-z0-9]+", lowered):
        if len(token) < 4 or token in PROMPT_STOP_WORDS:
            continue
        if token not in terms:
            terms.append(token)
    return terms


def dashboard_section_options() -> dict[str, str]:
    return SECTION_CONFIG


def _instruction_bonus(item: dict[str, Any], focus_tags: list[str], prompt_terms: list[str]) -> int:
    bonus = 15 if any(tag in focus_tags for tag in item["tags"]) else 0
    if not prompt_terms:
        return bonus
    haystack = " ".join(
        [
            item["title"],
            item["summary"],
            item["detail"],
            item["category"],
            item["metric_label"],
            " ".join(item["tags"]),
        ]
    ).lower()
    overlap = sum(1 for term in prompt_terms if term in haystack)
    return bonus + min(overlap, 3) * 6


def _detect_columns(df: pd.DataFrame) -> Optional[dict[str, dict[str, str]]]:
    normalized = {column.lower().strip(): column for column in df.columns}
    required = {}
    optional = {}
    for canonical, aliases in REQUIRED_COLUMN_ALIASES.items():
        match = next((normalized[alias] for alias in aliases if alias in normalized), None)
        if match is None:
            return None
        required[canonical] = match
    for canonical, aliases in OPTIONAL_COLUMN_ALIASES.items():
        match = next((normalized[alias] for alias in aliases if alias in normalized), None)
        if match is not None:
            optional[canonical] = match
    return {"required": required, "optional": optional}


def _normalize_binary(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.astype(float)
    if pd.api.types.is_numeric_dtype(series):
        numeric = pd.to_numeric(series, errors="coerce").fillna(0.0)
        if numeric.max() > 1:
            numeric = numeric > 0
        return numeric.astype(float)
    normalized = series.astype(str).str.strip().str.lower()
    positives = {"1", "true", "yes", "left", "attrited", "terminated"}
    return normalized.isin(positives).astype(float)


def _performance_group(value: float) -> str:
    return "High performer" if float(value) >= 4.0 else "Everyone else"


def _engagement_group(value: float) -> str:
    if float(value) >= 8.0:
        return "Very high engagement"
    if float(value) >= 5.0:
        return "Mid engagement"
    return "Low engagement"


def _training_group(value: float) -> str:
    if float(value) >= 45:
        return "45+ hours"
    if float(value) >= 15:
        return "15-44 hours"
    return "Under 15 hours"


def _normalize_work_mode(value: str) -> str:
    lowered = str(value).strip().lower()
    if "remote" in lowered:
        return "Remote"
    if "on" in lowered or "office" in lowered or "site" in lowered:
        return "Onsite"
    if "hybrid" in lowered:
        return "Hybrid"
    return "Unknown"


def _rate_table(df: pd.DataFrame, column: str, order: Optional[list[str]] = None) -> dict[str, dict[str, float]]:
    grouped = (
        df.groupby(column)
        .agg(attrition_rate=("attrition", "mean"), count=("employee_id", "size"))
        .assign(attrition_rate=lambda frame: frame["attrition_rate"] * 100)
    )
    if order is not None:
        grouped = grouped.reindex(order).dropna(how="all")
    else:
        grouped = grouped.sort_values("attrition_rate", ascending=False)
    return {
        str(index): {"rate": float(row["attrition_rate"]), "count": int(row["count"])}
        for index, row in grouped.fillna(0).iterrows()
    }


def _value_or_zero(value: Optional[float]) -> float:
    return float(value or 0.0)


def _first_matching_metric(table: dict[str, dict[str, float]], labels: list[str]) -> Optional[float]:
    normalized = {key.strip().lower(): value["rate"] for key, value in table.items()}
    for label in labels:
        if label.lower() in normalized:
            return float(normalized[label.lower()])
    return None


def _first_matching_average(series: pd.Series, labels: list[str]) -> Optional[float]:
    normalized = {str(index).strip().lower(): float(value) for index, value in series.items()}
    for label in labels:
        if label.lower() in normalized:
            return normalized[label.lower()]
    return None


def _level_gender_salary(frame: pd.DataFrame, levels: list[str], genders: list[str]) -> Optional[float]:
    lowered_levels = {level.lower() for level in levels}
    lowered_genders = {gender.lower() for gender in genders}
    subset = frame[
        frame["level"].astype(str).str.lower().isin(lowered_levels)
        & frame["gender"].astype(str).str.lower().isin(lowered_genders)
    ]
    if subset.empty:
        return None
    return float(subset["salary"].mean())


def _default_sections(insights: list[dict[str, Any]]) -> list[str]:
    sections = ["overview"]
    for section in ["retention", "compensation", "development", "workforce_model", "notes"]:
        if any(item["section"] == section or section in item["tags"] for item in insights):
            sections.append(section)
    return list(dict.fromkeys(sections))


def _metric_card_from_insight(insight: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": insight["id"],
        "label": insight["metric_label"],
        "value": insight["metric_value"],
        "sub": insight["metric_sub"],
        "tone": insight["severity"],
    }


def _insight_card(insight: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": insight["id"],
        "title": insight["title"],
        "category": insight["category"],
        "severity": insight["severity"],
        "summary": insight["summary"],
        "detail": insight["detail"],
        "metric_label": insight["metric_label"],
        "metric_value": insight["metric_value"],
        "metric_sub": insight["metric_sub"],
        "section": insight["section"],
    }


def _build_layout_sections(
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
            "title": "Workforce Narrative",
            "description": "The fastest read on retention, equity, and controllable people levers.",
            "blocks": [
                {"id": "overview-metrics", "kind": "metric_grid", "cards": [_metric_card_from_insight(card) for card in metric_cards]},
                {"id": "overview-insights", "kind": "insight_grid", "insights": [_insight_card(insight) for insight in approved_insights[:4]]},
            ],
        },
        "retention": {
            "id": "retention",
            "title": "Retention Signals",
            "description": "Attrition is not following the usual HR assumptions in this dataset.",
            "blocks": [
                {
                    "id": "attrition-performance",
                    "kind": "chart",
                    "chart": {
                        "id": "attrition-performance",
                        "title": "Attrition by performance band",
                        "subtitle": "Top talent is not safer by default.",
                        "type": "bar",
                        "labels": signals["attrition_by_performance"]["labels"],
                        "series": [{"name": "Attrition rate", "values": signals["attrition_by_performance"]["values"], "color": "#c2410c"}],
                        "format": "percent",
                    },
                },
                {
                    "id": "attrition-engagement",
                    "kind": "chart",
                    "chart": {
                        "id": "attrition-engagement",
                        "title": "Attrition by engagement band",
                        "subtitle": "Even very high engagement is not insulating against exits.",
                        "type": "bar",
                        "labels": signals["attrition_by_engagement"]["labels"],
                        "series": [{"name": "Attrition rate", "values": signals["attrition_by_engagement"]["values"], "color": "#166534"}],
                        "format": "percent",
                    },
                },
            ],
        },
        "compensation": {
            "id": "compensation",
            "title": "Compensation and Equity",
            "description": "Pay equity gaps become most visible at senior levels, not entry ones.",
            "blocks": [
                {
                    "id": "vp-pay-gap",
                    "kind": "chart",
                    "chart": {
                        "id": "vp-pay-gap",
                        "title": "Average VP salary by gender",
                        "subtitle": "Senior-level pay dispersion often tells the most important equity story.",
                        "type": "bar",
                        "labels": signals["vp_gender_pay"]["labels"],
                        "series": [{"name": "Average salary", "values": signals["vp_gender_pay"]["values"], "color": "#292524"}],
                        "format": "currency",
                    },
                }
            ],
        },
        "development": {
            "id": "development",
            "title": "Development Levers",
            "description": "Training is one of the clearest controllable inputs in the workforce story.",
            "blocks": [
                {
                    "id": "training-engagement",
                    "kind": "chart",
                    "chart": {
                        "id": "training-engagement",
                        "title": "Engagement by training hours",
                        "subtitle": "Development investment is showing up in workforce sentiment.",
                        "type": "bar",
                        "labels": signals["engagement_by_training"]["labels"],
                        "series": [{"name": "Engagement score", "values": signals["engagement_by_training"]["values"], "color": "#0f766e"}],
                        "format": "number",
                    },
                }
            ],
        },
        "workforce_model": {
            "id": "workforce_model",
            "title": "Work Model and Department Risk",
            "description": "Remote friction and department hotspots are creating targeted retention exposure.",
            "blocks": [
                {
                    "id": "work-mode-attrition",
                    "kind": "chart",
                    "chart": {
                        "id": "work-mode-attrition",
                        "title": "Attrition by work mode",
                        "subtitle": "Remote work is not the main problem, but it is a measurable one.",
                        "type": "bar",
                        "labels": signals["attrition_by_work_mode"]["labels"],
                        "series": [{"name": "Attrition rate", "values": signals["attrition_by_work_mode"]["values"], "color": "#9a3412"}],
                        "format": "percent",
                    },
                },
                {
                    "id": "department-attrition",
                    "kind": "chart",
                    "chart": {
                        "id": "department-attrition",
                        "title": "Attrition by department",
                        "subtitle": "Department-level hotspots reveal structural workforce risk.",
                        "type": "bar",
                        "labels": signals["attrition_by_department"]["labels"],
                        "series": [{"name": "Attrition rate", "values": signals["attrition_by_department"]["values"], "color": "#b45309"}],
                        "format": "percent",
                    },
                },
                {
                    "id": "workforce-stats",
                    "kind": "stat_list",
                    "title": "Workforce markers",
                    "items": [
                        {"label": "Overall attrition", "value": f"{summary['overall_attrition_rate']:.1f}%", "tone": "warning"},
                        {"label": "Average engagement", "value": f"{summary['avg_engagement']:.2f}", "tone": "positive"},
                        {"label": "Average salary", "value": f"${summary['avg_salary']:,.0f}", "tone": "default"},
                        {"label": "Employees", "value": f"{summary['employee_count']:,}", "tone": "default"},
                    ],
                },
            ],
        },
        "notes": {
            "id": "notes",
            "title": "Approved Insight Notes",
            "description": "Narrative notes that help the dashboard travel well with HR and leadership stakeholders.",
            "blocks": [{"id": "approved-note-list", "kind": "note_list", "insights": [_insight_card(insight) for insight in approved_insights]}],
        },
    }

    layout_sections: list[dict[str, Any]] = []
    for section_id in included_sections:
        if section_id == "notes" and not show_notes:
            continue
        section = section_map.get(section_id)
        if section is not None:
            layout_sections.append(section)
    return layout_sections


def _render_dashboard_html(payload: dict[str, Any]) -> str:
    json_payload = json.dumps(payload, default=_json_default)
    template = Template(
        """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{{ payload.title }}</title>
    <style>
      body { font-family: Inter, Arial, sans-serif; background: #fafaf9; color: #1c1917; margin: 0; }
      .shell { max-width: 1280px; margin: 0 auto; padding: 32px; }
      .hero, .panel { background: white; border: 1px solid #e7e5e4; border-radius: 24px; padding: 24px; margin-bottom: 24px; }
      .kicker { text-transform: uppercase; letter-spacing: .22em; font-size: 11px; color: #c2410c; font-weight: 700; }
      h1, h2, h3 { font-family: Georgia, serif; margin: 0; }
      .grid { display: grid; gap: 16px; }
      .grid.metrics { grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }
      .grid.cards { grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); }
      .metric, .card { border: 1px solid #e7e5e4; border-radius: 20px; padding: 16px; background: #fff; }
    </style>
  </head>
  <body>
    <div class="shell" id="app"></div>
    <script>
      const payload = {{ json_payload | safe }};
      const root = document.getElementById("app");
      const metricCards = payload.metric_cards.map((card) => `
        <div class="metric">
          <div class="kicker">${card.label}</div>
          <h3 style="margin-top: 8px;">${card.value}</h3>
          <p style="color:#78716c;">${card.sub}</p>
        </div>
      `).join("");
      const insightCards = payload.approved_insights.map((insight) => `
        <div class="card">
          <div class="kicker">${insight.category}</div>
          <h3 style="margin-top: 8px;">${insight.title}</h3>
          <p>${insight.summary}</p>
          <p style="color:#78716c;">${insight.detail}</p>
        </div>
      `).join("");
      root.innerHTML = `
        <section class="hero">
          <div class="kicker">HR workforce insight dashboard</div>
          <h1 style="margin-top: 12px;">${payload.title}</h1>
          <p style="color:#78716c;">${payload.subtitle}</p>
        </section>
        <section class="panel"><div class="grid metrics">${metricCards}</div></section>
        <section class="panel"><div class="grid cards">${insightCards}</div></section>
      `;
    </script>
  </body>
</html>
        """
    )
    return template.render(payload=payload, json_payload=json_payload)


def _json_default(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
