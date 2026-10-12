from __future__ import annotations

import re
from typing import Any, Optional

import pandas as pd

from pipeline.business.common import collect_insights, html_template, parse_flag, prune_empty_blocks, rate_by_dimensions, script_json, template_frame
from pipeline.business.matching import detect_columns, normalize_name
from pipeline.common.contracts import PipelineContext


# The one thing an HR dataset must have: who left. Everything else adds a part of the dashboard.
REQUIRED_COLUMN_ALIASES = {
    "attrition": [
        "attrition", "left", "left_company", "is_attrited", "terminated", "termd", "leave_or_not",
        "churned", "employment_status", "status",
    ],
}

OPTIONAL_COLUMN_ALIASES = {
    "department": ["department", "department_name", "dept", "team", "function", "business_unit", "division"],
    "performance_score": ["performance_score", "performance", "review_score", "performancerating", "performance_rating", "last_evaluation"],
    "engagement_score": [
        "engagement_score", "engagement", "engagement_index", "engagement_survey", "jobsatisfaction", "job_satisfaction",
        "environmentsatisfaction", "environment_satisfaction", "satisfaction_level", "emp_satisfaction", "satisfaction",
    ],
    "salary": ["salary", "compensation", "base_salary", "annual_salary", "monthlyincome", "monthly_income", "salaryslab", "salary_slab", "pay_rate", "wage"],
    "employee_id": ["employee_id", "employee", "worker_id", "employeenumber", "employee_number", "empid", "emp_id", "id"],
    "gender": ["gender", "sex", "gender_full", "gender_short"],
    "level": ["level", "job_level", "joblevel", "seniority", "title", "job_title", "position", "job_role"],
    "training_hours": ["training_hours", "learning_hours", "l_and_d_hours", "trainingtimeslastyear", "training_times_last_year"],
    "work_mode": ["work_mode", "location_type", "work_arrangement", "location", "workplace_type"],
    "tenure_years": ["tenure_years", "tenure", "years_at_company", "length_of_service", "years_of_service", "time_spend_company"],
    "manager_rating": ["manager_rating", "leadership_rating"],
    "age": ["age"],
    "hire_year": ["joining_year", "hire_year", "year_hired"],
}

# A leave flag alone could describe customers or subscriptions. These fields mark the rows as employees.
HR_CONTEXT_FIELDS = (
    "department", "performance_score", "engagement_score", "salary", "level",
    "training_hours", "tenure_years", "manager_rating", "hire_year",
)
# Column names too general to trust on one supporting field.
WEAK_ATTRITION_NAMES = {"status", "left", "terminated", "churned"}

LEFT_WORDS = {"left", "attrited", "terminated", "resigned", "quit", "inactive", "voluntarily terminated", "terminated for cause"}
STAYED_WORDS = {"stayed", "active", "employed", "current", "leave of absence", "future start"}

# Rating scales written as words, on the template's 0-10 scale (a 1-4 scale times 2.5).
ORDINAL_WORDS = {
    "very low": 0.0, "pip": 2.5, "low": 2.5, "poor": 2.5, "needs improvement": 5.0, "medium": 5.0, "average": 5.0,
    "fully meets": 7.5, "meets": 7.5, "high": 7.5, "good": 7.5, "excellent": 7.5,
    "exceeds": 10.0, "very high": 10.0, "outstanding": 10.0,
}

AGE_BANDS = [(0, 30, "Under 30"), (30, 40, "30-39"), (40, 50, "40-49"), (50, 200, "50 and over")]
TENURE_BANDS = [(0, 2, "Under 2 years"), (2, 5, "2-4 years"), (5, 10, "5-9 years"), (10, 100, "10+ years")]

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
    "development": "Charts: training and development",
    "workforce_model": "Charts: remote risk and departments",
    "notes": "Insight notes",
}


def analyze_hr_context(context: PipelineContext) -> Optional[dict[str, Any]]:
    df = template_frame(context)
    if df is None or df.empty:
        return None

    detected = _detect_columns(df)
    if detected is None:
        return None
    fields = {**detected["optional"], **detected["required"]}

    attrition = parse_flag(df[fields["attrition"]], positives=LEFT_WORDS, negatives=STAYED_WORDS)
    if attrition is None or not _looks_like_workforce(fields):
        return None

    def numeric(field: str) -> pd.Series:
        column = fields.get(field)
        if column is None:
            return pd.Series(float("nan"), index=df.index)
        return pd.to_numeric(df[column], errors="coerce")

    def score(field: str) -> pd.Series:
        column = fields.get(field)
        return _normalize_score(df[column]) if column is not None else pd.Series(float("nan"), index=df.index)

    working = pd.DataFrame(
        {
            "employee_id": _employee_ids(df, detected),
            "department": _optional_text_series(df, detected, "department", default="Unknown"),
            "performance_score": score("performance_score"),
            "attrition": attrition,
            "engagement_score": score("engagement_score"),
            "gender": _optional_text_series(df, detected, "gender", default="Unknown"),
            "level": _optional_text_series(df, detected, "level", default="Unknown"),
            "salary": _normalize_salary(df[fields["salary"]]) if "salary" in fields else numeric("salary"),
            "training_hours": numeric("training_hours"),
            "work_mode": _optional_text_series(df, detected, "work_mode", default="Unknown"),
            "tenure_years": numeric("tenure_years"),
            "age": numeric("age"),
        }
    ).dropna(subset=["attrition"])

    if len(working) < 20:
        return None

    if not working["training_hours"].isna().all():
        working["training_hours"] = _normalize_training_measure(working["training_hours"])

    has = {
        column: bool(working[column].notna().any())
        for column in ("performance_score", "engagement_score", "salary", "training_hours", "tenure_years", "age")
    }
    has["department"] = "department" in fields
    has["work_mode"] = "work_mode" in fields

    working["performance_group"] = working["performance_score"].map(_performance_group, na_action="ignore")
    working["engagement_group"] = working["engagement_score"].map(_engagement_group, na_action="ignore")
    working["training_group"] = working["training_hours"].apply(_training_group)
    working["work_mode_group"] = working["work_mode"].apply(_normalize_work_mode)

    attrition_by_performance = (
        _rate_table(working.dropna(subset=["performance_group"]), "performance_group", order=["High performer", "Everyone else"])
        if has["performance_score"]
        else {}
    )
    attrition_by_engagement = (
        _rate_table(working.dropna(subset=["engagement_group"]), "engagement_group", order=["Very high engagement", "Mid engagement", "Low engagement"])
        if has["engagement_score"]
        else {}
    )
    attrition_by_work_mode = _rate_table(working, "work_mode_group") if has["work_mode"] else {}
    engagement_by_work_mode = (
        working.groupby("work_mode_group")["engagement_score"].mean().dropna().sort_values(ascending=False)
        if has["work_mode"] and has["engagement_score"]
        else pd.Series(dtype="float64")
    )
    training_frame = working[working["training_group"] != "Unknown"]
    engagement_by_training = (
        training_frame.groupby("training_group")["engagement_score"].mean().reindex(["Under 15 hours", "15-44 hours", "45+ hours"]).fillna(0.0)
        if has["training_hours"] and has["engagement_score"]
        else pd.Series(dtype="float64")
    )
    attrition_by_department = _rate_table(working, "department", limit=12) if has["department"] else {}
    avg_salary_by_level_gender = (
        working.groupby(["level", "gender"])["salary"].mean().reset_index().sort_values(["level", "gender"])
    )

    # Whatever other grouping columns the file has, plus age and tenure bands: where does attrition differ most?
    dimension_frame = df.loc[working.index].drop(columns=[fields["attrition"]])
    named_columns = {fields[field] for field in ("department", "work_mode", "employee_id") if field in fields}
    dimensions = [column for column in dimension_frame.columns if column not in named_columns]
    if has["age"]:
        dimension_frame = dimension_frame.assign(**{"Age band": _band(working["age"], AGE_BANDS)})
        dimensions.append("Age band")
    if has["tenure_years"]:
        dimension_frame = dimension_frame.assign(**{"Tenure band": _band(working["tenure_years"], TENURE_BANDS)})
        dimensions.append("Tenure band")
    attrition_by_dimension = rate_by_dimensions(dimension_frame, working["attrition"], dimensions)
    if has["department"] and len(attrition_by_department) >= 2:
        rates = [metrics["rate"] for metrics in attrition_by_department.values()]
        attrition_by_dimension.append(
            {
                "column": str(fields["department"]),
                "labels": list(attrition_by_department.keys()),
                "values": [round(rate, 2) for rate in rates],
                "counts": [metrics["count"] for metrics in attrition_by_department.values()],
                "spread": max(rates) - min(rates),
            }
        )
    if not any([attrition_by_dimension, attrition_by_performance, attrition_by_engagement, attrition_by_department]):
        return None  # a leave flag with nothing to compare it across

    attrition_by_dimension.sort(key=lambda table: -table["spread"])
    attrition_gaps = [
        {
            "column": table["column"],
            "label": table["labels"][0],
            "rate": table["values"][0],
            "count": table["counts"][0],
            "low_label": table["labels"][-1],
            "low_rate": table["values"][-1],
        }
        for table in attrition_by_dimension[:2]
    ]
    leavers = working[working["attrition"] == 1.0]
    stayers = working[working["attrition"] == 0.0]
    company_attrition = float(working["attrition"].mean() * 100)

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
            "leaver_count": int(len(leavers)),
            "avg_engagement": _mean(working["engagement_score"]),
            "avg_salary": _mean(working["salary"]),
            "high_perf_attrition": _rate(attrition_by_performance, "High performer"),
            "everyone_else_attrition": _rate(attrition_by_performance, "Everyone else"),
            "very_high_engagement_attrition": _rate(attrition_by_engagement, "Very high engagement"),
            "mid_engagement_attrition": _rate(attrition_by_engagement, "Mid engagement"),
            "remote_attrition": _first_matching_metric(attrition_by_work_mode, ["Remote"]),
            "onsite_attrition": _first_matching_metric(attrition_by_work_mode, ["Onsite"]),
            "remote_engagement": _first_matching_average(engagement_by_work_mode, ["Remote"]),
            "onsite_engagement": _first_matching_average(engagement_by_work_mode, ["Onsite"]),
            "low_training_engagement": float(engagement_by_training.get("Under 15 hours", 0.0)),
            "high_training_engagement": float(engagement_by_training.get("45+ hours", 0.0)),
            "customer_support_attrition": _first_matching_metric(attrition_by_department, ["Customer Support", "Support"]),
            "female_vp_salary": _level_gender_salary(avg_salary_by_level_gender, ["VP", "Vice President"], ["female", "f"]),
            "male_vp_salary": _level_gender_salary(avg_salary_by_level_gender, ["VP", "Vice President"], ["male", "m"]),
            "attrition_hotspot": attrition_gaps[0] if attrition_gaps else None,
            "attrition_second_gap": attrition_gaps[1] if len(attrition_gaps) > 1 else None,
            "leaver_tenure": _median(leavers["tenure_years"]),
            "stayer_tenure": _median(stayers["tenure_years"]),
            "leaver_salary": _mean(leavers["salary"]),
            "stayer_salary": _mean(stayers["salary"]),
        },
        "signals": {
            "attrition_by_performance": _table_signal(attrition_by_performance),
            "attrition_by_engagement": _table_signal(attrition_by_engagement),
            "attrition_by_work_mode": _table_signal(attrition_by_work_mode),
            "engagement_by_work_mode": {
                "labels": engagement_by_work_mode.index.tolist(),
                "values": [round(float(value), 2) for value in engagement_by_work_mode.tolist()],
            },
            "engagement_by_training": {
                "labels": engagement_by_training.index.tolist(),
                "values": [round(float(value), 2) for value in engagement_by_training.tolist()],
            },
            "attrition_by_department": _table_signal(attrition_by_department),
            "attrition_by_dimension": [table for table in attrition_by_dimension if table["column"] != str(fields.get("department"))],
            "salary_by_attrition": {
                "labels": ["Left", "Stayed"],
                "values": [round(_mean(leavers["salary"]) or 0.0, 2), round(_mean(stayers["salary"]) or 0.0, 2)],
            },
            "vp_gender_pay": {
                "labels": ["Female VP", "Male VP"],
                "values": [
                    round(float(_level_gender_salary(avg_salary_by_level_gender, ["VP", "Vice President"], ["female", "f"]) or 0.0), 2),
                    round(float(_level_gender_salary(avg_salary_by_level_gender, ["VP", "Vice President"], ["male", "m"]) or 0.0), 2),
                ],
            },
        },
    }


def build_hr_insight_candidates(analysis: dict[str, Any], user_prompt: str = "") -> dict[str, Any]:
    summary = analysis["summary"]
    hotspot = summary["attrition_hotspot"]
    second_gap = summary["attrition_second_gap"]
    # Some files hold one row per employee per year; rates are then per record, not per person.
    unit = "employees" if summary["employee_count"] == analysis["dataset"]["row_count"] else "employee records"
    focus_tags = extract_focus_tags(user_prompt)

    insights = collect_insights([
        lambda: {
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
        lambda: {
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
        lambda: {
            "id": "vp_gender_pay_gap",
            "title": "The VP gender pay gap widens at senior levels",
            "category": "pay_equity",
            "severity": "high",
            "summary": (
                f"Female VPs average ${float(summary['female_vp_salary'] or 0):,.0f} versus ${float(summary['male_vp_salary'] or 0):,.0f} for male VPs."
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
        lambda: {
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
            "condition": summary["high_training_engagement"] > 0 or summary["low_training_engagement"] > 0,
        },
        lambda: {
            "id": "remote_retention_risk",
            "title": "Remote workers are a quiet retention risk",
            "category": "remote",
            "severity": "medium",
            "summary": (
                f"Remote attrition is {float(summary['remote_attrition'] or 0):.1f}% versus {float(summary['onsite_attrition'] or 0):.1f}% onsite, while engagement also runs lower."
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
        lambda: {
            "id": "customer_support_crisis",
            "title": "Customer Support is a structural crisis",
            "category": "department",
            "severity": "high",
            "summary": (
                f"Customer Support attrition runs at {float(summary['customer_support_attrition'] or 0):.1f}% versus a company average of {summary['overall_attrition_rate']:.1f}%."
            ),
            "detail": "That is not a normal staffing fluctuation. It points to a structural problem in role design, management load, or burnout conditions.",
            "metric_label": "Support attrition",
            "metric_value": f"{float(summary['customer_support_attrition'] or 0):.1f}%",
            "metric_sub": "vs company average",
            "tags": ["department", "retention"],
            "section": "workforce_model",
            "priority": 99,
            "condition": summary["customer_support_attrition"] is not None,
        },
        lambda: {
            "id": "attrition_hotspot",
            "title": f"Attrition is concentrated where {hotspot['column']} is {hotspot['label']}",
            "category": "retention",
            "severity": "high",
            "summary": (
                f"{hotspot['rate']:.1f}% of the {hotspot['count']:,} {unit} with {hotspot['column']} = {hotspot['label']} left, "
                f"versus {summary['overall_attrition_rate']:.1f}% overall and {hotspot['low_rate']:.1f}% where it is {hotspot['low_label']}."
            ),
            "detail": "This is the widest attrition gap across any grouping in the file. It is the first place to look for a cause, not proof of one.",
            "metric_label": "Highest-attrition group",
            "metric_value": f"{hotspot['rate']:.1f}%",
            "metric_sub": f"{hotspot['column']}: {hotspot['label']}",
            "tags": ["retention"],
            "section": "retention",
            "priority": 94,
            "condition": hotspot["rate"] - hotspot["low_rate"] >= 5,
        },
        lambda: {
            "id": "attrition_second_gap",
            "title": f"Attrition also differs by {second_gap['column']}",
            "category": "workforce model",
            "severity": "medium",
            "summary": (
                f"Where {second_gap['column']} is {second_gap['label']}, {second_gap['rate']:.1f}% left, "
                f"versus {second_gap['low_rate']:.1f}% where it is {second_gap['low_label']}."
            ),
            "detail": "Two groupings with wide gaps may describe the same people. Check how much they overlap before treating them as separate causes.",
            "metric_label": "Second-widest attrition gap",
            "metric_value": f"{second_gap['rate'] - second_gap['low_rate']:.1f} pts",
            "metric_sub": f"{second_gap['column']}: {second_gap['label']} vs {second_gap['low_label']}",
            "tags": ["retention", "workforce_model"],
            "section": "workforce_model",
            "priority": 87,
            "condition": second_gap["rate"] - second_gap["low_rate"] >= 5,
        },
        lambda: {
            "id": "leavers_tenure_gap",
            "title": "People who leave do so " + ("early" if summary["leaver_tenure"] < summary["stayer_tenure"] else "late") + " in their tenure",
            "category": "retention",
            "severity": "medium",
            "summary": (
                f"Employees who left had a median tenure of {summary['leaver_tenure']:.1f} years, "
                f"versus {summary['stayer_tenure']:.1f} years for those who stayed."
            ),
            "detail": "A gap this size points at a specific stage of the employee lifecycle rather than at attrition in general.",
            "metric_label": "Median tenure of leavers",
            "metric_value": f"{summary['leaver_tenure']:.1f} yrs",
            "metric_sub": f"stayers: {summary['stayer_tenure']:.1f} yrs",
            "tags": ["retention"],
            "section": "retention",
            "priority": 88,
            "condition": abs(summary["leaver_tenure"] - summary["stayer_tenure"]) >= 0.15 * max(summary["stayer_tenure"], 0.5),
        },
        lambda: {
            "id": "leavers_pay_gap",
            "title": "Leavers were paid " + ("less" if summary["leaver_salary"] < summary["stayer_salary"] else "more") + " than those who stayed",
            "category": "compensation",
            "severity": "medium",
            "summary": (
                f"Employees who left averaged ${summary['leaver_salary']:,.0f}, versus ${summary['stayer_salary']:,.0f} for those who stayed."
            ),
            "detail": "Pay differences often track level and tenure, so compare like with like before concluding pay drives exits.",
            "metric_label": "Leaver vs stayer pay",
            "metric_value": f"{(summary['leaver_salary'] / summary['stayer_salary'] - 1) * 100:+.0f}%",
            "metric_sub": "average pay of leavers relative to stayers",
            "tags": ["compensation", "retention"],
            "section": "compensation",
            "priority": 86,
            "condition": abs(summary["leaver_salary"] / summary["stayer_salary"] - 1) >= 0.05,
        },
        lambda: {
            "id": "overall_attrition",
            "title": f"{summary['overall_attrition_rate']:.1f}% of {unit} in this file are exits",
            "category": "retention",
            "severity": "low",
            "summary": f"{summary['leaver_count']:,} of {analysis['dataset']['row_count']:,} {unit} are recorded as having left.",
            "detail": "Whether that is high depends on the period the file covers and the industry; use it as the baseline for the comparisons here.",
            "metric_label": "Overall attrition",
            "metric_value": f"{summary['overall_attrition_rate']:.1f}%",
            "metric_sub": f"{summary['leaver_count']:,} leavers",
            "tags": ["retention"],
            "section": "retention",
            "priority": 85,
        },
    ])

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


def dashboard_section_options(analysis: Optional[dict[str, Any]] = None) -> dict[str, str]:
    """Sections this dataset can fill. Without an analysis, every section the template knows."""
    if not analysis:
        return SECTION_CONFIG
    filled = _build_layout_sections(
        approved_insights=[],
        metric_cards=[],
        analysis=analysis,
        included_sections=list(SECTION_CONFIG),
        show_notes=True,
    )
    available = {section["id"] for section in filled} | {"overview", "notes"}
    return {section: label for section, label in SECTION_CONFIG.items() if section in available}


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
    return detect_columns(df, REQUIRED_COLUMN_ALIASES, OPTIONAL_COLUMN_ALIASES)


def _looks_like_workforce(fields: dict[str, Any]) -> bool:
    """Enough employee-specific columns beside the leave flag to rule out customers or orders."""
    support = sum(1 for field in HR_CONTEXT_FIELDS if field in fields)
    employee_id = fields.get("employee_id")
    if employee_id is not None and any(word in normalize_name(employee_id) for word in ("emp", "worker", "staff")):
        support += 1
    needed = 2 if normalize_name(fields["attrition"]) in WEAK_ATTRITION_NAMES else 1
    return support >= needed


def _band(values: pd.Series, bands: list[tuple[float, float, str]]) -> pd.Series:
    edges = [bands[0][0]] + [upper for _, upper, _ in bands]
    return pd.cut(values, bins=edges, labels=[label for _, _, label in bands], right=False).astype(object)


def _mean(series: pd.Series) -> Optional[float]:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return float(values.mean()) if not values.empty else None


def _median(series: pd.Series) -> Optional[float]:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return float(values.median()) if not values.empty else None


def _rate(table: dict[str, dict[str, float]], label: str) -> Optional[float]:
    return float(table[label]["rate"]) if label in table else None


def _table_signal(table: dict[str, dict[str, float]]) -> dict[str, list[Any]]:
    return {"labels": list(table.keys()), "values": [round(metrics["rate"], 2) for metrics in table.values()]}


def _normalize_score(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().sum() < 0.5 * series.notna().sum():
        # Ratings written as words: "Low", "Fully Meets", "Very_High".
        words = series.map(lambda value: str(value).strip().lower().replace("_", " ") if pd.notna(value) else None)
        return words.map(ORDINAL_WORDS)
    max_value = numeric.dropna().max()
    if pd.isna(max_value):
        return numeric
    if max_value <= 1.0:
        return numeric * 10.0
    if max_value <= 5.0:
        return numeric * 2.5
    return numeric


def _normalize_salary(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")
    normalized = series.astype(str).str.strip().str.lower()
    mapped = normalized.map(
        {
            "low": 45000.0,
            "medium": 70000.0,
            "high": 95000.0,
            "upto 5k": 42000.0,
            "5k-10k": 90000.0,
            "10k-15k": 150000.0,
            "15k+": 210000.0,
        }
    )
    numeric = pd.to_numeric(series, errors="coerce")
    return mapped.where(mapped.notna(), numeric)


def _normalize_training_measure(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    max_value = numeric.dropna().max()
    if pd.isna(max_value):
        return numeric
    if max_value <= 10:
        return numeric * 10.0
    return numeric


def _employee_ids(df: pd.DataFrame, detected: dict[str, dict[str, str]]) -> pd.Series:
    column = detected["optional"].get("employee_id")
    if column is not None:
        return df[column].astype(str).str.strip()
    return pd.Series([f"employee_{index + 1}" for index in range(len(df))], index=df.index)


def _optional_text_series(
    df: pd.DataFrame,
    detected: dict[str, dict[str, str]],
    key: str,
    *,
    default: str,
) -> pd.Series:
    column = detected["optional"].get(key)
    if column is None:
        return pd.Series([default] * len(df), index=df.index)
    return df[column].astype(str).str.strip()


def _optional_numeric_series(df: pd.DataFrame, detected: dict[str, dict[str, str]], key: str) -> pd.Series:
    column = detected["optional"].get(key)
    if column is None:
        return pd.Series([None] * len(df), index=df.index, dtype="float64")
    return pd.to_numeric(df[column], errors="coerce")


def _performance_group(value: float) -> str:
    return "High performer" if float(value) >= 8.0 else "Everyone else"


def _engagement_group(value: float) -> str:
    if float(value) >= 8.0:
        return "Very high engagement"
    if float(value) >= 5.0:
        return "Mid engagement"
    return "Low engagement"


def _training_group(value: float) -> str:
    if pd.isna(value):
        return "Unknown"
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


def _rate_table(
    df: pd.DataFrame,
    column: str,
    order: Optional[list[str]] = None,
    limit: Optional[int] = None,
) -> dict[str, dict[str, float]]:
    grouped = (
        df.groupby(column)
        .agg(attrition_rate=("attrition", "mean"), count=("employee_id", "size"))
        .assign(attrition_rate=lambda frame: frame["attrition_rate"] * 100)
    )
    if order is not None:
        grouped = grouped.reindex(order).dropna(how="all")
    else:
        if limit is not None:
            grouped = grouped.nlargest(limit, "count")
        grouped = grouped.sort_values("attrition_rate", ascending=False)
    return {
        str(index): {"rate": float(row["attrition_rate"]), "count": int(row["count"])}
        for index, row in grouped.fillna(0).iterrows()
    }


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
                        *(
                            [{"label": "Average engagement", "value": f"{summary['avg_engagement']:.2f}", "tone": "positive"}]
                            if summary["avg_engagement"] is not None
                            else []
                        ),
                        *(
                            [{"label": "Average salary", "value": f"${summary['avg_salary']:,.0f}", "tone": "default"}]
                            if summary["avg_salary"] is not None
                            else []
                        ),
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

    section_map["compensation"]["blocks"].append(
        {
            "id": "salary-by-attrition",
            "kind": "chart",
            "chart": {
                "id": "salary-by-attrition",
                "title": "Average pay: leavers vs stayers",
                "subtitle": "Compare within the same level and tenure before reading this as cause.",
                "type": "bar",
                "labels": signals["salary_by_attrition"]["labels"],
                "series": [{"name": "Average salary", "values": signals["salary_by_attrition"]["values"], "color": "#0f766e"}],
                "format": "currency",
            },
        }
    )
    dimension_colors = ["#9a3412", "#0f766e", "#b45309", "#292524"]
    dimension_charts = [
        {
            "id": f"attrition-dimension-{index}",
            "kind": "chart",
            "chart": {
                "id": f"attrition-dimension-{index}",
                "title": f"Attrition by {table['column']}",
                "subtitle": "Share of each group recorded as having left; very small groups are left out.",
                "type": "bar",
                "labels": table["labels"],
                "series": [{"name": "Attrition rate", "values": table["values"], "color": dimension_colors[index % len(dimension_colors)]}],
                "format": "percent",
            },
        }
        for index, table in enumerate(signals["attrition_by_dimension"])
    ]
    section_map["retention"]["blocks"].extend(dimension_charts[:1])
    section_map["workforce_model"]["blocks"][0:0] = dimension_charts[1:]

    layout_sections: list[dict[str, Any]] = []
    for section_id in included_sections:
        if section_id == "notes" and not show_notes:
            continue
        section = section_map.get(section_id)
        if section is not None:
            layout_sections.append(section)
    layout_sections = prune_empty_blocks(layout_sections)
    return layout_sections


def _render_dashboard_html(payload: dict[str, Any]) -> str:
    json_payload = script_json(payload, default=_json_default)
    template = html_template(
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
      const payload = {{ json_payload }};
      const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (char) => (
        { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char]
      ));
      const root = document.getElementById("app");
      const metricCards = payload.metric_cards.map((card) => `
        <div class="metric">
          <div class="kicker">${esc(card.label)}</div>
          <h3 style="margin-top: 8px;">${esc(card.value)}</h3>
          <p style="color:#78716c;">${esc(card.sub)}</p>
        </div>
      `).join("");
      const insightCards = payload.approved_insights.map((insight) => `
        <div class="card">
          <div class="kicker">${esc(insight.category)}</div>
          <h3 style="margin-top: 8px;">${esc(insight.title)}</h3>
          <p>${esc(insight.summary)}</p>
          <p style="color:#78716c;">${esc(insight.detail)}</p>
        </div>
      `).join("");
      root.innerHTML = `
        <section class="hero">
          <div class="kicker">HR workforce insight dashboard</div>
          <h1 style="margin-top: 12px;">${esc(payload.title)}</h1>
          <p style="color:#78716c;">${esc(payload.subtitle)}</p>
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
