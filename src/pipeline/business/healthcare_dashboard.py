from __future__ import annotations

import json
import re
from typing import Any, Optional

import pandas as pd
from jinja2 import Template

from pipeline.common.contracts import PipelineContext


OUTCOMES_REQUIRED_COLUMN_ALIASES = {
    "patient_id": ["patient_id", "patient", "member_id", "id"],
    "adherence": ["medication_adherence", "adherence", "adherence_level"],
    "readmitted": ["readmitted", "readmission", "is_readmitted", "readmit_flag"],
    "visit_type": ["visit_type", "care_mode", "encounter_type"],
    "satisfaction": ["satisfaction_score", "satisfaction", "experience_score"],
    "follow_up": ["follow_up_scheduled", "followup_scheduled", "follow_up", "scheduled_follow_up"],
    "insurance": ["insurance_type", "insurance", "payer_type", "payer"],
    "cost": ["cost", "episode_cost", "total_cost", "charges"],
    "race": ["race", "ethnicity_group", "patient_race"],
}

OUTCOMES_OPTIONAL_COLUMN_ALIASES = {
    "diagnosis": ["diagnosis", "primary_diagnosis", "condition"],
    "age_group": ["age_group", "age_band"],
}

ADMISSIONS_REQUIRED_COLUMN_ALIASES = {
    "patient_id": ["patient_id", "patient", "member_id", "id", "name"],
    "age": ["age"],
    "gender": ["gender", "sex"],
    "condition": ["medical condition", "medical_condition", "diagnosis", "primary_diagnosis", "condition"],
    "admission_date": ["date of admission", "admission_date", "date_admitted"],
    "discharge_date": ["discharge date", "discharge_date", "date_discharged"],
    "insurance": ["insurance provider", "insurance_type", "insurance", "payer_type", "payer"],
    "cost": ["billing amount", "cost", "episode_cost", "total_cost", "charges"],
    "admission_type": ["admission type", "admission_type", "visit_type", "care_mode"],
    "medication": ["medication", "drug"],
    "test_results": ["test results", "test_results", "lab_result", "result"],
}

ADMISSIONS_OPTIONAL_COLUMN_ALIASES = {
    "doctor": ["doctor", "attending_physician"],
    "hospital": ["hospital", "facility"],
    "blood_type": ["blood type", "blood_type"],
}

INSURANCE_REQUIRED_COLUMN_ALIASES = {
    "age": ["age"],
    "gender": ["sex", "gender"],
    "bmi": ["bmi", "body_mass_index"],
    "children": ["children", "dependents"],
    "smoker": ["smoker", "smoking_status", "tobacco_use"],
    "region": ["region", "market", "geography"],
    "charges": ["charges", "cost", "billing amount", "billing_amount"],
}

FOCUS_KEYWORDS = {
    "adherence": ["adherence", "medication", "compliance"],
    "telehealth": ["telehealth", "virtual", "visit type", "visit mode"],
    "follow_up": ["follow-up", "follow up", "scheduling", "appointment"],
    "cost": ["cost", "self-pay", "payer", "insurance"],
    "equity": ["equity", "race", "ethnicity", "disparity"],
    "readmission": ["readmission", "readmit", "outcomes"],
    "operations": ["admission", "discharge", "length of stay", "throughput", "operations"],
    "diagnosis": ["diagnosis", "condition", "disease", "medical condition"],
    "billing": ["billing", "charges", "cost", "revenue", "insurance provider"],
    "risk_factors": ["smoker", "smoking", "bmi", "obesity", "risk"],
    "pricing": ["pricing", "charges", "cost", "region"],
    "demographics": ["age", "gender", "children", "region"],
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

OUTCOMES_SECTION_CONFIG = {
    "overview": "KPI cards and key insights",
    "adherence": "Charts: adherence and outcomes",
    "care_delivery": "Charts: telehealth and follow-up",
    "equity": "Charts: equity and payer mix",
    "costs": "Charts: cost and support risk",
    "notes": "Insight notes",
}

ADMISSIONS_SECTION_CONFIG = {
    "overview": "KPI cards and key insights",
    "utilization": "Charts: admissions and length of stay",
    "diagnoses": "Charts: condition patterns",
    "billing": "Charts: billing and payer mix",
    "notes": "Insight notes",
}

INSURANCE_SECTION_CONFIG = {
    "overview": "KPI cards and key insights",
    "risk_factors": "Charts: smoking and BMI risk",
    "demographics": "Charts: age, gender, and family mix",
    "pricing": "Charts: regional and pricing spread",
    "notes": "Insight notes",
}


def analyze_healthcare_context(context: PipelineContext) -> Optional[dict[str, Any]]:
    df = context.clean_df if context.clean_df is not None else context.raw_df
    if df is None or df.empty:
        return None

    return (
        _analyze_outcomes_context(df)
        or _analyze_admissions_context(df)
        or _analyze_insurance_context(df)
    )


def _analyze_outcomes_context(df: pd.DataFrame) -> Optional[dict[str, Any]]:
    detected = _detect_columns(df, OUTCOMES_REQUIRED_COLUMN_ALIASES, OUTCOMES_OPTIONAL_COLUMN_ALIASES)
    if detected is None:
        return None

    working = pd.DataFrame(
        {
            "patient_id": df[detected["required"]["patient_id"]].astype(str).str.strip(),
            "adherence": df[detected["required"]["adherence"]].astype(str).str.strip(),
            "readmitted": _normalize_binary(df[detected["required"]["readmitted"]]),
            "visit_type": df[detected["required"]["visit_type"]].astype(str).str.strip(),
            "satisfaction": pd.to_numeric(df[detected["required"]["satisfaction"]], errors="coerce"),
            "follow_up": _normalize_binary(df[detected["required"]["follow_up"]]),
            "insurance": df[detected["required"]["insurance"]].astype(str).str.strip(),
            "cost": pd.to_numeric(df[detected["required"]["cost"]], errors="coerce"),
            "race": df[detected["required"]["race"]].astype(str).str.strip(),
        }
    ).dropna(subset=["patient_id", "satisfaction", "cost"])

    if len(working) < 20:
        return None

    if detected["optional"].get("diagnosis"):
        working["diagnosis"] = df.loc[working.index, detected["optional"]["diagnosis"]].astype(str).str.strip()
    else:
        working["diagnosis"] = "Unknown"

    if detected["optional"].get("age_group"):
        working["age_group"] = df.loc[working.index, detected["optional"]["age_group"]].astype(str).str.strip()
    else:
        working["age_group"] = "Unknown"

    working["adherence_group"] = working["adherence"].map(_normalize_adherence)
    if working["adherence_group"].eq("Unknown").all():
        return None

    working["follow_up_label"] = working["follow_up"].map({1.0: "Follow-up scheduled", 0.0: "No follow-up"}).fillna("No follow-up")

    readmission_by_adherence = _rate_table(working, "adherence_group", order=["High adherence", "Medium adherence", "Low adherence"])
    visit_type_readmission = _rate_table(working, "visit_type")
    visit_type_satisfaction = working.groupby("visit_type")["satisfaction"].mean().sort_values(ascending=False)
    follow_up_readmission = _rate_table(working, "follow_up_label", order=["Follow-up scheduled", "No follow-up"])
    insurance_cost = working.groupby("insurance")["cost"].mean().sort_values(ascending=False)
    insurance_low_adherence = (
        working.assign(low_adherence=working["adherence_group"].eq("Low adherence").astype(float))
        .groupby("insurance")["low_adherence"]
        .mean()
        .mul(100)
        .sort_values(ascending=False)
    )
    race_readmission = _rate_table(working, "race")

    high_adherence_rate = _value_or_zero(readmission_by_adherence["High adherence"]["rate"])
    low_adherence_rate = _value_or_zero(readmission_by_adherence["Low adherence"]["rate"])
    telehealth_rate = _first_matching_metric(visit_type_readmission, ["telehealth", "virtual"])
    in_person_rate = _first_matching_metric(visit_type_readmission, ["in-person", "in person", "clinic", "office"])
    telehealth_satisfaction = _first_matching_average(visit_type_satisfaction, ["telehealth", "virtual"])
    in_person_satisfaction = _first_matching_average(visit_type_satisfaction, ["in-person", "in person", "clinic", "office"])
    scheduled_follow_up_rate = _value_or_zero(follow_up_readmission["Follow-up scheduled"]["rate"])
    unscheduled_follow_up_rate = _value_or_zero(follow_up_readmission["No follow-up"]["rate"])
    self_pay_cost = _first_matching_average(insurance_cost, ["self-pay", "self pay"])
    medicare_cost = _first_matching_average(insurance_cost, ["medicare"])
    self_pay_low_adherence = _first_matching_average(insurance_low_adherence, ["self-pay", "self pay"])
    highest_other_low_adherence = _highest_other_value(insurance_low_adherence, ["self-pay", "self pay"])
    white_readmission = _first_matching_metric(race_readmission, ["white"])
    black_readmission = _first_matching_metric(race_readmission, ["black"])
    hispanic_readmission = _first_matching_metric(race_readmission, ["hispanic", "latino"])

    return {
        "kind": "healthcare_medical",
        "profile": "outcomes",
        "column_map": detected,
        "dataset": {
            "row_count": int(len(working)),
            "column_count": int(len(df.columns)),
            "input_columns": list(df.columns),
        },
        "summary": {
            "patient_count": int(working["patient_id"].nunique()),
            "overall_readmission_rate": float(working["readmitted"].mean() * 100),
            "avg_satisfaction": float(working["satisfaction"].mean()),
            "avg_cost": float(working["cost"].mean()),
            "high_adherence_readmission": high_adherence_rate,
            "low_adherence_readmission": low_adherence_rate,
            "telehealth_readmission": telehealth_rate,
            "in_person_readmission": in_person_rate,
            "telehealth_satisfaction": telehealth_satisfaction,
            "in_person_satisfaction": in_person_satisfaction,
            "scheduled_follow_up_readmission": scheduled_follow_up_rate,
            "unscheduled_follow_up_readmission": unscheduled_follow_up_rate,
            "self_pay_cost": self_pay_cost,
            "medicare_cost": medicare_cost,
            "self_pay_low_adherence": self_pay_low_adherence,
            "highest_other_low_adherence": highest_other_low_adherence,
            "white_readmission": white_readmission,
            "black_readmission": black_readmission,
            "hispanic_readmission": hispanic_readmission,
        },
        "signals": {
            "adherence_readmission": {
                "labels": list(readmission_by_adherence.keys()),
                "values": [round(metrics["rate"], 2) for metrics in readmission_by_adherence.values()],
            },
            "visit_type_readmission": {
                "labels": list(visit_type_readmission.keys()),
                "values": [round(metrics["rate"], 2) for metrics in visit_type_readmission.values()],
            },
            "visit_type_satisfaction": {
                "labels": visit_type_satisfaction.index.tolist(),
                "values": [round(float(value), 2) for value in visit_type_satisfaction.tolist()],
            },
            "follow_up_readmission": {
                "labels": list(follow_up_readmission.keys()),
                "values": [round(metrics["rate"], 2) for metrics in follow_up_readmission.values()],
            },
            "insurance_cost": {
                "labels": insurance_cost.index.tolist(),
                "values": [round(float(value), 2) for value in insurance_cost.tolist()],
            },
            "insurance_low_adherence": {
                "labels": insurance_low_adherence.index.tolist(),
                "values": [round(float(value), 2) for value in insurance_low_adherence.tolist()],
            },
            "race_readmission": {
                "labels": list(race_readmission.keys()),
                "values": [round(metrics["rate"], 2) for metrics in race_readmission.values()],
            },
        },
    }


def build_healthcare_insight_candidates(analysis: dict[str, Any], user_prompt: str = "") -> dict[str, Any]:
    profile = analysis.get("profile", "outcomes")
    if profile == "admissions":
        return _build_admissions_insight_candidates(analysis, user_prompt)
    if profile == "insurance_risk":
        return _build_insurance_insight_candidates(analysis, user_prompt)

    summary = analysis["summary"]
    focus_tags = extract_focus_tags(user_prompt)

    insights = [
        {
            "id": "adherence_beats_everything",
            "title": "Adherence beats everything else",
            "category": "adherence",
            "severity": "high",
            "summary": (
                f"Low-adherence patients readmit at {summary['low_adherence_readmission']:.1f}% "
                f"versus {summary['high_adherence_readmission']:.1f}% for high-adherence patients."
            ),
            "detail": "That gap is larger than the spread from most diagnosis, age, or insurance segments, which makes adherence programs the most leveraged intervention in the dataset.",
            "metric_label": "Readmission gap",
            "metric_value": f"{summary['low_adherence_readmission'] - summary['high_adherence_readmission']:.1f} pts",
            "metric_sub": "low vs high adherence",
            "tags": ["adherence", "readmission"],
            "section": "adherence",
            "priority": 100,
        },
        {
            "id": "telehealth_quietly_winning",
            "title": "Telehealth is quietly winning",
            "category": "telehealth",
            "severity": "medium",
            "summary": (
                f"Telehealth readmission is {summary['telehealth_readmission']:.1f}% versus {summary['in_person_readmission']:.1f}% for in-person care."
            ),
            "detail": (
                f"Satisfaction is also {summary['telehealth_satisfaction'] - summary['in_person_satisfaction']:+.2f} points higher, "
                "which undercuts the idea that virtual care is a lower-quality substitute."
            ),
            "metric_label": "Telehealth advantage",
            "metric_value": f"{summary['in_person_readmission'] - summary['telehealth_readmission']:.1f} pts",
            "metric_sub": "readmission reduction",
            "tags": ["telehealth", "readmission"],
            "section": "care_delivery",
            "priority": 95,
            "condition": summary["telehealth_readmission"] is not None and summary["in_person_readmission"] is not None,
        },
        {
            "id": "follow_up_placebo",
            "title": "Scheduling follow-ups does little on its own",
            "category": "follow_up",
            "severity": "medium",
            "summary": (
                f"Readmission stays at {summary['scheduled_follow_up_readmission']:.1f}% with follow-up scheduling "
                f"versus {summary['unscheduled_follow_up_readmission']:.1f}% without it."
            ),
            "detail": "Booking the appointment without moving adherence or support behaviors looks like a placebo intervention in this dataset.",
            "metric_label": "Follow-up delta",
            "metric_value": f"{abs(summary['scheduled_follow_up_readmission'] - summary['unscheduled_follow_up_readmission']):.1f} pts",
            "metric_sub": "scheduled vs not scheduled",
            "tags": ["follow_up", "readmission"],
            "section": "care_delivery",
            "priority": 90,
        },
        {
            "id": "self_pay_system_failure",
            "title": "Self-pay patients look like a system failure",
            "category": "cost",
            "severity": "high",
            "summary": (
                f"Self-pay patients average ${summary['self_pay_cost']:,.0f} in cost versus ${summary['medicare_cost']:,.0f} for Medicare, "
                f"but low adherence spikes to {summary['self_pay_low_adherence']:.1f}%."
            ),
            "detail": "That combination of high cost, low support, and poor adherence makes self-pay the clearest intervention gap in the payer mix.",
            "metric_label": "Self-pay low adherence",
            "metric_value": f"{summary['self_pay_low_adherence']:.1f}%",
            "metric_sub": "highest-risk payer group",
            "tags": ["cost", "adherence"],
            "section": "costs",
            "priority": 98,
            "condition": summary["self_pay_cost"] is not None and summary["self_pay_low_adherence"] is not None,
        },
        {
            "id": "equity_gap_measurable",
            "title": "The equity gap is measurable",
            "category": "equity",
            "severity": "medium",
            "summary": (
                f"Black and Hispanic patients readmit at {summary['black_readmission']:.1f}% and {summary['hispanic_readmission']:.1f}%, "
                f"versus {summary['white_readmission']:.1f}% for White patients."
            ),
            "detail": "The absolute difference is not huge, but it is consistent enough to justify a dedicated investigation rather than being dismissed as noise.",
            "metric_label": "Equity gap",
            "metric_value": f"{max(summary['black_readmission'], summary['hispanic_readmission']) - summary['white_readmission']:.1f} pts",
            "metric_sub": "vs White patients",
            "tags": ["equity", "readmission"],
            "section": "equity",
            "priority": 92,
            "condition": summary["white_readmission"] is not None and summary["black_readmission"] is not None and summary["hispanic_readmission"] is not None,
        },
    ]

    filtered = [item for item in insights if item.get("condition", True)]
    prompt_terms = extract_prompt_terms(user_prompt)
    for item in filtered:
        focus_bonus = _instruction_bonus(item, focus_tags, prompt_terms)
        item["score"] = item["priority"] + focus_bonus
        item["recommended"] = item["score"] >= 85
    filtered.sort(key=lambda item: (-item["score"], item["title"]))
    return {"insights": filtered, "focus_tags": focus_tags}


def _build_admissions_insight_candidates(analysis: dict[str, Any], user_prompt: str = "") -> dict[str, Any]:
    summary = analysis["summary"]
    focus_tags = extract_focus_tags(user_prompt)

    insights = [
        {
            "id": "admission_type_cost_flat",
            "title": "Admission type barely changes cost",
            "category": "operations",
            "severity": "medium",
            "summary": (
                f"Elective, emergency, and urgent visits cluster between ${summary['lowest_admission_cost']:,.0f} and "
                f"${summary['highest_admission_cost']:,.0f} on average."
            ),
            "detail": "That weak spread suggests operational cost is being driven more by the underlying case mix than by the labeled admission urgency itself.",
            "metric_label": "Admission cost spread",
            "metric_value": f"${summary['admission_cost_spread']:,.0f}",
            "metric_sub": "highest vs lowest admission type",
            "tags": ["operations", "billing"],
            "section": "utilization",
            "priority": 94,
        },
        {
            "id": "length_of_stay_flat",
            "title": "Length of stay is operationally flat",
            "category": "operations",
            "severity": "medium",
            "summary": (
                f"Average length of stay stays tightly clustered at {summary['avg_length_of_stay']:.1f} days, "
                f"with only {summary['length_of_stay_spread']:.2f} days between the shortest and longest admission types."
            ),
            "detail": "That usually means the hospital is running a standardized throughput pattern rather than one strongly shaped by admission category.",
            "metric_label": "Length-of-stay spread",
            "metric_value": f"{summary['length_of_stay_spread']:.2f} days",
            "metric_sub": "highest vs lowest admission type",
            "tags": ["operations"],
            "section": "utilization",
            "priority": 91,
        },
        {
            "id": "obesity_cost_signal",
            "title": "Obesity is the most expensive condition in the file",
            "category": "diagnosis",
            "severity": "high",
            "summary": (
                f"{summary['top_condition']} averages ${summary['top_condition_cost']:,.0f} per encounter, "
                f"leading the condition table by ${summary['condition_cost_gap']:,.0f}."
            ),
            "detail": "That makes this condition the clearest high-cost diagnosis cluster in the dataset, even before looking at patient-level complexity.",
            "metric_label": "Condition cost leader",
            "metric_value": f"${summary['top_condition_cost']:,.0f}",
            "metric_sub": summary["top_condition"],
            "tags": ["diagnosis", "billing"],
            "section": "diagnoses",
            "priority": 99,
        },
        {
            "id": "diagnostic_ambiguity_signal",
            "title": "Inconclusive results are too common to ignore",
            "category": "diagnosis",
            "severity": "medium",
            "summary": f"Inconclusive test results make up {summary['inconclusive_rate']:.1f}% of the file.",
            "detail": "A one-third inconclusive share points to diagnostic ambiguity or repeat-work risk that would matter operationally even if gross billing stays stable.",
            "metric_label": "Inconclusive result share",
            "metric_value": f"{summary['inconclusive_rate']:.1f}%",
            "metric_sub": "of all recorded test outcomes",
            "tags": ["diagnosis", "operations"],
            "section": "diagnoses",
            "priority": 93,
        },
        {
            "id": "payer_pricing_flat",
            "title": "Payer pricing is surprisingly flat",
            "category": "billing",
            "severity": "medium",
            "summary": (
                f"The spread between the highest and lowest average insurer billing is only ${summary['insurance_cost_spread']:,.0f}, "
                f"with {summary['highest_cost_insurer']} leading."
            ),
            "detail": "That points away from payer mix as the dominant cost driver and more toward provider-side or case-mix consistency across the book.",
            "metric_label": "Insurance billing spread",
            "metric_value": f"${summary['insurance_cost_spread']:,.0f}",
            "metric_sub": "highest vs lowest insurer average",
            "tags": ["billing", "cost"],
            "section": "billing",
            "priority": 92,
        },
    ]

    prompt_terms = extract_prompt_terms(user_prompt)
    for item in insights:
        focus_bonus = _instruction_bonus(item, focus_tags, prompt_terms)
        item["score"] = item["priority"] + focus_bonus
        item["recommended"] = item["score"] >= 85
    insights.sort(key=lambda item: (-item["score"], item["title"]))
    return {"insights": insights, "focus_tags": focus_tags}


def _build_insurance_insight_candidates(analysis: dict[str, Any], user_prompt: str = "") -> dict[str, Any]:
    summary = analysis["summary"]
    focus_tags = extract_focus_tags(user_prompt)

    insights = [
        {
            "id": "smoking_cost_wall",
            "title": "Smoking dominates the cost structure",
            "category": "risk_factors",
            "severity": "high",
            "summary": (
                f"Smokers average ${summary['smoker_avg_charge']:,.0f} in charges versus ${summary['non_smoker_avg_charge']:,.0f} "
                f"for non-smokers."
            ),
            "detail": "The pricing gap is so large that smoking acts like the primary segmentation axis, overwhelming the contribution from most demographic variables.",
            "metric_label": "Smoker multiplier",
            "metric_value": f"{summary['smoker_multiplier']:.1f}x",
            "metric_sub": "smoker vs non-smoker average charges",
            "tags": ["risk_factors", "pricing", "cost"],
            "section": "risk_factors",
            "priority": 100,
        },
        {
            "id": "obesity_cost_compounder",
            "title": "Obesity is a quiet cost compounder",
            "category": "risk_factors",
            "severity": "medium",
            "summary": (
                f"Obese members average ${summary['obese_avg_charge']:,.0f}, versus ${summary['normal_bmi_avg_charge']:,.0f} "
                f"for the normal-BMI band."
            ),
            "detail": "BMI does not hit as hard as smoking, but it still creates a durable cost premium that is easy to miss in overall averages.",
            "metric_label": "Obesity surcharge",
            "metric_value": f"${summary['obese_charge_gap']:,.0f}",
            "metric_sub": "obese vs normal BMI average charges",
            "tags": ["risk_factors", "cost"],
            "section": "risk_factors",
            "priority": 94,
        },
        {
            "id": "region_is_not_the_story",
            "title": "Region is not the real story",
            "category": "pricing",
            "severity": "medium",
            "summary": (
                f"{summary['highest_cost_region']} is the most expensive region at ${summary['highest_region_charge']:,.0f}, "
                f"but the full regional spread is only ${summary['region_charge_spread']:,.0f}."
            ),
            "detail": "Compared with the smoking gap, geography looks secondary. Regional pricing differences are real, but they are not the dominant narrative in this file.",
            "metric_label": "Regional spread",
            "metric_value": f"${summary['region_charge_spread']:,.0f}",
            "metric_sub": "highest vs lowest region average",
            "tags": ["pricing", "demographics"],
            "section": "pricing",
            "priority": 89,
        },
        {
            "id": "gender_gap_is_small",
            "title": "The gender cost gap is modest",
            "category": "demographics",
            "severity": "low",
            "summary": (
                f"Male members average ${summary['male_avg_charge']:,.0f} in charges versus ${summary['female_avg_charge']:,.0f} for female members."
            ),
            "detail": "There is a difference, but it is far smaller than the risk-factor-driven gaps. Gender is not the lever this file is shouting about.",
            "metric_label": "Gender charge gap",
            "metric_value": f"${summary['gender_charge_gap']:,.0f}",
            "metric_sub": "male vs female average charges",
            "tags": ["demographics", "cost"],
            "section": "demographics",
            "priority": 83,
        },
        {
            "id": "age_matters_but_not_enough",
            "title": "Age matters, but not enough to explain the whole file",
            "category": "demographics",
            "severity": "medium",
            "summary": f"Age and charges only correlate at {summary['age_charge_corr']:.2f}, which is meaningful but not dominant.",
            "detail": "Age clearly matters, but it does not explain the cost profile nearly as strongly as smoking or BMI. The risk story is multivariate, not simply age-led.",
            "metric_label": "Age-charge correlation",
            "metric_value": f"{summary['age_charge_corr']:.2f}",
            "metric_sub": "Pearson correlation",
            "tags": ["demographics", "pricing"],
            "section": "demographics",
            "priority": 86,
        },
    ]

    prompt_terms = extract_prompt_terms(user_prompt)
    for item in insights:
        focus_bonus = _instruction_bonus(item, focus_tags, prompt_terms)
        item["score"] = item["priority"] + focus_bonus
        item["recommended"] = item["score"] >= 85
    insights.sort(key=lambda item: (-item["score"], item["title"]))
    return {"insights": insights, "focus_tags": focus_tags}


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
        analysis = analyze_healthcare_context(context)
    if analysis is None:
        return None

    insight_bundle = build_healthcare_insight_candidates(analysis, user_prompt)
    insights = insight_bundle["insights"]
    approved_set = set(approved_insight_ids or [item["id"] for item in insights if item.get("recommended", True)])
    approved = [item for item in insights if item["id"] in approved_set] or insights[:4]

    settings = settings or {}
    included_sections = settings.get("included_sections") or _default_sections(approved, analysis)
    title = settings.get("title") or "Healthcare Hidden Insights"
    subtitle = settings.get("subtitle") or "Approved care delivery and patient outcome narrative"
    metric_count = int(settings.get("metric_count", 4))
    show_notes = bool(settings.get("show_notes", True))

    payload = {
        "kind": "healthcare_medical",
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
            included_sections=list(_section_config_for_profile(analysis.get("profile")).keys()),
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
        "kind": "healthcare_medical",
        "title": title,
        "html": html,
        "height": height,
        "payload": payload,
        "blueprint": payload,
        "download_name": "healthcare_insights_dashboard.html",
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


def dashboard_section_options(profile: Optional[str] = None) -> dict[str, str]:
    return _section_config_for_profile(profile)


def _section_config_for_profile(profile: Optional[str]) -> dict[str, str]:
    if profile == "admissions":
        return ADMISSIONS_SECTION_CONFIG
    if profile == "insurance_risk":
        return INSURANCE_SECTION_CONFIG
    return OUTCOMES_SECTION_CONFIG


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


def _analyze_admissions_context(df: pd.DataFrame) -> Optional[dict[str, Any]]:
    detected = _detect_columns(df, ADMISSIONS_REQUIRED_COLUMN_ALIASES, ADMISSIONS_OPTIONAL_COLUMN_ALIASES)
    if detected is None:
        return None

    working = pd.DataFrame(
        {
            "patient_id": df[detected["required"]["patient_id"]].astype(str).str.strip(),
            "age": pd.to_numeric(df[detected["required"]["age"]], errors="coerce"),
            "gender": df[detected["required"]["gender"]].astype(str).str.strip(),
            "condition": df[detected["required"]["condition"]].astype(str).str.strip(),
            "admission_date": pd.to_datetime(df[detected["required"]["admission_date"]], errors="coerce"),
            "discharge_date": pd.to_datetime(df[detected["required"]["discharge_date"]], errors="coerce"),
            "insurance": df[detected["required"]["insurance"]].astype(str).str.strip(),
            "cost": pd.to_numeric(df[detected["required"]["cost"]], errors="coerce"),
            "admission_type": df[detected["required"]["admission_type"]].astype(str).str.strip(),
            "medication": df[detected["required"]["medication"]].astype(str).str.strip(),
            "test_results": df[detected["required"]["test_results"]].astype(str).str.strip(),
        }
    ).dropna(subset=["patient_id", "admission_date", "discharge_date", "cost"])

    if len(working) < 20:
        return None

    working["length_of_stay"] = (working["discharge_date"] - working["admission_date"]).dt.days
    working = working[working["length_of_stay"].notna() & (working["length_of_stay"] >= 0)].copy()
    if len(working) < 20:
        return None

    billing_by_admission = working.groupby("admission_type")["cost"].mean().sort_values(ascending=False)
    los_by_admission = working.groupby("admission_type")["length_of_stay"].mean().sort_values(ascending=False)
    billing_by_condition = working.groupby("condition")["cost"].mean().sort_values(ascending=False)
    insurance_billing = working.groupby("insurance")["cost"].mean().sort_values(ascending=False)
    test_result_share = working["test_results"].value_counts(normalize=True).mul(100).sort_values(ascending=False)
    condition_mix = working["condition"].value_counts().sort_values(ascending=False)

    top_condition = str(billing_by_condition.index[0])
    top_condition_cost = float(billing_by_condition.iloc[0])
    bottom_condition_cost = float(billing_by_condition.iloc[-1])
    highest_admission_cost = float(billing_by_admission.max())
    lowest_admission_cost = float(billing_by_admission.min())
    highest_los = float(los_by_admission.max())
    lowest_los = float(los_by_admission.min())
    highest_cost_insurer = str(insurance_billing.index[0])
    highest_insurer_cost = float(insurance_billing.iloc[0])
    lowest_insurer_cost = float(insurance_billing.iloc[-1])
    inconclusive_rate = float(test_result_share.get("Inconclusive", 0.0))

    return {
        "kind": "healthcare_medical",
        "profile": "admissions",
        "column_map": detected,
        "dataset": {
            "row_count": int(len(working)),
            "column_count": int(len(df.columns)),
            "input_columns": list(df.columns),
        },
        "summary": {
            "patient_count": int(working["patient_id"].nunique()),
            "avg_length_of_stay": float(working["length_of_stay"].mean()),
            "avg_billing": float(working["cost"].mean()),
            "highest_admission_cost": highest_admission_cost,
            "lowest_admission_cost": lowest_admission_cost,
            "admission_cost_spread": highest_admission_cost - lowest_admission_cost,
            "length_of_stay_spread": highest_los - lowest_los,
            "top_condition": top_condition,
            "top_condition_cost": top_condition_cost,
            "condition_cost_gap": top_condition_cost - bottom_condition_cost,
            "inconclusive_rate": inconclusive_rate,
            "highest_cost_insurer": highest_cost_insurer,
            "insurance_cost_spread": highest_insurer_cost - lowest_insurer_cost,
        },
        "signals": {
            "billing_by_admission_type": {
                "labels": billing_by_admission.index.tolist(),
                "values": [round(float(value), 2) for value in billing_by_admission.tolist()],
            },
            "length_of_stay_by_admission_type": {
                "labels": los_by_admission.index.tolist(),
                "values": [round(float(value), 2) for value in los_by_admission.tolist()],
            },
            "billing_by_condition": {
                "labels": billing_by_condition.head(6).index.tolist(),
                "values": [round(float(value), 2) for value in billing_by_condition.head(6).tolist()],
            },
            "test_results_share": {
                "labels": test_result_share.index.tolist(),
                "values": [round(float(value), 2) for value in test_result_share.tolist()],
            },
            "billing_by_insurance": {
                "labels": insurance_billing.index.tolist(),
                "values": [round(float(value), 2) for value in insurance_billing.tolist()],
            },
            "condition_mix": {
                "labels": condition_mix.index.tolist(),
                "values": [int(value) for value in condition_mix.tolist()],
            },
        },
    }


def _analyze_insurance_context(df: pd.DataFrame) -> Optional[dict[str, Any]]:
    detected = _detect_columns(df, INSURANCE_REQUIRED_COLUMN_ALIASES)
    if detected is None:
        return None

    working = pd.DataFrame(
        {
            "age": pd.to_numeric(df[detected["required"]["age"]], errors="coerce"),
            "gender": df[detected["required"]["gender"]].astype(str).str.strip(),
            "bmi": pd.to_numeric(df[detected["required"]["bmi"]], errors="coerce"),
            "children": pd.to_numeric(df[detected["required"]["children"]], errors="coerce"),
            "smoker": df[detected["required"]["smoker"]].astype(str).str.strip(),
            "region": df[detected["required"]["region"]].astype(str).str.strip(),
            "charges": pd.to_numeric(df[detected["required"]["charges"]], errors="coerce"),
        }
    ).dropna(subset=["age", "bmi", "children", "charges"])

    if len(working) < 20:
        return None

    working["smoker_group"] = working["smoker"].astype(str).str.strip().str.lower().map(
        lambda value: "Smoker" if value in {"yes", "true", "1", "smoker"} else "Non-smoker"
    )
    working["bmi_band"] = pd.cut(
        working["bmi"],
        bins=[0, 25, 30, float("inf")],
        labels=["Normal BMI", "Overweight", "Obese"],
        include_lowest=True,
    )

    charges_by_smoker = working.groupby("smoker_group")["charges"].mean().reindex(["Smoker", "Non-smoker"]).dropna()
    charges_by_bmi = working.groupby("bmi_band", observed=False)["charges"].mean().dropna()
    charges_by_region = working.groupby("region")["charges"].mean().sort_values(ascending=False)
    charges_by_gender = working.groupby("gender")["charges"].mean().sort_values(ascending=False)
    charges_by_children = working.groupby("children")["charges"].mean().sort_index()

    smoker_avg = float(charges_by_smoker.get("Smoker", 0.0))
    non_smoker_avg = float(charges_by_smoker.get("Non-smoker", 0.0))
    obese_avg = float(charges_by_bmi.get("Obese", 0.0))
    normal_avg = float(charges_by_bmi.get("Normal BMI", 0.0))
    male_avg = _first_matching_average(charges_by_gender, ["male"])
    female_avg = _first_matching_average(charges_by_gender, ["female"])
    highest_region = str(charges_by_region.index[0])
    highest_region_charge = float(charges_by_region.iloc[0])
    lowest_region_charge = float(charges_by_region.iloc[-1])

    return {
        "kind": "healthcare_medical",
        "profile": "insurance_risk",
        "column_map": detected,
        "dataset": {
            "row_count": int(len(working)),
            "column_count": int(len(df.columns)),
            "input_columns": list(df.columns),
        },
        "summary": {
            "member_count": int(len(working)),
            "avg_charges": float(working["charges"].mean()),
            "smoker_avg_charge": smoker_avg,
            "non_smoker_avg_charge": non_smoker_avg,
            "smoker_multiplier": smoker_avg / non_smoker_avg if non_smoker_avg else 0.0,
            "obese_avg_charge": obese_avg,
            "normal_bmi_avg_charge": normal_avg,
            "obese_charge_gap": obese_avg - normal_avg,
            "highest_cost_region": highest_region,
            "highest_region_charge": highest_region_charge,
            "region_charge_spread": highest_region_charge - lowest_region_charge,
            "male_avg_charge": float(male_avg or 0.0),
            "female_avg_charge": float(female_avg or 0.0),
            "gender_charge_gap": abs(float(male_avg or 0.0) - float(female_avg or 0.0)),
            "age_charge_corr": float(working[["age", "charges"]].corr().loc["age", "charges"]),
        },
        "signals": {
            "charges_by_smoker": {
                "labels": charges_by_smoker.index.tolist(),
                "values": [round(float(value), 2) for value in charges_by_smoker.tolist()],
            },
            "charges_by_bmi_band": {
                "labels": [str(label) for label in charges_by_bmi.index.tolist()],
                "values": [round(float(value), 2) for value in charges_by_bmi.tolist()],
            },
            "charges_by_region": {
                "labels": charges_by_region.index.tolist(),
                "values": [round(float(value), 2) for value in charges_by_region.tolist()],
            },
            "charges_by_gender": {
                "labels": charges_by_gender.index.tolist(),
                "values": [round(float(value), 2) for value in charges_by_gender.tolist()],
            },
            "charges_by_children": {
                "labels": [str(int(label)) for label in charges_by_children.index.tolist()],
                "values": [round(float(value), 2) for value in charges_by_children.tolist()],
            },
        },
    }


def _detect_columns(
    df: pd.DataFrame,
    required_aliases: dict[str, list[str]],
    optional_aliases: Optional[dict[str, list[str]]] = None,
) -> Optional[dict[str, dict[str, str]]]:
    normalized = {column.lower().strip(): column for column in df.columns}
    required = {}
    optional = {}

    for canonical, aliases in required_aliases.items():
        match = next((normalized[alias] for alias in aliases if alias in normalized), None)
        if match is None:
            return None
        required[canonical] = match

    for canonical, aliases in (optional_aliases or {}).items():
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
    positives = {"1", "true", "yes", "readmitted", "scheduled", "telehealth", "virtual"}
    return normalized.isin(positives).astype(float)


def _normalize_adherence(value: str) -> str:
    lowered = str(value).strip().lower()
    if "high" in lowered:
        return "High adherence"
    if "low" in lowered:
        return "Low adherence"
    if "medium" in lowered or "moderate" in lowered:
        return "Medium adherence"
    return "Unknown"


def _rate_table(df: pd.DataFrame, column: str, order: Optional[list[str]] = None) -> dict[str, dict[str, float]]:
    grouped = (
        df.groupby(column)
        .agg(readmission_rate=("readmitted", "mean"), count=("patient_id", "size"))
        .assign(readmission_rate=lambda frame: frame["readmission_rate"] * 100)
    )
    if order is not None:
        grouped = grouped.reindex(order).dropna(how="all")
    else:
        grouped = grouped.sort_values("readmission_rate", ascending=False)

    return {
        str(index): {"rate": float(row["readmission_rate"]), "count": int(row["count"])}
        for index, row in grouped.fillna(0).iterrows()
    }


def _value_or_zero(value: Optional[float]) -> float:
    return float(value or 0.0)


def _first_matching_metric(table: dict[str, dict[str, float]], labels: list[str]) -> Optional[float]:
    normalized = {key.strip().lower(): value["rate"] for key, value in table.items()}
    for label in labels:
        if label in normalized:
            return float(normalized[label])
    return None


def _first_matching_average(series: pd.Series, labels: list[str]) -> Optional[float]:
    normalized = {str(index).strip().lower(): float(value) for index, value in series.items()}
    for label in labels:
        if label in normalized:
            return normalized[label]
    return None


def _highest_other_value(series: pd.Series, excluded_labels: list[str]) -> Optional[float]:
    excluded = {label.lower() for label in excluded_labels}
    remaining = [float(value) for index, value in series.items() if str(index).strip().lower() not in excluded]
    return max(remaining) if remaining else None


def _default_sections(insights: list[dict[str, Any]], analysis: Optional[dict[str, Any]] = None) -> list[str]:
    profile = (analysis or {}).get("profile")
    sections = ["overview"]
    for section in _section_config_for_profile(profile):
        if section == "overview":
            continue
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
    profile = analysis.get("profile", "outcomes")

    if profile == "admissions":
        section_map = {
            "overview": {
                "id": "overview",
                "title": "Hospital Operations Narrative",
                "description": "A fast read on throughput, diagnosis mix, and billing consistency across the admissions book.",
                "blocks": [
                    {
                        "id": "overview-metrics",
                        "kind": "metric_grid",
                        "cards": [_metric_card_from_insight(card) for card in metric_cards],
                    },
                    {
                        "id": "overview-insights",
                        "kind": "insight_grid",
                        "insights": [_insight_card(insight) for insight in approved_insights[:4]],
                    },
                ],
            },
            "utilization": {
                "id": "utilization",
                "title": "Admissions and Length of Stay",
                "description": "Operational flow looks standardized, with only small differences by admission type.",
                "blocks": [
                    {
                        "id": "billing-by-admission-type",
                        "kind": "chart",
                        "chart": {
                            "id": "billing-by-admission-type",
                            "title": "Average billing by admission type",
                            "subtitle": "Urgency labels are not doing much explanatory work on their own.",
                            "type": "bar",
                            "labels": signals["billing_by_admission_type"]["labels"],
                            "series": [{"name": "Average billing", "values": signals["billing_by_admission_type"]["values"], "color": "#c2410c"}],
                            "format": "currency",
                        },
                    },
                    {
                        "id": "length-of-stay-by-admission-type",
                        "kind": "chart",
                        "chart": {
                            "id": "length-of-stay-by-admission-type",
                            "title": "Average length of stay by admission type",
                            "subtitle": "Throughput remains tightly clustered across the admission categories.",
                            "type": "bar",
                            "labels": signals["length_of_stay_by_admission_type"]["labels"],
                            "series": [{"name": "Length of stay", "values": signals["length_of_stay_by_admission_type"]["values"], "color": "#166534"}],
                            "format": "number",
                        },
                    },
                ],
            },
            "diagnoses": {
                "id": "diagnoses",
                "title": "Condition Patterns",
                "description": "Condition mix is carrying more signal than the admission labels themselves.",
                "blocks": [
                    {
                        "id": "billing-by-condition",
                        "kind": "chart",
                        "chart": {
                            "id": "billing-by-condition",
                            "title": "Average billing by condition",
                            "subtitle": "This surfaces the highest-cost diagnosis clusters in the file.",
                            "type": "bar",
                            "labels": signals["billing_by_condition"]["labels"],
                            "series": [{"name": "Average billing", "values": signals["billing_by_condition"]["values"], "color": "#9a3412"}],
                            "format": "currency",
                        },
                    },
                    {
                        "id": "test-results-share",
                        "kind": "chart",
                        "chart": {
                            "id": "test-results-share",
                            "title": "Test result distribution",
                            "subtitle": "A large inconclusive share can signal repeat-work and diagnostic ambiguity risk.",
                            "type": "pie",
                            "labels": signals["test_results_share"]["labels"],
                            "series": [{"name": "Share", "values": signals["test_results_share"]["values"], "color": "#b45309"}],
                            "format": "percent",
                        },
                    },
                    {
                        "id": "condition-mix",
                        "kind": "chart",
                        "chart": {
                            "id": "condition-mix",
                            "title": "Condition mix",
                            "subtitle": "The diagnosis book helps explain where volume is concentrated.",
                            "type": "bar",
                            "labels": signals["condition_mix"]["labels"],
                            "series": [{"name": "Cases", "values": signals["condition_mix"]["values"], "color": "#57534e"}],
                            "format": "number",
                        },
                    },
                ],
            },
            "billing": {
                "id": "billing",
                "title": "Billing and Payer Mix",
                "description": "Payer-level pricing is flatter than expected, which points back to provider-side consistency and case mix.",
                "blocks": [
                    {
                        "id": "billing-by-insurance",
                        "kind": "chart",
                        "chart": {
                            "id": "billing-by-insurance",
                            "title": "Average billing by insurance provider",
                            "subtitle": "Payer mix is present, but it is not driving a wide spread in this file.",
                            "type": "bar",
                            "labels": signals["billing_by_insurance"]["labels"],
                            "series": [{"name": "Average billing", "values": signals["billing_by_insurance"]["values"], "color": "#0f766e"}],
                            "format": "currency",
                        },
                    },
                    {
                        "id": "operations-summary-stats",
                        "kind": "stat_list",
                        "title": "Operational markers",
                        "items": [
                            {"label": "Average length of stay", "value": f"{summary['avg_length_of_stay']:.1f} days", "tone": "default"},
                            {"label": "Average billing", "value": f"${summary['avg_billing']:,.0f}", "tone": "default"},
                            {"label": "Top cost condition", "value": summary["top_condition"], "tone": "warning"},
                            {"label": "Unique patients", "value": f"{summary['patient_count']:,}", "tone": "default"},
                        ],
                    },
                ],
            },
            "notes": {
                "id": "notes",
                "title": "Approved Insight Notes",
                "description": "Narrative notes that travel with the dashboard when it reaches operators and executives.",
                "blocks": [{"id": "approved-note-list", "kind": "note_list", "insights": [_insight_card(insight) for insight in approved_insights]}],
            },
        }
    elif profile == "insurance_risk":
        section_map = {
            "overview": {
                "id": "overview",
                "title": "Healthcare Risk Narrative",
                "description": "A fast read on risk factors, demographic signals, and pricing concentration in the member file.",
                "blocks": [
                    {
                        "id": "overview-metrics",
                        "kind": "metric_grid",
                        "cards": [_metric_card_from_insight(card) for card in metric_cards],
                    },
                    {
                        "id": "overview-insights",
                        "kind": "insight_grid",
                        "insights": [_insight_card(insight) for insight in approved_insights[:4]],
                    },
                ],
            },
            "risk_factors": {
                "id": "risk_factors",
                "title": "Smoking and BMI Risk",
                "description": "Behavioral and clinical risk factors are doing more explanatory work than geography or gender.",
                "blocks": [
                    {
                        "id": "charges-by-smoker",
                        "kind": "chart",
                        "chart": {
                            "id": "charges-by-smoker",
                            "title": "Average charges by smoking status",
                            "subtitle": "Smoking is the most dominant cost separator in the file.",
                            "type": "bar",
                            "labels": signals["charges_by_smoker"]["labels"],
                            "series": [{"name": "Average charges", "values": signals["charges_by_smoker"]["values"], "color": "#c2410c"}],
                            "format": "currency",
                        },
                    },
                    {
                        "id": "charges-by-bmi-band",
                        "kind": "chart",
                        "chart": {
                            "id": "charges-by-bmi-band",
                            "title": "Average charges by BMI band",
                            "subtitle": "BMI compounds cost, even if it does not dominate like smoking.",
                            "type": "bar",
                            "labels": signals["charges_by_bmi_band"]["labels"],
                            "series": [{"name": "Average charges", "values": signals["charges_by_bmi_band"]["values"], "color": "#9a3412"}],
                            "format": "currency",
                        },
                    },
                ],
            },
            "demographics": {
                "id": "demographics",
                "title": "Age, Gender, and Family Mix",
                "description": "Demographic variables matter, but they are not the loudest signal in the pricing story.",
                "blocks": [
                    {
                        "id": "charges-by-gender",
                        "kind": "chart",
                        "chart": {
                            "id": "charges-by-gender",
                            "title": "Average charges by gender",
                            "subtitle": "The gender gap exists, but it is small compared with the risk-factor story.",
                            "type": "bar",
                            "labels": signals["charges_by_gender"]["labels"],
                            "series": [{"name": "Average charges", "values": signals["charges_by_gender"]["values"], "color": "#166534"}],
                            "format": "currency",
                        },
                    },
                    {
                        "id": "charges-by-children",
                        "kind": "chart",
                        "chart": {
                            "id": "charges-by-children",
                            "title": "Average charges by number of children",
                            "subtitle": "Family structure has signal, but it is not the primary driver.",
                            "type": "line",
                            "labels": signals["charges_by_children"]["labels"],
                            "series": [{"name": "Average charges", "values": signals["charges_by_children"]["values"], "color": "#0f766e"}],
                            "format": "currency",
                        },
                    },
                    {
                        "id": "pricing-summary-stats",
                        "kind": "stat_list",
                        "title": "Demographic markers",
                        "items": [
                            {"label": "Average charges", "value": f"${summary['avg_charges']:,.0f}", "tone": "default"},
                            {"label": "Age-charge correlation", "value": f"{summary['age_charge_corr']:.2f}", "tone": "default"},
                            {"label": "Members", "value": f"{summary['member_count']:,}", "tone": "default"},
                            {"label": "Gender charge gap", "value": f"${summary['gender_charge_gap']:,.0f}", "tone": "default"},
                        ],
                    },
                ],
            },
            "pricing": {
                "id": "pricing",
                "title": "Regional and Pricing Spread",
                "description": "Geography affects pricing, but the spread is modest relative to risk factors.",
                "blocks": [
                    {
                        "id": "charges-by-region",
                        "kind": "chart",
                        "chart": {
                            "id": "charges-by-region",
                            "title": "Average charges by region",
                            "subtitle": "Regional differences exist, but they are not the central narrative.",
                            "type": "bar",
                            "labels": signals["charges_by_region"]["labels"],
                            "series": [{"name": "Average charges", "values": signals["charges_by_region"]["values"], "color": "#292524"}],
                            "format": "currency",
                        },
                    }
                ],
            },
            "notes": {
                "id": "notes",
                "title": "Approved Insight Notes",
                "description": "Narrative notes that travel with the dashboard when it reaches operators and executives.",
                "blocks": [{"id": "approved-note-list", "kind": "note_list", "insights": [_insight_card(insight) for insight in approved_insights]}],
            },
        }
    else:
        section_map = {
            "overview": {
                "id": "overview",
                "title": "Care Narrative",
                "description": "The fastest read on outcome risk, care delivery, and patient support gaps.",
                "blocks": [
                    {
                        "id": "overview-metrics",
                        "kind": "metric_grid",
                        "cards": [_metric_card_from_insight(card) for card in metric_cards],
                    },
                    {
                        "id": "overview-insights",
                        "kind": "insight_grid",
                        "insights": [_insight_card(insight) for insight in approved_insights[:4]],
                    },
                ],
            },
            "adherence": {
                "id": "adherence",
                "title": "Adherence and Outcomes",
                "description": "Medication adherence is carrying more signal than most clinical segmentation in this dataset.",
                "blocks": [
                    {
                        "id": "adherence-readmission",
                        "kind": "chart",
                        "chart": {
                            "id": "adherence-readmission",
                            "title": "Readmission rate by adherence",
                            "subtitle": "This is the clearest leverage point in the patient journey.",
                            "type": "bar",
                            "labels": signals["adherence_readmission"]["labels"],
                            "series": [{"name": "Readmission rate", "values": signals["adherence_readmission"]["values"], "color": "#c2410c"}],
                            "format": "percent",
                        },
                    }
                ],
            },
            "care_delivery": {
                "id": "care_delivery",
                "title": "Care Delivery Model",
                "description": "Telehealth and follow-up mechanics matter differently than teams usually assume.",
                "blocks": [
                    {
                        "id": "visit-type-readmission",
                        "kind": "chart",
                        "chart": {
                            "id": "visit-type-readmission",
                            "title": "Readmission by visit type",
                            "subtitle": "Virtual care is not underperforming in this sample.",
                            "type": "bar",
                            "labels": signals["visit_type_readmission"]["labels"],
                            "series": [{"name": "Readmission rate", "values": signals["visit_type_readmission"]["values"], "color": "#166534"}],
                            "format": "percent",
                        },
                    },
                    {
                        "id": "follow-up-readmission",
                        "kind": "chart",
                        "chart": {
                            "id": "follow-up-readmission",
                            "title": "Readmission with vs without follow-up scheduling",
                            "subtitle": "Scheduling alone does not appear to shift the outcome curve.",
                            "type": "bar",
                            "labels": signals["follow_up_readmission"]["labels"],
                            "series": [{"name": "Readmission rate", "values": signals["follow_up_readmission"]["values"], "color": "#b45309"}],
                            "format": "percent",
                        },
                    },
                ],
            },
            "equity": {
                "id": "equity",
                "title": "Equity and Access",
                "description": "Small absolute gaps can still be material when they are stable across population groups.",
                "blocks": [
                    {
                        "id": "race-readmission",
                        "kind": "chart",
                        "chart": {
                            "id": "race-readmission",
                            "title": "Readmission by race or ethnicity",
                            "subtitle": "This should trigger deeper investigation, not be treated as background variation.",
                            "type": "bar",
                            "labels": signals["race_readmission"]["labels"],
                            "series": [{"name": "Readmission rate", "values": signals["race_readmission"]["values"], "color": "#9a3412"}],
                            "format": "percent",
                        },
                    }
                ],
            },
            "costs": {
                "id": "costs",
                "title": "Cost and Support Risk",
                "description": "The cost burden and support burden are not landing on the same patient groups.",
                "blocks": [
                    {
                        "id": "insurance-cost",
                        "kind": "chart",
                        "chart": {
                            "id": "insurance-cost",
                            "title": "Average cost by insurance type",
                            "subtitle": "Cost exposure matters most when paired with weak adherence support.",
                            "type": "bar",
                            "labels": signals["insurance_cost"]["labels"],
                            "series": [{"name": "Average cost", "values": signals["insurance_cost"]["values"], "color": "#292524"}],
                            "format": "currency",
                        },
                    },
                    {
                        "id": "insurance-low-adherence",
                        "kind": "chart",
                        "chart": {
                            "id": "insurance-low-adherence",
                            "title": "Low adherence by insurance type",
                            "subtitle": "This surfaces where support failure is concentrated, not just where costs are high.",
                            "type": "bar",
                            "labels": signals["insurance_low_adherence"]["labels"],
                            "series": [{"name": "Low adherence share", "values": signals["insurance_low_adherence"]["values"], "color": "#0f766e"}],
                            "format": "percent",
                        },
                    },
                    {
                        "id": "care-summary-stats",
                        "kind": "stat_list",
                        "title": "System risk markers",
                        "items": [
                            {"label": "Overall readmission", "value": f"{summary['overall_readmission_rate']:.1f}%", "tone": "warning"},
                            {"label": "Average satisfaction", "value": f"{summary['avg_satisfaction']:.2f}", "tone": "positive"},
                            {"label": "Average cost", "value": f"${summary['avg_cost']:,.0f}", "tone": "default"},
                            {"label": "Unique patients", "value": f"{summary['patient_count']:,}", "tone": "default"},
                        ],
                    },
                ],
            },
            "notes": {
                "id": "notes",
                "title": "Approved Insight Notes",
                "description": "Narrative notes that travel with the dashboard when it reaches operators and executives.",
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
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
      body { font-family: Inter, Arial, sans-serif; background: #fafaf9; color: #1c1917; margin: 0; }
      .shell { max-width: 1280px; margin: 0 auto; padding: 32px; }
      .hero, .panel { background: white; border: 1px solid #e7e5e4; border-radius: 24px; padding: 24px; margin-bottom: 24px; }
      .kicker { text-transform: uppercase; letter-spacing: .22em; font-size: 11px; color: #c2410c; font-weight: 700; }
      h1, h2, h3 { font-family: Georgia, serif; margin: 0; }
      .grid { display: grid; gap: 16px; }
      .grid.metrics { grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }
      .grid.cards { grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); }
      .metric, .card, .stat { border: 1px solid #e7e5e4; border-radius: 20px; padding: 16px; background: #fff; }
      .chart { height: 320px; }
      .stat-list { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; }
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
          <div class="kicker">Healthcare insight dashboard</div>
          <h1 style="margin-top: 12px;">${payload.title}</h1>
          <p style="color:#78716c;">${payload.subtitle}</p>
        </section>
        <section class="panel">
          <div class="grid metrics">${metricCards}</div>
        </section>
        <section class="panel">
          <div class="grid cards">${insightCards}</div>
        </section>
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
