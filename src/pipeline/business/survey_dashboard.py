from __future__ import annotations

import json
import re
from typing import Any, Optional

import pandas as pd
from jinja2 import Template

from pipeline.common.contracts import PipelineContext


B2B_REQUIRED_COLUMN_ALIASES = {
    "role": ["role", "persona", "respondent_role", "user_role", "segment"],
    "tenure_months": ["tenure_months", "customer_tenure_months", "months_as_customer", "months_since_signup", "tenure"],
    "nps": ["nps", "nps_score", "recommend_likelihood", "would_recommend_nps", "likelihood_to_recommend"],
    "ces": ["ces", "effort_score", "customer_effort_score"],
    "would_recommend": ["would_recommend", "recommend_binary", "would_recommend_yes_no", "recommend_flag"],
    "renewal_intent": ["renewal_intent", "renew_intent", "renewal_score", "renewal_likelihood"],
}

B2B_OPTIONAL_COLUMN_ALIASES = {
    "reporting_score": ["reporting_score", "reporting", "feature_reporting_score", "reporting_rating"],
    "reliability_score": ["reliability_score", "reliability", "feature_reliability_score", "reliability_rating"],
    "complaint_theme": ["complaint_theme", "top_complaint_theme", "feedback_theme", "issue_theme"],
    "sentiment": ["sentiment", "sentiment_score", "csat"],
}

FOCUS_KEYWORDS = {
    "stakeholders": ["executive", "buyer", "end user", "persona", "stakeholder", "role"],
    "onboarding": ["onboarding", "tenure", "new customers", "first 90 days", "adoption"],
    "effort": ["effort", "ces", "friction", "ease", "workflow"],
    "product": ["reporting", "features", "reliability", "complaints", "theme"],
    "renewal": ["renewal", "churn", "retention", "detractors"],
    "sources": ["platform", "source", "channel", "topic", "brand", "social"],
    "language": ["language", "text", "sentiment", "tone", "polarity"],
    "momentum": ["time", "timing", "daypart", "cadence", "volume"],
    "experience": ["experience", "satisfaction", "quality", "service", "ambiance", "wifi"],
    "loyalty": ["loyalty", "loyal", "return", "repurchase", "continue", "recommend"],
    "barriers": ["barrier", "cost", "friction", "hesitation", "hearing test"],
    "wellbeing": ["wellbeing", "hearing", "comfort", "missed sounds", "left out"],
    "demographics": ["demographics", "age", "segment", "group"],
}

PROMPT_STOP_WORDS = {
    "about",
    "across",
    "after",
    "before",
    "build",
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
    "survey",
    "sentiment",
    "that",
    "them",
    "these",
    "this",
    "those",
    "want",
    "with",
}

B2B_SECTION_CONFIG = {
    "overview": "KPI cards and key insights",
    "stakeholders": "Charts: stakeholder sentiment gap",
    "onboarding": "Charts: onboarding and tenure",
    "effort": "Charts: effort and loyalty signals",
    "product": "Charts: product quality and complaint themes",
    "renewal": "Charts: renewal and churn risk",
    "notes": "Insight notes",
}

TEXT_SENTIMENT_SECTION_CONFIG = {
    "overview": "KPI cards and key insights",
    "sources": "Charts: sentiment by source",
    "language": "Charts: sentiment distribution and text depth",
    "momentum": "Charts: time and cadence",
    "notes": "Insight notes",
}

SATISFACTION_SECTION_CONFIG = {
    "overview": "KPI cards and key insights",
    "experience": "Charts: experience and feature ratings",
    "channels": "Charts: promo and visit behavior",
    "loyalty": "Charts: loyalty and return intent",
    "notes": "Insight notes",
}

WELLBEING_SECTION_CONFIG = {
    "overview": "KPI cards and key insights",
    "barriers": "Charts: barriers and adoption friction",
    "wellbeing": "Charts: wellbeing and discomfort signals",
    "demographics": "Charts: age and audience mix",
    "notes": "Insight notes",
}


def analyze_survey_context(context: PipelineContext) -> Optional[dict[str, Any]]:
    df = context.clean_df if context.clean_df is not None else context.raw_df
    if df is None or df.empty:
        return None

    return (
        _analyze_b2b_survey_context(df)
        or _analyze_text_sentiment_context(df)
        or _analyze_satisfaction_survey_context(df)
        or _analyze_wellbeing_survey_context(df)
    )


def _analyze_b2b_survey_context(df: pd.DataFrame) -> Optional[dict[str, Any]]:
    detected = _detect_b2b_columns(df)
    if detected is None:
        return None

    working = pd.DataFrame(
        {
            "role": df[detected["required"]["role"]].astype(str).str.strip(),
            "tenure_months": pd.to_numeric(df[detected["required"]["tenure_months"]], errors="coerce"),
            "nps_raw": pd.to_numeric(df[detected["required"]["nps"]], errors="coerce"),
            "ces": pd.to_numeric(df[detected["required"]["ces"]], errors="coerce"),
            "would_recommend": _normalize_binary(df[detected["required"]["would_recommend"]]),
            "renewal_intent": pd.to_numeric(df[detected["required"]["renewal_intent"]], errors="coerce"),
        }
    ).dropna(subset=["role", "tenure_months", "nps_raw", "ces", "renewal_intent"])

    if len(working) < 20:
        return None

    working["role_group"] = working["role"].apply(_normalize_role)
    if working["role_group"].eq("Other").all():
        return None

    optional = detected["optional"]
    if optional.get("reporting_score"):
        working["reporting_score"] = pd.to_numeric(df.loc[working.index, optional["reporting_score"]], errors="coerce")
    else:
        working["reporting_score"] = pd.NA
    if optional.get("reliability_score"):
        working["reliability_score"] = pd.to_numeric(df.loc[working.index, optional["reliability_score"]], errors="coerce")
    else:
        working["reliability_score"] = pd.NA
    if optional.get("complaint_theme"):
        working["complaint_theme"] = df.loc[working.index, optional["complaint_theme"]].astype(str).str.strip()
    else:
        working["complaint_theme"] = ""

    working["nps_score"] = working["nps_raw"].apply(_normalize_nps_score)
    if working["nps_score"].isna().all():
        return None
    working = working.dropna(subset=["nps_score"]).copy()
    if len(working) < 20:
        return None

    working["nps_bucket"] = working["nps_score"].apply(_nps_bucket)
    working["tenure_band"] = working["tenure_months"].apply(_tenure_band)

    role_nps = _group_nps(working, "role_group")
    tenure_nps = _group_nps(working, "tenure_band", order=["Under 3 months", "3-12 months", "1-5 years", "5+ years"])
    renewal_by_bucket = (
        working.groupby("nps_bucket", observed=False)["renewal_intent"]
        .mean()
        .reindex(["Promoters", "Passives", "Detractors"])
        .dropna()
    )

    correlations = {
        "CES": _safe_corr(working["ces"], working["nps_score"]),
        "Reporting": _safe_corr(working["reporting_score"], working["nps_score"]),
        "Reliability": _safe_corr(working["reliability_score"], working["nps_score"]),
        "Renewal intent": _safe_corr(working["renewal_intent"], working["nps_score"]),
    }
    complaint_counts = (
        working.loc[working["complaint_theme"].astype(str).str.len() > 0, "complaint_theme"]
        .str.strip()
        .value_counts()
        .head(6)
    )

    executive_nps = _first_matching_value(role_nps, ["Executive", "Buyer"])
    end_user_nps = _first_matching_value(role_nps, ["End User"])
    new_customer_nps = _first_matching_value(tenure_nps, ["Under 3 months"])
    veteran_nps = _first_matching_value(tenure_nps, ["5+ years"])
    detractor_renewal = float(renewal_by_bucket.get("Detractors", 0.0)) if not renewal_by_bucket.empty else None
    reporting_score = _safe_mean(working["reporting_score"])
    reliability_score = _safe_mean(working["reliability_score"])
    top_complaint_theme = str(complaint_counts.index[0]) if not complaint_counts.empty else None

    return {
        "kind": "survey_sentiment",
        "profile": "b2b_nps",
        "dataset": {
            "row_count": int(len(working)),
            "column_count": int(len(df.columns)),
            "input_columns": list(df.columns),
            "respondent_count": int(len(working)),
        },
        "column_map": detected,
        "summary": {
            "respondent_count": int(len(working)),
            "overall_nps": _overall_nps(working["nps_score"]),
            "recommend_rate": float(working["would_recommend"].mean() * 100),
            "avg_ces": float(working["ces"].mean()),
            "executive_nps": executive_nps,
            "end_user_nps": end_user_nps,
            "new_customer_nps": new_customer_nps,
            "veteran_nps": veteran_nps,
            "ces_correlation": correlations["CES"],
            "reporting_correlation": correlations["Reporting"],
            "reliability_correlation": correlations["Reliability"],
            "renewal_correlation": correlations["Renewal intent"],
            "reporting_score": reporting_score,
            "reliability_score": reliability_score,
            "detractor_renewal_intent": detractor_renewal,
            "top_complaint_theme": top_complaint_theme,
        },
        "signals": {
            "role_nps": {"labels": list(role_nps.keys()), "values": [round(float(value), 2) for value in role_nps.values()]},
            "tenure_nps": {"labels": list(tenure_nps.keys()), "values": [round(float(value), 2) for value in tenure_nps.values()]},
            "driver_correlations": {
                "labels": list(correlations.keys()),
                "values": [round(float(value), 2) for value in correlations.values()],
            },
            "recommend_vs_nps": {
                "labels": ["Would recommend", "NPS"],
                "values": [round(float(working["would_recommend"].mean() * 100), 2), round(float(_overall_nps(working["nps_score"])), 2)],
            },
            "feature_scores": {
                "labels": [label for label, value in [("Reporting", reporting_score), ("Reliability", reliability_score)] if value is not None],
                "values": [round(float(value), 2) for value in [reporting_score, reliability_score] if value is not None],
            },
            "renewal_by_bucket": {
                "labels": renewal_by_bucket.index.tolist(),
                "values": [round(float(value), 2) for value in renewal_by_bucket.tolist()],
            },
            "complaint_themes": {"labels": complaint_counts.index.tolist(), "values": [int(value) for value in complaint_counts.tolist()]},
        },
    }


def build_survey_insight_candidates(analysis: dict[str, Any], user_prompt: str = "") -> dict[str, Any]:
    profile = str(analysis.get("profile") or "b2b_nps")
    if profile == "text_sentiment":
        return _build_text_sentiment_insight_candidates(analysis, user_prompt)
    if profile == "satisfaction":
        return _build_satisfaction_insight_candidates(analysis, user_prompt)
    if profile == "wellbeing":
        return _build_wellbeing_insight_candidates(analysis, user_prompt)
    return _build_b2b_insight_candidates(analysis, user_prompt)


def _build_b2b_insight_candidates(analysis: dict[str, Any], user_prompt: str = "") -> dict[str, Any]:
    summary = analysis["summary"]
    focus_tags = extract_focus_tags(user_prompt)

    insights = [
        {
            "id": "buyer_user_gap",
            "title": "Executives love it, end users hate it",
            "category": "stakeholders",
            "severity": "high",
            "summary": f"Executive NPS is {summary['executive_nps']:+.1f} while End User NPS is {summary['end_user_nps']:+.1f}.",
            "detail": "That is a classic B2B renewal trap: the buyer is satisfied, but the people who live in the product each day are not.",
            "metric_label": "Persona NPS gap",
            "metric_value": f"{abs((summary['executive_nps'] or 0) - (summary['end_user_nps'] or 0)):.1f} pts",
            "metric_sub": "executives vs end users",
            "tags": ["stakeholders"],
            "section": "stakeholders",
            "priority": 100,
            "condition": summary["executive_nps"] is not None and summary["end_user_nps"] is not None,
        },
        {
            "id": "onboarding_problem",
            "title": "New customers are your biggest detractors",
            "category": "onboarding",
            "severity": "high",
            "summary": f"Customers under 3 months score {summary['new_customer_nps']:+.1f} NPS versus {summary['veteran_nps']:+.1f} for customers with 5+ years of tenure.",
            "detail": "This does not look like a broad satisfaction problem. It looks like an onboarding and early-value problem concentrated in the first 90 days.",
            "metric_label": "Tenure NPS gap",
            "metric_value": f"{abs((summary['new_customer_nps'] or 0) - (summary['veteran_nps'] or 0)):.1f} pts",
            "metric_sub": "under 3 months vs 5+ years",
            "tags": ["onboarding"],
            "section": "onboarding",
            "priority": 98,
            "condition": summary["new_customer_nps"] is not None and summary["veteran_nps"] is not None,
        },
        {
            "id": "effort_is_best_predictor",
            "title": "Effort score is the best NPS predictor",
            "category": "effort",
            "severity": "high",
            "summary": f"CES has a {summary['ces_correlation']:+.2f} correlation with NPS, stronger than the other measured drivers.",
            "detail": "This points to friction reduction as the highest-leverage loyalty move, even before adding new functionality.",
            "metric_label": "CES correlation",
            "metric_value": f"{summary['ces_correlation']:+.2f}",
            "metric_sub": "correlation with NPS",
            "tags": ["effort"],
            "section": "effort",
            "priority": 97,
            "condition": summary["ces_correlation"] is not None,
        },
        {
            "id": "recommendation_contradiction",
            "title": "\"Would recommend\" is much rosier than NPS",
            "category": "stakeholders",
            "severity": "medium",
            "summary": f"{summary['recommend_rate']:.1f}% say they would recommend, while overall NPS is {summary['overall_nps']:+.1f}.",
            "detail": "That contradiction usually means customers can imagine recommending the product situationally, but hesitate when the NPS framing asks them to stake their reputation on it.",
            "metric_label": "Recommend vs NPS gap",
            "metric_value": f"{abs(summary['recommend_rate'] - summary['overall_nps']):.1f} pts",
            "metric_sub": "recommend rate minus NPS",
            "tags": ["stakeholders", "effort"],
            "section": "stakeholders",
            "priority": 92,
        },
        {
            "id": "reporting_is_the_broken_feature",
            "title": "Reporting is the most broken feature",
            "category": "product",
            "severity": "high",
            "summary": f"Reporting averages {summary['reporting_score']:.2f} versus {summary['reliability_score']:.2f} for reliability, with \"{summary['top_complaint_theme']}\" emerging as the top complaint theme.",
            "detail": "That is a strong signal that missing capabilities and weak reporting experience are the highest-ROI product investments available.",
            "metric_label": "Reporting deficit",
            "metric_value": f"{((summary['reliability_score'] or 0) - (summary['reporting_score'] or 0)):.2f}",
            "metric_sub": "reliability minus reporting",
            "tags": ["product"],
            "section": "product",
            "priority": 96,
            "condition": summary["reporting_score"] is not None and summary["reliability_score"] is not None,
        },
        {
            "id": "detractors_are_churn_risk",
            "title": "Detractors are already showing renewal risk",
            "category": "renewal",
            "severity": "high",
            "summary": f"Detractors average only {summary['detractor_renewal_intent']:.2f}/5 on renewal intent.",
            "detail": "These are not just unhappy respondents. They are active churn risk sitting in the pipeline right now.",
            "metric_label": "Detractor renewal intent",
            "metric_value": f"{summary['detractor_renewal_intent']:.2f}/5",
            "metric_sub": "average renewal intent among detractors",
            "tags": ["renewal"],
            "section": "renewal",
            "priority": 95,
            "condition": summary["detractor_renewal_intent"] is not None,
        },
    ]
    return _score_insights(insights, focus_tags, user_prompt)


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
        analysis = analyze_survey_context(context)
    if analysis is None:
        return None

    insight_bundle = build_survey_insight_candidates(analysis, user_prompt)
    insights = insight_bundle["insights"]
    approved_set = set(approved_insight_ids or [item["id"] for item in insights if item.get("recommended", True)])
    approved = [item for item in insights if item["id"] in approved_set] or insights[:4]

    settings = settings or {}
    profile = str(analysis.get("profile") or "b2b_nps")
    section_config = _section_config_for_profile(profile)
    included_sections = settings.get("included_sections") or _default_sections(approved, section_config)
    title = settings.get("title") or _default_title_for_profile(profile)
    subtitle = settings.get("subtitle") or _default_subtitle_for_profile(profile)
    metric_count = int(settings.get("metric_count", 4))
    show_notes = bool(settings.get("show_notes", True))

    payload = {
        "kind": "survey_sentiment",
        "title": title,
        "subtitle": subtitle,
        "headline": {"title": title, "subtitle": subtitle},
        "approved_insights": approved,
        "metric_cards": approved[:metric_count],
        "sections": included_sections,
        "layout_sections": _build_layout_sections_for_profile(
            approved_insights=approved,
            metric_cards=approved[:metric_count],
            analysis=analysis,
            included_sections=included_sections,
            show_notes=show_notes,
        ),
        "all_layout_sections": _build_layout_sections_for_profile(
            approved_insights=approved,
            metric_cards=approved,
            analysis=analysis,
            included_sections=list(section_config.keys()),
            show_notes=True,
        ),
        "signals": analysis["signals"],
        "summary": analysis["summary"],
        "dataset": analysis["dataset"],
        "show_notes": show_notes,
        "focus_tags": insight_bundle["focus_tags"],
    }
    html = _render_dashboard_html(payload)
    return {
        "kind": "survey_sentiment",
        "title": title,
        "html": html,
        "height": 1120 + (150 * len(included_sections)),
        "payload": payload,
        "blueprint": payload,
        "download_name": "survey_sentiment_insights_dashboard.html",
    }


def dashboard_section_options() -> dict[str, str]:
    merged: dict[str, str] = {}
    for config in (
        B2B_SECTION_CONFIG,
        TEXT_SENTIMENT_SECTION_CONFIG,
        SATISFACTION_SECTION_CONFIG,
        WELLBEING_SECTION_CONFIG,
    ):
        merged.update(config)
    return merged


def extract_focus_tags(prompt: str) -> list[str]:
    lowered = prompt.strip().lower()
    if not lowered:
        return []
    matches: list[str] = []
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


def _detect_b2b_columns(df: pd.DataFrame) -> Optional[dict[str, dict[str, str]]]:
    normalized = {str(column).lower().strip(): column for column in df.columns}
    required: dict[str, str] = {}
    optional: dict[str, str] = {}
    for canonical, aliases in B2B_REQUIRED_COLUMN_ALIASES.items():
        match = next((normalized[alias] for alias in aliases if alias in normalized), None)
        if match is None:
            return None
        required[canonical] = match
    for canonical, aliases in B2B_OPTIONAL_COLUMN_ALIASES.items():
        match = next((normalized[alias] for alias in aliases if alias in normalized), None)
        if match is not None:
            optional[canonical] = match
    return {"required": required, "optional": optional}


def _normalize_binary(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        numeric = pd.to_numeric(series, errors="coerce").fillna(0.0)
        if numeric.max() > 1:
            numeric = numeric > 0
        return numeric.astype(float)
    lowered = series.astype(str).str.strip().str.lower()
    positives = {"1", "true", "yes", "y", "would recommend", "recommend"}
    return lowered.isin(positives).astype(float)


def _normalize_nps_score(value: Any) -> Optional[float]:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return None
    numeric = float(numeric)
    if 0.0 <= numeric <= 10.0:
        return numeric
    if -100.0 <= numeric <= 100.0:
        if numeric >= 50:
            return 10.0
        if numeric <= -50:
            return 0.0
        return 7.0
    return None


def _normalize_role(value: str) -> str:
    lowered = str(value).strip().lower()
    if any(token in lowered for token in ["executive", "vp", "director", "cxo", "buyer", "admin", "decision maker"]):
        return "Executive"
    if any(token in lowered for token in ["end user", "user", "agent", "analyst", "manager", "staff", "operator", "practitioner"]):
        return "End User"
    return "Other"


def _tenure_band(months: float) -> str:
    if months < 3:
        return "Under 3 months"
    if months < 12:
        return "3-12 months"
    if months < 60:
        return "1-5 years"
    return "5+ years"


def _nps_bucket(score: float) -> str:
    if score >= 9:
        return "Promoters"
    if score >= 7:
        return "Passives"
    return "Detractors"


def _overall_nps(scores: pd.Series) -> float:
    clean = pd.to_numeric(scores, errors="coerce").dropna()
    if clean.empty:
        return 0.0
    promoters = float((clean >= 9).mean() * 100)
    detractors = float((clean <= 6).mean() * 100)
    return promoters - detractors


def _group_nps(df: pd.DataFrame, column: str, order: Optional[list[str]] = None) -> dict[str, float]:
    grouped = {}
    labels = order or list(dict.fromkeys(df[column].tolist()))
    for label in labels:
        subset = df.loc[df[column] == label, "nps_score"]
        if subset.empty:
            continue
        grouped[str(label)] = _overall_nps(subset)
    return grouped


def _safe_corr(series_a: pd.Series, series_b: pd.Series) -> Optional[float]:
    pair = pd.concat([pd.to_numeric(series_a, errors="coerce"), pd.to_numeric(series_b, errors="coerce")], axis=1).dropna()
    if len(pair) < 5:
        return None
    value = pair.iloc[:, 0].corr(pair.iloc[:, 1])
    return None if pd.isna(value) else float(value)


def _safe_mean(series: pd.Series) -> Optional[float]:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return None
    return float(clean.mean())


def _first_matching_value(values: dict[str, float], labels: list[str]) -> Optional[float]:
    normalized = {key.strip().lower(): value for key, value in values.items()}
    for label in labels:
        if label.lower() in normalized:
            return float(normalized[label.lower()])
    return None


def _score_insights(insights: list[dict[str, Any]], focus_tags: list[str], user_prompt: str) -> dict[str, Any]:
    filtered = [item for item in insights if item.get("condition", True)]
    prompt_terms = extract_prompt_terms(user_prompt)
    for item in filtered:
        item["score"] = _instruction_bonus(item, focus_tags, prompt_terms) + item["priority"]
        item["recommended"] = item["score"] >= 85
    filtered.sort(key=lambda item: (-item["score"], item["title"]))
    return {"insights": filtered, "focus_tags": focus_tags}


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


def _default_sections(insights: list[dict[str, Any]], section_config: dict[str, str]) -> list[str]:
    sections = ["overview"]
    for section in section_config:
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


def _build_b2b_layout_sections(
    *,
    approved_insights: list[dict[str, Any]],
    metric_cards: list[dict[str, Any]],
    analysis: dict[str, Any],
    included_sections: list[str],
    show_notes: bool,
) -> list[dict[str, Any]]:
    signals = analysis["signals"]

    section_map = {
        "overview": {
            "id": "overview",
            "title": "Survey Sentiment Narrative",
            "description": "The fastest read on persona gaps, onboarding drag, and churn risk signals in the feedback loop.",
            "blocks": [
                {"id": "overview-metrics", "kind": "metric_grid", "cards": [_metric_card_from_insight(card) for card in metric_cards]},
                {"id": "overview-insights", "kind": "insight_grid", "insights": [_insight_card(insight) for insight in approved_insights[:4]]},
            ],
        },
        "stakeholders": {
            "id": "stakeholders",
            "title": "Stakeholder Sentiment Gap",
            "description": "Buyer and user sentiment are telling different stories, which changes how retention risk should be read.",
            "blocks": [
                {"id": "role-nps", "kind": "chart", "chart": {"id": "role-nps", "title": "NPS by respondent role", "subtitle": "Executive and end-user sentiment should not be treated as interchangeable.", "type": "bar", "labels": signals["role_nps"]["labels"], "series": [{"name": "NPS", "values": signals["role_nps"]["values"], "color": "#c2410c"}], "format": "number"}},
                {"id": "recommend-vs-nps", "kind": "chart", "chart": {"id": "recommend-vs-nps", "title": "Recommendation rate versus NPS", "subtitle": "Situational recommendation can still hide reluctance and reputation risk.", "type": "bar", "labels": signals["recommend_vs_nps"]["labels"], "series": [{"name": "Score", "values": signals["recommend_vs_nps"]["values"], "color": "#9a3412"}], "format": "number"}},
            ],
        },
        "onboarding": {
            "id": "onboarding",
            "title": "Onboarding and Tenure",
            "description": "The first months of the customer journey are carrying far more dissatisfaction than the mature base.",
            "blocks": [
                {"id": "tenure-nps", "kind": "chart", "chart": {"id": "tenure-nps", "title": "NPS by tenure band", "subtitle": "Early experience quality is shaping the long-term relationship curve.", "type": "line", "labels": signals["tenure_nps"]["labels"], "series": [{"name": "NPS", "values": signals["tenure_nps"]["values"], "color": "#b45309"}], "format": "number"}},
            ],
        },
        "effort": {
            "id": "effort",
            "title": "Effort and Loyalty Signals",
            "description": "The strongest predictors of loyalty are often the friction metrics teams underweight.",
            "blocks": [
                {"id": "driver-correlations", "kind": "chart", "chart": {"id": "driver-correlations", "title": "Survey drivers correlated with NPS", "subtitle": "Higher absolute correlation means a stronger relationship to loyalty.", "type": "bar", "labels": signals["driver_correlations"]["labels"], "series": [{"name": "Correlation", "values": signals["driver_correlations"]["values"], "color": "#166534"}], "format": "number"}},
            ],
        },
        "product": {
            "id": "product",
            "title": "Product Quality and Complaint Themes",
            "description": "Feature-level scores and complaint topics show where product investment is most likely to move sentiment.",
            "blocks": [
                {"id": "feature-scores", "kind": "chart", "chart": {"id": "feature-scores", "title": "Average feature scores", "subtitle": "Low-scoring capabilities deserve disproportionate roadmap attention.", "type": "bar", "labels": signals["feature_scores"]["labels"], "series": [{"name": "Average score", "values": signals["feature_scores"]["values"], "color": "#0f766e"}], "format": "number"}},
                {"id": "complaint-themes", "kind": "chart", "chart": {"id": "complaint-themes", "title": "Most common complaint themes", "subtitle": "Theme concentration helps translate open feedback into roadmap pressure.", "type": "bar", "labels": signals["complaint_themes"]["labels"], "series": [{"name": "Count", "values": signals["complaint_themes"]["values"], "color": "#292524"}], "format": "number"}},
            ],
        },
        "renewal": {
            "id": "renewal",
            "title": "Renewal and Churn Risk",
            "description": "Detractors are not just unhappy; they are already signaling commercial risk in the renewal pipeline.",
            "blocks": [
                {"id": "renewal-by-bucket", "kind": "chart", "chart": {"id": "renewal-by-bucket", "title": "Renewal intent by NPS bucket", "subtitle": "Loyalty categories are already surfacing future retention risk.", "type": "bar", "labels": signals["renewal_by_bucket"]["labels"], "series": [{"name": "Renewal intent", "values": signals["renewal_by_bucket"]["values"], "color": "#7c2d12"}], "format": "number"}},
            ],
        },
    }

    if show_notes:
        section_map["notes"] = {
            "id": "notes",
            "title": "Approved Insight Notes",
            "description": "Narrative notes kept alongside the dashboard for review and export.",
            "blocks": [{"id": "notes-list", "kind": "note_list", "insights": [_insight_card(insight) for insight in approved_insights]}],
        }

    return [section_map[section] for section in included_sections if section in section_map]


def _section_config_for_profile(profile: str) -> dict[str, str]:
    if profile == "text_sentiment":
        return TEXT_SENTIMENT_SECTION_CONFIG
    if profile == "satisfaction":
        return SATISFACTION_SECTION_CONFIG
    if profile == "wellbeing":
        return WELLBEING_SECTION_CONFIG
    return B2B_SECTION_CONFIG


def _default_title_for_profile(profile: str) -> str:
    if profile == "text_sentiment":
        return "Survey Sentiment Signal Dashboard"
    if profile == "satisfaction":
        return "Customer Satisfaction Insight Dashboard"
    if profile == "wellbeing":
        return "Wellbeing Survey Insight Dashboard"
    return "Survey Sentiment Hidden Insights"


def _default_subtitle_for_profile(profile: str) -> str:
    if profile == "text_sentiment":
        return "Approved narrative from text and sentiment signals"
    if profile == "satisfaction":
        return "Approved narrative from customer satisfaction responses"
    if profile == "wellbeing":
        return "Approved narrative from wellbeing questionnaire responses"
    return "Approved survey and sentiment narrative"


def _build_layout_sections_for_profile(
    *,
    approved_insights: list[dict[str, Any]],
    metric_cards: list[dict[str, Any]],
    analysis: dict[str, Any],
    included_sections: list[str],
    show_notes: bool,
) -> list[dict[str, Any]]:
    profile = str(analysis.get("profile") or "b2b_nps")
    if profile == "text_sentiment":
        return _build_text_sentiment_layout_sections(
            approved_insights=approved_insights,
            metric_cards=metric_cards,
            analysis=analysis,
            included_sections=included_sections,
            show_notes=show_notes,
        )
    if profile == "satisfaction":
        return _build_satisfaction_layout_sections(
            approved_insights=approved_insights,
            metric_cards=metric_cards,
            analysis=analysis,
            included_sections=included_sections,
            show_notes=show_notes,
        )
    if profile == "wellbeing":
        return _build_wellbeing_layout_sections(
            approved_insights=approved_insights,
            metric_cards=metric_cards,
            analysis=analysis,
            included_sections=included_sections,
            show_notes=show_notes,
        )
    return _build_b2b_layout_sections(
        approved_insights=approved_insights,
        metric_cards=metric_cards,
        analysis=analysis,
        included_sections=included_sections,
        show_notes=show_notes,
    )


def _analyze_text_sentiment_context(df: pd.DataFrame) -> Optional[dict[str, Any]]:
    working_df = _recover_headerless_sentiment_frame(df)
    normalized = {_normalize_column_name(column): column for column in working_df.columns}
    text_col = _match_column(normalized, ["text", "tweet", "message", "content", "review", "comment"])
    sentiment_col = _match_column(normalized, ["sentiment", "label", "emotion", "sentimentlabel"])
    if text_col is None or sentiment_col is None:
        return None

    source_col = _match_column(normalized, ["platform", "source", "channel", "entity", "brand", "topic"])
    time_col = _match_column(normalized, ["timeofday", "timeoftweet", "daypart", "timeslot", "time"])

    working = pd.DataFrame(
        {
            "text": working_df[text_col].astype(str).str.strip(),
            "sentiment": working_df[sentiment_col].apply(_normalize_sentiment_label),
            "source": working_df[source_col].astype(str).str.strip() if source_col else "All responses",
            "time_segment": working_df[time_col].astype(str).str.strip() if time_col else "",
        }
    )
    working = working.loc[working["text"].str.len() > 0].copy()
    working = working.loc[working["sentiment"].isin(["Positive", "Neutral", "Negative"])].copy()
    if len(working) < 20:
        return None

    working["text_length"] = working["text"].str.len()
    sentiment_counts = working["sentiment"].value_counts().reindex(["Positive", "Neutral", "Negative"], fill_value=0)
    working["is_negative"] = working["sentiment"].eq("Negative").astype(float)
    working["polarity"] = working["sentiment"].map({"Positive": 1.0, "Neutral": 0.0, "Negative": -1.0})

    source_negative = (
        working.groupby("source")["is_negative"]
        .mean()
        .mul(100)
        .sort_values(ascending=False)
        .head(6)
    )
    source_polarity = (
        working.groupby("source")["polarity"]
        .mean()
        .mul(100)
        .sort_values()
        .head(6)
    )
    length_by_sentiment = working.groupby("sentiment")["text_length"].mean().reindex(["Positive", "Neutral", "Negative"]).dropna()
    time_negative = (
        working.loc[working["time_segment"].str.len() > 0]
        .groupby("time_segment")["is_negative"]
        .mean()
        .mul(100)
        .sort_values(ascending=False)
        .head(6)
    )

    positive_rate = float(sentiment_counts["Positive"] / len(working) * 100)
    negative_rate = float(sentiment_counts["Negative"] / len(working) * 100)
    neutral_rate = float(sentiment_counts["Neutral"] / len(working) * 100)
    most_negative_source = str(source_negative.index[0]) if not source_negative.empty else None
    most_negative_source_rate = float(source_negative.iloc[0]) if not source_negative.empty else None
    highest_time_segment = str(time_negative.index[0]) if not time_negative.empty else None
    highest_time_negative_rate = float(time_negative.iloc[0]) if not time_negative.empty else None

    return {
        "kind": "survey_sentiment",
        "profile": "text_sentiment",
        "dataset": {
            "row_count": int(len(working)),
            "column_count": int(len(working_df.columns)),
            "input_columns": list(working_df.columns),
            "respondent_count": int(len(working)),
        },
        "summary": {
            "respondent_count": int(len(working)),
            "positive_rate": positive_rate,
            "negative_rate": negative_rate,
            "neutral_rate": neutral_rate,
            "overall_sentiment_index": float(working["polarity"].mean() * 100),
            "avg_text_length": float(working["text_length"].mean()),
            "most_negative_source": most_negative_source,
            "most_negative_source_rate": most_negative_source_rate,
            "highest_time_segment": highest_time_segment,
            "highest_time_negative_rate": highest_time_negative_rate,
        },
        "signals": {
            "sentiment_distribution": {
                "labels": sentiment_counts.index.tolist(),
                "values": [int(value) for value in sentiment_counts.tolist()],
            },
            "source_negative_rate": {
                "labels": source_negative.index.tolist(),
                "values": [round(float(value), 2) for value in source_negative.tolist()],
            },
            "source_polarity": {
                "labels": source_polarity.index.tolist(),
                "values": [round(float(value), 2) for value in source_polarity.tolist()],
            },
            "length_by_sentiment": {
                "labels": length_by_sentiment.index.tolist(),
                "values": [round(float(value), 2) for value in length_by_sentiment.tolist()],
            },
            "time_negative_rate": {
                "labels": time_negative.index.tolist(),
                "values": [round(float(value), 2) for value in time_negative.tolist()],
            },
        },
    }


def _build_text_sentiment_insight_candidates(analysis: dict[str, Any], user_prompt: str = "") -> dict[str, Any]:
    summary = analysis["summary"]
    signals = analysis["signals"]
    focus_tags = extract_focus_tags(user_prompt)
    polarization = summary["positive_rate"] + summary["negative_rate"]
    most_negative_source = str(summary.get("most_negative_source") or "Source")
    most_negative_source_rate = float(summary.get("most_negative_source_rate") or 0.0)
    highest_time_segment = str(summary.get("highest_time_segment") or "Time segment")
    highest_time_negative_rate = float(summary.get("highest_time_negative_rate") or 0.0)
    source_count = len(signals.get("source_negative_rate", {}).get("labels", []))
    insights = [
        {
            "id": "sentiment_baseline",
            "title": "The sentiment mix sets the first decision layer",
            "category": "language",
            "severity": "medium",
            "summary": f"The response stream is {summary['positive_rate']:.1f}% positive, {summary['neutral_rate']:.1f}% neutral, and {summary['negative_rate']:.1f}% negative.",
            "detail": "Before slicing by audience or source, the overall tone tells whether this dataset is a growth narrative, a risk narrative, or a balanced monitoring feed.",
            "metric_label": "Net sentiment index",
            "metric_value": f"{summary['overall_sentiment_index']:+.1f}",
            "metric_sub": "positive minus negative balance",
            "tags": ["language"],
            "section": "language",
            "priority": 93,
        },
        {
            "id": "source_mix_matters",
            "title": "Sentiment varies by source, so the average is hiding context",
            "category": "sources",
            "severity": "medium",
            "summary": f"The dataset has {source_count} source/entity groups with different negative-response rates.",
            "detail": "That makes source-level triage more useful than acting on one blended sentiment average, especially when the same message behaves differently across platforms or brands.",
            "metric_label": "Source groups",
            "metric_value": f"{source_count}",
            "metric_sub": "groups with sentiment coverage",
            "tags": ["sources"],
            "section": "sources",
            "priority": 91,
            "condition": source_count > 1,
        },
        {
            "id": "sentiment_skews_negative",
            "title": "Negative sentiment is outweighing the positive conversation",
            "category": "language",
            "severity": "high",
            "summary": f"Negative sentiment accounts for {summary['negative_rate']:.1f}% of responses versus {summary['positive_rate']:.1f}% positive.",
            "detail": "That makes the feedback stream more of a risk radar than a celebration feed. Narrative and support triage should start from the negative cluster first.",
            "metric_label": "Negative share",
            "metric_value": f"{summary['negative_rate']:.1f}%",
            "metric_sub": "share of labeled responses",
            "tags": ["language"],
            "section": "language",
            "priority": 96,
            "condition": summary["negative_rate"] >= summary["positive_rate"],
        },
        {
            "id": "source_hotspot",
            "title": "One source is carrying disproportionate negativity",
            "category": "sources",
            "severity": "high",
            "summary": f"{most_negative_source} shows a {most_negative_source_rate:.1f}% negative-response rate.",
            "detail": "That makes source quality and message fit a more actionable problem than treating the sentiment stream as one blended audience.",
            "metric_label": "Highest negative source",
            "metric_value": f"{most_negative_source_rate:.1f}%",
            "metric_sub": most_negative_source,
            "tags": ["sources"],
            "section": "sources",
            "priority": 94,
            "condition": summary["most_negative_source"] is not None and summary["most_negative_source_rate"] is not None,
        },
        {
            "id": "polarized_conversation",
            "title": "The conversation is polarized rather than neutral",
            "category": "language",
            "severity": "medium",
            "summary": f"Only {summary['neutral_rate']:.1f}% of responses are neutral while {polarization:.1f}% are clearly positive or negative.",
            "detail": "This is not passive background chatter. People are forming strong opinions, which means messaging changes should move the distribution noticeably.",
            "metric_label": "Polarized share",
            "metric_value": f"{polarization:.1f}%",
            "metric_sub": "positive plus negative responses",
            "tags": ["language"],
            "section": "language",
            "priority": 90,
            "condition": summary["neutral_rate"] < 35,
        },
        {
            "id": "time_segment_spike",
            "title": "Negative sentiment clusters in one time segment",
            "category": "momentum",
            "severity": "medium",
            "summary": f"{highest_time_segment} carries the highest negative share at {highest_time_negative_rate:.1f}%.",
            "detail": "That gives the team a concrete timing lens for moderation, publishing cadence, or incident review instead of treating timing as noise.",
            "metric_label": "Highest negative time segment",
            "metric_value": f"{highest_time_negative_rate:.1f}%",
            "metric_sub": highest_time_segment,
            "tags": ["momentum"],
            "section": "momentum",
            "priority": 88,
            "condition": summary["highest_time_segment"] is not None and summary["highest_time_negative_rate"] is not None,
        },
    ]
    return _score_insights(insights, focus_tags, user_prompt)


def _analyze_satisfaction_survey_context(df: pd.DataFrame) -> Optional[dict[str, Any]]:
    normalized = {_normalize_column_name(column): column for column in df.columns}
    loyalty_col = _match_column_contains(
        normalized,
        ["loyal", "willyoucontinuebuyingatstarbucks", "continuebuying", "continuebuyingatstarbucks", "repurchase", "returnintent"],
    )
    if loyalty_col is None:
        return None

    feature_patterns = [
        ("Product quality", ["productrate", "quality", "productquality"]),
        ("Price", ["pricerate", "pricerange", "price"]),
        ("Promotions", ["promorate", "promotion", "salesandpromotions"]),
        ("Ambiance", ["ambiancerate", "ambiance", "lightingmusic"]),
        ("WiFi", ["wifirate", "wifi"]),
        ("Service", ["servicerate", "service"]),
        ("Occasion fit", ["chooserate", "businessmeetingsorhangout", "choose"]),
    ]
    feature_columns: list[tuple[str, str]] = []
    seen: set[str] = set()
    for label, patterns in feature_patterns:
        match = _match_column_contains(normalized, patterns)
        if match and match not in seen:
            feature_columns.append((label, match))
            seen.add(match)
    if len(feature_columns) < 3:
        return None

    working = pd.DataFrame({"loyalty": _normalize_binary(df[loyalty_col])})
    for label, column in feature_columns:
        working[label] = pd.to_numeric(df[column], errors="coerce")
    working = working.dropna()
    if len(working) < 20:
        return None

    feature_scores = working[[label for label, _ in feature_columns]].mean().sort_values()
    weakest_feature = str(feature_scores.index[0])
    strongest_feature = str(feature_scores.index[-1])
    weakest_score = float(feature_scores.iloc[0])
    strongest_score = float(feature_scores.iloc[-1])

    visit_col = _match_column_contains(normalized, ["visitno", "howoftendoyouvisit", "visit"])
    visit_counts = (
        df[visit_col].astype(str).str.strip().value_counts().head(6)
        if visit_col
        else pd.Series(dtype=int)
    )

    promo_counts = _promo_channel_counts(df, normalized)
    loyalty_labels = ["Would return", "At risk"]
    loyalty_values = [
        round(float(working["loyalty"].mean() * 100), 2),
        round(float((1 - working["loyalty"].mean()) * 100), 2),
    ]

    return {
        "kind": "survey_sentiment",
        "profile": "satisfaction",
        "dataset": {
            "row_count": int(len(working)),
            "column_count": int(len(df.columns)),
            "input_columns": list(df.columns),
            "respondent_count": int(len(working)),
        },
        "summary": {
            "respondent_count": int(len(working)),
            "loyalty_rate": float(working["loyalty"].mean() * 100),
            "weakest_feature": weakest_feature,
            "weakest_feature_score": weakest_score,
            "strongest_feature": strongest_feature,
            "strongest_feature_score": strongest_score,
            "experience_spread": float(strongest_score - weakest_score),
            "top_promo_channel": str(promo_counts.index[0]) if not promo_counts.empty else None,
            "top_promo_channel_share": float(promo_counts.iloc[0] / len(df) * 100) if not promo_counts.empty else None,
        },
        "signals": {
            "feature_scores": {
                "labels": feature_scores.index.tolist(),
                "values": [round(float(value), 2) for value in feature_scores.tolist()],
            },
            "loyalty_mix": {"labels": loyalty_labels, "values": loyalty_values},
            "visit_frequency": {
                "labels": visit_counts.index.tolist(),
                "values": [int(value) for value in visit_counts.tolist()],
            },
            "promo_channels": {
                "labels": promo_counts.index.tolist(),
                "values": [int(value) for value in promo_counts.tolist()],
            },
        },
    }


def _build_satisfaction_insight_candidates(analysis: dict[str, Any], user_prompt: str = "") -> dict[str, Any]:
    summary = analysis["summary"]
    focus_tags = extract_focus_tags(user_prompt)
    insights = [
        {
            "id": "weakest_experience_dimension",
            "title": f"{summary['weakest_feature']} is the weakest part of the experience",
            "category": "experience",
            "severity": "high",
            "summary": f"{summary['weakest_feature']} scores {summary['weakest_feature_score']:.2f}, trailing {summary['strongest_feature']} at {summary['strongest_feature_score']:.2f}.",
            "detail": "That makes it the cleanest candidate for intervention because it is already the biggest drag in an otherwise readable experience stack.",
            "metric_label": "Weakest feature score",
            "metric_value": f"{summary['weakest_feature_score']:.2f}/5",
            "metric_sub": summary["weakest_feature"],
            "tags": ["experience"],
            "section": "experience",
            "priority": 98,
        },
        {
            "id": "loyalty_does_not_match_experience",
            "title": "Experience scores are stronger than the loyalty outcome",
            "category": "loyalty",
            "severity": "high",
            "summary": f"Only {summary['loyalty_rate']:.1f}% signal return intent despite solid top-line feature scores.",
            "detail": "That suggests the experience is acceptable but not distinctive enough to create repeat behavior, which is a more commercial problem than a service-quality problem.",
            "metric_label": "Return intent",
            "metric_value": f"{summary['loyalty_rate']:.1f}%",
            "metric_sub": "share likely to continue buying",
            "tags": ["loyalty"],
            "section": "loyalty",
            "priority": 94,
            "condition": summary["loyalty_rate"] < 75,
        },
        {
            "id": "experience_is_inconsistent",
            "title": "The experience stack is inconsistent across features",
            "category": "experience",
            "severity": "medium",
            "summary": f"There is a {summary['experience_spread']:.2f}-point spread between the strongest and weakest rated dimensions.",
            "detail": "The brand is not failing uniformly. It is producing a lopsided experience, which is often easier to fix than a universal satisfaction slump.",
            "metric_label": "Feature spread",
            "metric_value": f"{summary['experience_spread']:.2f}",
            "metric_sub": "strongest minus weakest score",
            "tags": ["experience"],
            "section": "experience",
            "priority": 91,
            "condition": summary["experience_spread"] >= 0.75,
        },
        {
            "id": "promo_discovery_concentrated",
            "title": "Promotion discovery is concentrated in a small set of channels",
            "category": "channels",
            "severity": "medium",
            "summary": f"{summary['top_promo_channel']} reaches {summary['top_promo_channel_share']:.1f}% of respondents, more than any other promo source.",
            "detail": "That concentration is useful operationally: it tells the team where message reach is actually being created instead of assuming a balanced channel mix.",
            "metric_label": "Top promo channel",
            "metric_value": f"{summary['top_promo_channel_share']:.1f}%",
            "metric_sub": str(summary["top_promo_channel"] or "promo source"),
            "tags": ["channels"],
            "section": "channels",
            "priority": 86,
            "condition": summary["top_promo_channel"] is not None and summary["top_promo_channel_share"] is not None,
        },
    ]
    return _score_insights(insights, focus_tags, user_prompt)


def _analyze_wellbeing_survey_context(df: pd.DataFrame) -> Optional[dict[str, Any]]:
    normalized = {_normalize_column_name(column): column for column in df.columns}
    hearing_like_columns = [column for key, column in normalized.items() if any(token in key for token in ["hearing", "app", "barrier", "discomfort", "sound", "leftout"])]
    if len(hearing_like_columns) < 4:
        return None

    barrier_col = _match_column_contains(normalized, ["hearingtestbarrier", "barrier"])
    app_interest_col = _match_column_contains(normalized, ["interestinhearingapp", "appinterest"])
    early_care_col = _match_column_contains(normalized, ["beliefearlyhearingcare", "earlyhearingcare"])
    discomfort_col = _match_column_contains(normalized, ["eardiscomfortafteruse", "discomfort"])
    left_out_col = _match_column_contains(normalized, ["leftoutduetohearing", "leftout"])
    age_col = _match_column_contains(normalized, ["agegroup", "age"])

    if barrier_col is None and app_interest_col is None and early_care_col is None:
        return None

    working = pd.DataFrame(index=df.index)
    if barrier_col:
        working["barrier"] = df[barrier_col].astype(str).str.strip()
    else:
        working["barrier"] = "Unknown"
    if app_interest_col:
        working["app_interest"] = _normalize_binary(df[app_interest_col])
    else:
        working["app_interest"] = 0.0
    if early_care_col:
        working["early_care"] = pd.to_numeric(df[early_care_col], errors="coerce")
    else:
        working["early_care"] = pd.NA
    if discomfort_col:
        working["discomfort"] = _normalize_binary(df[discomfort_col])
    else:
        working["discomfort"] = 0.0
    if left_out_col:
        working["left_out"] = df[left_out_col].astype(str).str.strip().str.lower().apply(_normalize_impact_signal)
    else:
        working["left_out"] = 0.0
    if age_col:
        working["age_group"] = df[age_col].astype(str).str.strip()
    else:
        working["age_group"] = "Unknown"

    working = working.loc[working["barrier"].str.len() > 0].copy()
    if len(working) < 20:
        return None

    barrier_counts = working["barrier"].value_counts().head(6)
    age_distribution = working["age_group"].value_counts().head(6)
    app_interest_rate = float(pd.to_numeric(working["app_interest"], errors="coerce").fillna(0).mean() * 100)
    discomfort_rate = float(pd.to_numeric(working["discomfort"], errors="coerce").fillna(0).mean() * 100)
    social_impact_rate = float(pd.to_numeric(working["left_out"], errors="coerce").fillna(0).mean() * 100)
    early_care_score = _safe_mean(pd.to_numeric(working["early_care"], errors="coerce"))

    return {
        "kind": "survey_sentiment",
        "profile": "wellbeing",
        "dataset": {
            "row_count": int(len(working)),
            "column_count": int(len(df.columns)),
            "input_columns": list(df.columns),
            "respondent_count": int(len(working)),
        },
        "summary": {
            "respondent_count": int(len(working)),
            "top_barrier": str(barrier_counts.index[0]) if not barrier_counts.empty else None,
            "top_barrier_share": float(barrier_counts.iloc[0] / len(working) * 100) if not barrier_counts.empty else None,
            "app_interest_rate": app_interest_rate,
            "discomfort_rate": discomfort_rate,
            "social_impact_rate": social_impact_rate,
            "early_care_score": early_care_score,
        },
        "signals": {
            "barrier_counts": {
                "labels": barrier_counts.index.tolist(),
                "values": [int(value) for value in barrier_counts.tolist()],
            },
            "age_distribution": {
                "labels": age_distribution.index.tolist(),
                "values": [int(value) for value in age_distribution.tolist()],
            },
            "adoption_signals": {
                "labels": ["Interested in app", "Ear discomfort", "Feeling left out"],
                "values": [round(app_interest_rate, 2), round(discomfort_rate, 2), round(social_impact_rate, 2)],
            },
            "early_care": {
                "labels": ["Belief in early hearing care"],
                "values": [round(float(early_care_score or 0.0), 2)],
            },
        },
    }


def _build_wellbeing_insight_candidates(analysis: dict[str, Any], user_prompt: str = "") -> dict[str, Any]:
    summary = analysis["summary"]
    focus_tags = extract_focus_tags(user_prompt)
    insights = [
        {
            "id": "top_barrier",
            "title": "One barrier is clearly blocking action",
            "category": "barriers",
            "severity": "high",
            "summary": f"{summary['top_barrier']} is the leading obstacle, cited by {summary['top_barrier_share']:.1f}% of respondents.",
            "detail": "That gives the team a far more concrete adoption problem to solve than treating hearing-care hesitation as generic resistance.",
            "metric_label": "Top barrier share",
            "metric_value": f"{summary['top_barrier_share']:.1f}%",
            "metric_sub": str(summary["top_barrier"] or "barrier"),
            "tags": ["barriers"],
            "section": "barriers",
            "priority": 97,
            "condition": summary["top_barrier"] is not None and summary["top_barrier_share"] is not None,
        },
        {
            "id": "app_interest_signal",
            "title": "Digital support interest is already present",
            "category": "wellbeing",
            "severity": "medium",
            "summary": f"{summary['app_interest_rate']:.1f}% show interest in a hearing-related app experience.",
            "detail": "That means the product question is less about whether a digital format is viable and more about which barrier it needs to remove first.",
            "metric_label": "App interest",
            "metric_value": f"{summary['app_interest_rate']:.1f}%",
            "metric_sub": "share open to app support",
            "tags": ["wellbeing"],
            "section": "wellbeing",
            "priority": 92,
            "condition": summary["app_interest_rate"] >= 40,
        },
        {
            "id": "social_impact",
            "title": "The issue is affecting daily participation, not just awareness",
            "category": "wellbeing",
            "severity": "high",
            "summary": f"{summary['social_impact_rate']:.1f}% report feeling left out or socially impacted by hearing issues.",
            "detail": "That makes this more than an education challenge. It is a lived-experience problem, which raises the ROI of practical intervention support.",
            "metric_label": "Social impact rate",
            "metric_value": f"{summary['social_impact_rate']:.1f}%",
            "metric_sub": "share reporting social impact",
            "tags": ["wellbeing"],
            "section": "wellbeing",
            "priority": 95,
            "condition": summary["social_impact_rate"] >= 20,
        },
        {
            "id": "care_belief_gap",
            "title": "Belief in early care is strong, but action still stalls",
            "category": "barriers",
            "severity": "medium",
            "summary": f"Belief in early hearing care averages {summary['early_care_score']:.2f}/5 even though action barriers remain concentrated.",
            "detail": "That usually means the problem is not awareness alone. It is the translation from belief into a low-friction next step.",
            "metric_label": "Early care belief",
            "metric_value": f"{summary['early_care_score']:.2f}/5",
            "metric_sub": "average agreement score",
            "tags": ["barriers", "wellbeing"],
            "section": "barriers",
            "priority": 88,
            "condition": summary["early_care_score"] is not None,
        },
    ]
    return _score_insights(insights, focus_tags, user_prompt)


def _build_text_sentiment_layout_sections(
    *,
    approved_insights: list[dict[str, Any]],
    metric_cards: list[dict[str, Any]],
    analysis: dict[str, Any],
    included_sections: list[str],
    show_notes: bool,
) -> list[dict[str, Any]]:
    signals = analysis["signals"]
    section_map = {
        "overview": {
            "id": "overview",
            "title": "Text Sentiment Narrative",
            "description": "A quick read on conversation polarity, source concentration, and timing spikes in the feedback stream.",
            "blocks": [
                {"id": "overview-metrics", "kind": "metric_grid", "cards": [_metric_card_from_insight(card) for card in metric_cards]},
                {"id": "overview-insights", "kind": "insight_grid", "insights": [_insight_card(insight) for insight in approved_insights[:4]]},
            ],
        },
        "sources": {
            "id": "sources",
            "title": "Source-Level Sentiment",
            "description": "Not all channels or entities are behaving the same way, so one blended average hides the sharpest risk.",
            "blocks": [
                {"id": "source-negative-rate", "kind": "chart", "chart": {"id": "source-negative-rate", "title": "Negative rate by source", "subtitle": "Where negativity concentrates most heavily.", "type": "bar", "labels": signals["source_negative_rate"]["labels"], "series": [{"name": "Negative rate", "values": signals["source_negative_rate"]["values"], "color": "#c2410c"}], "format": "number"}},
                {"id": "source-polarity", "kind": "chart", "chart": {"id": "source-polarity", "title": "Net sentiment by source", "subtitle": "Positive versus negative balance at the source level.", "type": "bar", "labels": signals["source_polarity"]["labels"], "series": [{"name": "Net sentiment", "values": signals["source_polarity"]["values"], "color": "#7c2d12"}], "format": "number"}},
            ],
        },
        "language": {
            "id": "language",
            "title": "Language and Sentiment Mix",
            "description": "The tone profile matters because strong polarity behaves differently from neutral commentary.",
            "blocks": [
                {"id": "sentiment-distribution", "kind": "chart", "chart": {"id": "sentiment-distribution", "title": "Sentiment distribution", "subtitle": "How the conversation splits across positive, neutral, and negative labels.", "type": "bar", "labels": signals["sentiment_distribution"]["labels"], "series": [{"name": "Responses", "values": signals["sentiment_distribution"]["values"], "color": "#b45309"}], "format": "number"}},
                {"id": "length-by-sentiment", "kind": "chart", "chart": {"id": "length-by-sentiment", "title": "Average text length by sentiment", "subtitle": "Complaint and praise depth can differ meaningfully.", "type": "bar", "labels": signals["length_by_sentiment"]["labels"], "series": [{"name": "Characters", "values": signals["length_by_sentiment"]["values"], "color": "#166534"}], "format": "number"}},
            ],
        },
        "momentum": {
            "id": "momentum",
            "title": "Timing and Cadence",
            "description": "Timing gives the team a concrete lens for moderation and publishing rhythm.",
            "blocks": [
                {"id": "time-negative-rate", "kind": "chart", "chart": {"id": "time-negative-rate", "title": "Negative rate by time segment", "subtitle": "Useful when the sentiment stream is not evenly distributed across the day.", "type": "bar", "labels": signals["time_negative_rate"]["labels"], "series": [{"name": "Negative rate", "values": signals["time_negative_rate"]["values"], "color": "#0f766e"}], "format": "number"}},
            ],
        },
    }
    if show_notes:
        section_map["notes"] = {
            "id": "notes",
            "title": "Approved Insight Notes",
            "description": "Narrative notes kept alongside the dashboard for review and export.",
            "blocks": [{"id": "notes-list", "kind": "note_list", "insights": [_insight_card(insight) for insight in approved_insights]}],
        }
    return [section_map[section] for section in included_sections if section in section_map and (section != "momentum" or signals["time_negative_rate"]["labels"])]


def _build_satisfaction_layout_sections(
    *,
    approved_insights: list[dict[str, Any]],
    metric_cards: list[dict[str, Any]],
    analysis: dict[str, Any],
    included_sections: list[str],
    show_notes: bool,
) -> list[dict[str, Any]]:
    signals = analysis["signals"]
    section_map = {
        "overview": {
            "id": "overview",
            "title": "Satisfaction Narrative",
            "description": "A compact read on what customers rate highest, what drags the experience, and whether that translates into loyalty.",
            "blocks": [
                {"id": "overview-metrics", "kind": "metric_grid", "cards": [_metric_card_from_insight(card) for card in metric_cards]},
                {"id": "overview-insights", "kind": "insight_grid", "insights": [_insight_card(insight) for insight in approved_insights[:4]]},
            ],
        },
        "experience": {
            "id": "experience",
            "title": "Experience Ratings",
            "description": "The rating stack highlights where the brand feels strong and where the experience breaks down.",
            "blocks": [
                {"id": "feature-scores", "kind": "chart", "chart": {"id": "feature-scores", "title": "Average feature scores", "subtitle": "The weakest dimension is often the highest-ROI fix.", "type": "bar", "labels": signals["feature_scores"]["labels"], "series": [{"name": "Average score", "values": signals["feature_scores"]["values"], "color": "#c2410c"}], "format": "number"}},
            ],
        },
        "channels": {
            "id": "channels",
            "title": "Discovery and Visit Behavior",
            "description": "Promo discovery and visit frequency help explain how demand is actually being created.",
            "blocks": [
                {"id": "promo-channels", "kind": "chart", "chart": {"id": "promo-channels", "title": "Promo channel reach", "subtitle": "Which promo surfaces are doing the distribution work.", "type": "bar", "labels": signals["promo_channels"]["labels"], "series": [{"name": "Mentions", "values": signals["promo_channels"]["values"], "color": "#166534"}], "format": "number"}},
                {"id": "visit-frequency", "kind": "chart", "chart": {"id": "visit-frequency", "title": "Visit frequency mix", "subtitle": "How often the current audience actually returns.", "type": "bar", "labels": signals["visit_frequency"]["labels"], "series": [{"name": "Responses", "values": signals["visit_frequency"]["values"], "color": "#0f766e"}], "format": "number"}},
            ],
        },
        "loyalty": {
            "id": "loyalty",
            "title": "Loyalty and Return Intent",
            "description": "A serviceable experience does not automatically create repeat behavior.",
            "blocks": [
                {"id": "loyalty-mix", "kind": "chart", "chart": {"id": "loyalty-mix", "title": "Return intent mix", "subtitle": "The commercial outcome to compare against the rating stack.", "type": "bar", "labels": signals["loyalty_mix"]["labels"], "series": [{"name": "Share", "values": signals["loyalty_mix"]["values"], "color": "#7c2d12"}], "format": "number"}},
            ],
        },
    }
    if show_notes:
        section_map["notes"] = {
            "id": "notes",
            "title": "Approved Insight Notes",
            "description": "Narrative notes kept alongside the dashboard for review and export.",
            "blocks": [{"id": "notes-list", "kind": "note_list", "insights": [_insight_card(insight) for insight in approved_insights]}],
        }
    return [section_map[section] for section in included_sections if section in section_map and (section != "channels" or signals["promo_channels"]["labels"] or signals["visit_frequency"]["labels"])]


def _build_wellbeing_layout_sections(
    *,
    approved_insights: list[dict[str, Any]],
    metric_cards: list[dict[str, Any]],
    analysis: dict[str, Any],
    included_sections: list[str],
    show_notes: bool,
) -> list[dict[str, Any]]:
    signals = analysis["signals"]
    section_map = {
        "overview": {
            "id": "overview",
            "title": "Wellbeing Survey Narrative",
            "description": "A compact read on action barriers, lived impact, and adoption readiness in the wellbeing survey.",
            "blocks": [
                {"id": "overview-metrics", "kind": "metric_grid", "cards": [_metric_card_from_insight(card) for card in metric_cards]},
                {"id": "overview-insights", "kind": "insight_grid", "insights": [_insight_card(insight) for insight in approved_insights[:4]]},
            ],
        },
        "barriers": {
            "id": "barriers",
            "title": "Barriers and Friction",
            "description": "The strongest barrier tells the team where to simplify the next action first.",
            "blocks": [
                {"id": "barrier-counts", "kind": "chart", "chart": {"id": "barrier-counts", "title": "Most common barriers", "subtitle": "What is actually preventing action right now.", "type": "bar", "labels": signals["barrier_counts"]["labels"], "series": [{"name": "Responses", "values": signals["barrier_counts"]["values"], "color": "#c2410c"}], "format": "number"}},
                {"id": "early-care", "kind": "chart", "chart": {"id": "early-care", "title": "Belief in early hearing care", "subtitle": "Awareness alone does not guarantee action.", "type": "bar", "labels": signals["early_care"]["labels"], "series": [{"name": "Average score", "values": signals["early_care"]["values"], "color": "#7c2d12"}], "format": "number"}},
            ],
        },
        "wellbeing": {
            "id": "wellbeing",
            "title": "Wellbeing and Daily Impact",
            "description": "This is where the lived experience shows up, beyond awareness or attitudes alone.",
            "blocks": [
                {"id": "adoption-signals", "kind": "chart", "chart": {"id": "adoption-signals", "title": "Adoption and impact signals", "subtitle": "Interest, discomfort, and social effect in one view.", "type": "bar", "labels": signals["adoption_signals"]["labels"], "series": [{"name": "Share", "values": signals["adoption_signals"]["values"], "color": "#166534"}], "format": "number"}},
            ],
        },
        "demographics": {
            "id": "demographics",
            "title": "Audience Mix",
            "description": "Age concentration helps ground the wellbeing story in who is actually responding.",
            "blocks": [
                {"id": "age-distribution", "kind": "chart", "chart": {"id": "age-distribution", "title": "Age-group distribution", "subtitle": "A simple view of the current respondent mix.", "type": "bar", "labels": signals["age_distribution"]["labels"], "series": [{"name": "Responses", "values": signals["age_distribution"]["values"], "color": "#0f766e"}], "format": "number"}},
            ],
        },
    }
    if show_notes:
        section_map["notes"] = {
            "id": "notes",
            "title": "Approved Insight Notes",
            "description": "Narrative notes kept alongside the dashboard for review and export.",
            "blocks": [{"id": "notes-list", "kind": "note_list", "insights": [_insight_card(insight) for insight in approved_insights]}],
        }
    return [section_map[section] for section in included_sections if section in section_map]


def _normalize_column_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


def _match_column(normalized: dict[str, Any], aliases: list[str]) -> Optional[Any]:
    normalized_aliases = {_normalize_column_name(alias) for alias in aliases}
    for alias in normalized_aliases:
        if alias in normalized:
            return normalized[alias]
    return None


def _match_column_contains(normalized: dict[str, Any], patterns: list[str]) -> Optional[Any]:
    normalized_patterns = [_normalize_column_name(pattern) for pattern in patterns]
    for key, original in normalized.items():
        if any(pattern and pattern in key for pattern in normalized_patterns):
            return original
    return None


def _recover_headerless_sentiment_frame(df: pd.DataFrame) -> pd.DataFrame:
    column_tokens = [str(column).strip() for column in df.columns]
    if len(column_tokens) != 4:
        return df
    sentiment_token = column_tokens[2].strip().lower()
    if sentiment_token not in {"positive", "negative", "neutral", "irrelevant"}:
        return df
    if not column_tokens[0].strip().isdigit():
        return df
    body_rows = df.iloc[:, :4].astype(str).values.tolist()
    return pd.DataFrame([column_tokens, *body_rows], columns=["id", "source", "sentiment", "text"])


def _normalize_sentiment_label(value: Any) -> str:
    lowered = str(value).strip().lower()
    if lowered in {"positive", "pos", "joy", "praise"}:
        return "Positive"
    if lowered in {"negative", "neg", "anger", "sad", "complaint"}:
        return "Negative"
    if lowered in {"neutral", "mixed", "irrelevant"}:
        return "Neutral"
    return "Unknown"


def _promo_channel_counts(df: pd.DataFrame, normalized: dict[str, Any]) -> pd.Series:
    encoded_channels = [
        ("App", "promomethodapp"),
        ("Social", "promomethodsoc"),
        ("Email", "promomethodemail"),
        ("Deal sites", "promomethoddeal"),
        ("Friends", "promomethodfriend"),
        ("Display", "promomethoddisplay"),
        ("Billboard", "promomethodbillboard"),
        ("Other", "promomethodothers"),
    ]
    counts: dict[str, int] = {}
    for label, key in encoded_channels:
        if key in normalized:
            counts[label] = int(pd.to_numeric(df[normalized[key]], errors="coerce").fillna(0).gt(0).sum())

    if counts:
        return pd.Series(counts).sort_values(ascending=False)

    promo_col = _match_column_contains(normalized, ["promotion", "promotions", "hearofpromotions"])
    if promo_col is None:
        return pd.Series(dtype=int)

    exploded = (
        df[promo_col]
        .astype(str)
        .str.split(r"[;,]")
        .explode()
        .str.strip()
    )
    exploded = exploded.loc[exploded.str.len() > 0]
    return exploded.value_counts().head(6)


def _normalize_impact_signal(value: Any) -> float:
    lowered = str(value).strip().lower()
    if not lowered or lowered in {"no", "never", "not at all"}:
        return 0.0
    if any(token in lowered for token in ["yes", "often", "family", "public", "noisy", "left out", "sometimes", "only"]):
        return 1.0
    return 0.0


def _render_dashboard_html(payload: dict[str, Any]) -> str:
    template = Template(
        """
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{{ payload.title }}</title>
    <style>
      body { font-family: Arial, sans-serif; background: #fafaf9; color: #1c1917; margin: 0; }
      .wrap { max-width: 1100px; margin: 0 auto; padding: 32px 24px 48px; }
      .hero, .section { background: white; border: 1px solid #e7e5e4; border-radius: 20px; padding: 24px; margin-bottom: 20px; }
      .eyebrow { text-transform: uppercase; letter-spacing: 0.16em; font-size: 12px; color: #78716c; }
      h1, h2, h3 { margin: 0 0 12px; }
      .grid { display: grid; gap: 16px; }
      .grid.cards { grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); }
      .card { border: 1px solid #e7e5e4; border-radius: 16px; padding: 16px; background: #fff; }
      .insight { border-left: 4px solid #c2410c; background: #fff7ed; }
      .note { background: #f5f5f4; }
      .metric { font-size: 28px; font-weight: bold; margin-top: 8px; }
      ul { margin: 0; padding-left: 18px; }
    </style>
  </head>
  <body>
    <div class="wrap">
      <section class="hero">
        <div class="eyebrow">Loom Survey Narrative</div>
        <h1>{{ payload.title }}</h1>
        <p>{{ payload.subtitle }}</p>
      </section>
      {% for section in payload.layout_sections %}
      <section class="section">
        <div class="eyebrow">{{ section.title }}</div>
        <p>{{ section.description }}</p>
        {% for block in section.blocks %}
          {% if block.kind == "metric_grid" %}
            <div class="grid cards">
              {% for card in block.cards %}
              <div class="card">
                <div class="eyebrow">{{ card.label }}</div>
                <div class="metric">{{ card.value }}</div>
                <div>{{ card.sub }}</div>
              </div>
              {% endfor %}
            </div>
          {% elif block.kind == "insight_grid" or block.kind == "note_list" %}
            <div class="grid cards">
              {% for insight in block.insights %}
              <div class="card {% if block.kind == 'insight_grid' %}insight{% else %}note{% endif %}">
                <div class="eyebrow">{{ insight.category }}</div>
                <h3>{{ insight.title }}</h3>
                <p>{% if block.kind == "insight_grid" %}{{ insight.summary }}{% else %}{{ insight.detail }}{% endif %}</p>
              </div>
              {% endfor %}
            </div>
          {% elif block.kind == "chart" %}
            <div class="card">
              <div class="eyebrow">{{ block.chart.title }}</div>
              <p>{{ block.chart.subtitle }}</p>
              <ul>
                {% for label in block.chart.labels %}
                <li>{{ label }}: {{ block.chart.series[0].values[loop.index0] }}</li>
                {% endfor %}
              </ul>
            </div>
          {% endif %}
        {% endfor %}
      </section>
      {% endfor %}
    </div>
  </body>
</html>
        """
    )
    return template.render(payload=json.loads(json.dumps(payload, default=_json_default)))


def _json_default(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return str(value)
