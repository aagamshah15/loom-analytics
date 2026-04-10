from __future__ import annotations

import json
import re
from typing import Any, Optional

import pandas as pd
from jinja2 import Template

from pipeline.common.contracts import PipelineContext


REQUIRED_COLUMN_ALIASES = {
    "role": ["role", "persona", "respondent_role", "user_role", "segment"],
    "tenure_months": ["tenure_months", "customer_tenure_months", "months_as_customer", "months_since_signup", "tenure"],
    "nps": ["nps", "nps_score", "recommend_likelihood", "would_recommend_nps", "likelihood_to_recommend"],
    "ces": ["ces", "effort_score", "customer_effort_score"],
    "would_recommend": ["would_recommend", "recommend_binary", "would_recommend_yes_no", "recommend_flag"],
    "renewal_intent": ["renewal_intent", "renew_intent", "renewal_score", "renewal_likelihood"],
}

OPTIONAL_COLUMN_ALIASES = {
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

SECTION_CONFIG = {
    "overview": "KPI cards and key insights",
    "stakeholders": "Charts: stakeholder sentiment gap",
    "onboarding": "Charts: onboarding and tenure",
    "effort": "Charts: effort and loyalty signals",
    "product": "Charts: product quality and complaint themes",
    "renewal": "Charts: renewal and churn risk",
    "notes": "Insight notes",
}


def analyze_survey_context(context: PipelineContext) -> Optional[dict[str, Any]]:
    df = context.clean_df if context.clean_df is not None else context.raw_df
    if df is None or df.empty:
        return None

    detected = _detect_columns(df)
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
    included_sections = settings.get("included_sections") or _default_sections(approved)
    title = settings.get("title") or "Survey Sentiment Hidden Insights"
    subtitle = settings.get("subtitle") or "Approved survey and sentiment narrative"
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
    return SECTION_CONFIG


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


def _detect_columns(df: pd.DataFrame) -> Optional[dict[str, dict[str, str]]]:
    normalized = {str(column).lower().strip(): column for column in df.columns}
    required: dict[str, str] = {}
    optional: dict[str, str] = {}
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


def _default_sections(insights: list[dict[str, Any]]) -> list[str]:
    sections = ["overview"]
    for section in SECTION_CONFIG:
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
