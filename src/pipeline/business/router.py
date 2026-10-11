from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from pipeline.business.ecommerce_dashboard import (
    analyze_ecommerce_context,
    build_business_dashboard as build_ecommerce_dashboard,
    build_ecommerce_insight_candidates,
    dashboard_section_options as ecommerce_section_options,
)
from pipeline.business.healthcare_dashboard import (
    analyze_healthcare_context,
    build_business_dashboard as build_healthcare_dashboard,
    build_healthcare_insight_candidates,
    dashboard_section_options as healthcare_section_options,
)
from pipeline.business.marketing_dashboard import (
    analyze_marketing_context,
    build_business_dashboard as build_marketing_dashboard,
    build_marketing_insight_candidates,
    dashboard_section_options as marketing_section_options,
)
from pipeline.business.survey_dashboard import (
    analyze_survey_context,
    build_business_dashboard as build_survey_dashboard,
    build_survey_insight_candidates,
    dashboard_section_options as survey_section_options,
)
from pipeline.business.web_analytics_dashboard import (
    analyze_web_analytics_context,
    build_business_dashboard as build_web_analytics_dashboard,
    build_web_analytics_insight_candidates,
    dashboard_section_options as web_analytics_section_options,
)
from pipeline.business.hr_dashboard import (
    analyze_hr_context,
    build_business_dashboard as build_hr_dashboard,
    build_hr_insight_candidates,
    dashboard_section_options as hr_section_options,
)
from pipeline.business.financial_dashboard import (
    analyze_financial_context,
    build_business_dashboard as build_financial_dashboard,
    build_financial_insight_candidates,
    dashboard_section_options as financial_section_options,
)
from pipeline.common.contracts import PipelineContext

logger = logging.getLogger(__name__)


# Detection order matters: the first template whose analyzer accepts the dataset wins.
_TEMPLATES: list[tuple[str, str, float, Callable[[PipelineContext], Optional[dict[str, Any]]]]] = [
    ("financial_timeseries", "Financial Time Series", 0.94, analyze_financial_context),
    ("ecommerce_orders", "E-commerce / Retail", 0.92, analyze_ecommerce_context),
    ("healthcare_medical", "Healthcare / Medical", 0.91, analyze_healthcare_context),
    ("marketing_campaign", "Marketing / Campaign", 0.9, analyze_marketing_context),
    ("hr_workforce", "HR / Workforce", 0.9, analyze_hr_context),
    ("survey_sentiment", "Survey / Sentiment", 0.89, analyze_survey_context),
    ("web_app_analytics", "Web / App Analytics", 0.88, analyze_web_analytics_context),
]


def detect_business_context(context: PipelineContext) -> Optional[dict[str, Any]]:
    for kind, display_name, confidence, analyze in _TEMPLATES:
        analysis = _try_analyze(kind, analyze, context)
        if analysis is not None:
            return _business_context(kind, display_name, confidence, analysis)
    return None


def analyze_for_kind(context: PipelineContext, kind: str) -> Optional[dict[str, Any]]:
    if kind == "generic":
        return _business_context("generic", "Generic CSV", 0.5, {})
    for template_kind, display_name, confidence, analyze in _TEMPLATES:
        if template_kind != kind:
            continue
        analysis = _try_analyze(kind, analyze, context)
        if analysis is None:
            return None
        return _business_context(kind, display_name, confidence, analysis)
    return None


def _business_context(kind: str, display_name: str, confidence: float, analysis: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": kind,
        "analysis": analysis,
        "display_name": display_name,
        "confidence": confidence,
        # What the UI needs to label the builder, so it does not keep its own copy.
        "sections": section_options(kind, analysis),
        "default_title": default_dashboard_title(kind),
    }


def _try_analyze(
    kind: str,
    analyze: Callable[[PipelineContext], Optional[dict[str, Any]]],
    context: PipelineContext,
) -> Optional[dict[str, Any]]:
    """Run one template's analyzer; a template that breaks on a dataset must not sink the upload."""
    try:
        return analyze(context)
    except Exception as exc:
        logger.exception("Template %s failed while analyzing the dataset", kind)
        context.metadata.setdefault("template_errors", []).append(
            {"kind": kind, "stage": "analyze", "error": f"{type(exc).__name__}: {exc}"}
        )
        return None


def build_insight_candidates(kind: str, analysis: dict[str, Any], user_prompt: str = "") -> dict[str, Any]:
    if kind == "financial_timeseries":
        return build_financial_insight_candidates(analysis, user_prompt)
    if kind == "ecommerce_orders":
        return build_ecommerce_insight_candidates(analysis, user_prompt)
    if kind == "healthcare_medical":
        return build_healthcare_insight_candidates(analysis, user_prompt)
    if kind == "marketing_campaign":
        return build_marketing_insight_candidates(analysis, user_prompt)
    if kind == "hr_workforce":
        return build_hr_insight_candidates(analysis, user_prompt)
    if kind == "survey_sentiment":
        return build_survey_insight_candidates(analysis, user_prompt)
    if kind == "web_app_analytics":
        return build_web_analytics_insight_candidates(analysis, user_prompt)
    return {"insights": [], "focus_tags": []}


def build_dashboard(
    kind: str,
    analysis: dict[str, Any],
    approved_insight_ids: Optional[list[str]] = None,
    user_prompt: str = "",
    settings: Optional[dict[str, Any]] = None,
) -> Optional[dict[str, Any]]:
    if kind == "financial_timeseries":
        return build_financial_dashboard(
            analysis=analysis,
            approved_insight_ids=approved_insight_ids,
            user_prompt=user_prompt,
            settings=settings,
        )
    if kind == "ecommerce_orders":
        return build_ecommerce_dashboard(
            analysis=analysis,
            approved_insight_ids=approved_insight_ids,
            user_prompt=user_prompt,
            settings=settings,
        )
    if kind == "healthcare_medical":
        return build_healthcare_dashboard(
            analysis=analysis,
            approved_insight_ids=approved_insight_ids,
            user_prompt=user_prompt,
            settings=settings,
        )
    if kind == "marketing_campaign":
        return build_marketing_dashboard(
            analysis=analysis,
            approved_insight_ids=approved_insight_ids,
            user_prompt=user_prompt,
            settings=settings,
        )
    if kind == "hr_workforce":
        return build_hr_dashboard(
            analysis=analysis,
            approved_insight_ids=approved_insight_ids,
            user_prompt=user_prompt,
            settings=settings,
        )
    if kind == "survey_sentiment":
        return build_survey_dashboard(
            analysis=analysis,
            approved_insight_ids=approved_insight_ids,
            user_prompt=user_prompt,
            settings=settings,
        )
    if kind == "web_app_analytics":
        return build_web_analytics_dashboard(
            analysis=analysis,
            approved_insight_ids=approved_insight_ids,
            user_prompt=user_prompt,
            settings=settings,
        )
    return None


def section_options(kind: str, analysis: Optional[dict[str, Any]] = None) -> dict[str, str]:
    """Section ids and labels for a template. Pass the analysis to get the sections of its profile."""
    profile = (analysis or {}).get("profile")
    if kind == "financial_timeseries":
        return financial_section_options()
    if kind == "ecommerce_orders":
        return ecommerce_section_options()
    if kind == "healthcare_medical":
        return healthcare_section_options(profile)
    if kind == "marketing_campaign":
        return marketing_section_options(profile)
    if kind == "hr_workforce":
        return hr_section_options()
    if kind == "survey_sentiment":
        return survey_section_options(profile)
    if kind == "web_app_analytics":
        return web_analytics_section_options()
    return {}


def workflow_overview(kind: str, analysis: dict[str, Any]) -> dict[str, Any]:
    if kind == "financial_timeseries":
        dataset = analysis["dataset"]
        summary = analysis["summary"]
        return {
            "title": "Financial dataset detected",
            "blurb": (
                "This looks like long-horizon market data with enough structure to generate hidden market-behavior insights "
                "before committing to a dashboard."
            ),
            "metrics": [
                ("Date Span", f"{dataset['start_year']} - {dataset['end_year']}"),
                ("Latest Close", summary["latest_close_display"]),
                ("Long-Term Move", f"{summary['total_return_pct']:+.0f}%"),
                ("Dividend Events", f"{summary['dividend_events']:,}"),
            ],
        }
    if kind == "ecommerce_orders":
        dataset = analysis["dataset"]
        summary = analysis["summary"]
        return {
            "title": "E-commerce dataset detected",
            "blurb": (
                "This looks like order-level commerce data, so the app is surfacing margin, return, channel, and customer-behavior patterns "
                "that are easy to miss in a plain report."
            ),
            "metrics": [
                ("Date Span", f"{dataset['start_year']} - {dataset['end_year']}"),
                ("Revenue", summary["total_revenue_display"]),
                ("AOV", summary["avg_order_value_display"]),
                ("Return Rate", f"{summary['return_rate']:.1f}%"),
            ],
        }
    if kind == "healthcare_medical":
        summary = analysis["summary"]
        profile = analysis.get("profile", "outcomes")
        if profile == "claims_fraud":
            return {
                "title": "Healthcare claims dataset detected",
                "blurb": (
                    "This looks like healthcare claims and fraud-review data, so Loom is surfacing provider, payer, "
                    "diagnosis, billing, and claim-control patterns."
                ),
                "metrics": [
                    ("Claims", f"{summary['claim_count']:,}"),
                    ("Fraud Rate", f"{summary['fraud_rate']:.1f}%"),
                    ("Providers", f"{summary['provider_count']:,}"),
                    ("Avg Claim", f"${summary['avg_claim_amount']:,.0f}"),
                ],
            }
        if profile == "admissions":
            return {
                "title": "Healthcare admissions dataset detected",
                "blurb": (
                    "This looks like hospital admissions data, so Loom is surfacing utilization, diagnosis, billing, "
                    "and payer-mix patterns."
                ),
                "metrics": [
                    ("Patients", f"{summary['patient_count']:,}"),
                    ("Avg LOS", f"{summary['avg_length_of_stay']:.1f} days"),
                    ("Avg Billing", f"${summary['avg_billing']:,.0f}"),
                    ("Top Condition", summary["top_condition"]),
                ],
            }
        if profile == "insurance_risk":
            return {
                "title": "Healthcare insurance-risk dataset detected",
                "blurb": (
                    "This looks like insurance-risk and pricing data, so Loom is surfacing risk-factor, demographic, "
                    "and charge concentration patterns."
                ),
                "metrics": [
                    ("Members", f"{summary['member_count']:,}"),
                    ("Avg Charges", f"${summary['avg_charges']:,.0f}"),
                    ("Smoker Multiplier", f"{summary['smoker_multiplier']:.1f}x"),
                    ("Age Correlation", f"{summary['age_charge_corr']:.2f}"),
                ],
            }
        return {
            "title": "Healthcare dataset detected",
            "blurb": (
                "This looks like patient-level healthcare data, so Loom is surfacing hidden patterns around adherence, "
                "care delivery, follow-up effectiveness, payer risk, and measurable equity gaps."
            ),
            "metrics": [
                ("Patients", f"{summary['patient_count']:,}"),
                ("Readmission Rate", f"{summary['overall_readmission_rate']:.1f}%"),
                ("Satisfaction", f"{summary['avg_satisfaction']:.2f}"),
                ("Average Cost", f"${summary['avg_cost']:,.0f}"),
            ],
        }
    if kind == "marketing_campaign":
        summary = analysis["summary"]
        profile = analysis.get("profile", "attribution")
        if profile == "crm":
            return {
                "title": "CRM marketing dataset detected",
                "blurb": (
                    "This looks like customer-level campaign response data, so Loom is surfacing hidden patterns around "
                    "offer fatigue, customer value, and response quality."
                ),
                "metrics": [
                    ("Customers", f"{summary['customer_count']:,}"),
                    ("Response Rate", f"{summary['response_rate']:.1f}%"),
                    ("Average Income", f"${summary['avg_income']:,.0f}"),
                    ("Average Recency", f"{summary['avg_recency']:.0f} days"),
                ],
            }
        if profile == "lead_generation":
            return {
                "title": "Lead generation dataset detected",
                "blurb": (
                    "This looks like top-of-funnel lead data, so Loom is surfacing source concentration, landing-page dependency, "
                    "and acquisition volatility."
                ),
                "metrics": [
                    ("Leads", f"{summary['lead_count']:,}"),
                    ("Top Origin", summary["top_origin"]),
                    ("Top Origin Share", f"{summary['top_origin_share']:.1f}%"),
                    ("Top 5 Landing Pages", f"{summary['top_landing_page_share']:.1f}%"),
                ],
            }
        if profile == "closed_deals":
            return {
                "title": "Closed deals dataset detected",
                "blurb": (
                    "This looks like closed marketing-sourced deals, so Loom is surfacing lead quality, revenue concentration, "
                    "and segment mix patterns."
                ),
                "metrics": [
                    ("Deals", f"{summary['deal_count']:,}"),
                    ("Avg Revenue", f"{summary['avg_declared_revenue']:,.0f}"),
                    ("Top Lead Type", summary["top_lead_type"]),
                    ("Top Segment", summary["top_segment"]),
                ],
            }
        return {
            "title": "Marketing dataset detected",
            "blurb": (
                "This looks like campaign-level marketing data, so Loom is surfacing hidden channel allocation, "
                "testing, audience, and device-performance patterns that are easy to miss in standard ROAS reporting."
            ),
            "metrics": [
                ("Campaigns", f"{summary['campaign_count']:,}"),
                ("Total Spend", f"${summary['total_spend']:,.0f}"),
                ("Total Revenue", f"${summary['total_revenue']:,.0f}"),
                ("Overall ROAS", f"{summary['overall_roas']:.2f}x"),
            ],
        }
    if kind == "hr_workforce":
        summary = analysis["summary"]
        return {
            "title": "HR dataset detected",
            "blurb": (
                "This looks like workforce data, so Loom is surfacing retention, pay equity, training, remote-work, "
                "and department-risk patterns that are easy to miss in standard HR reporting."
            ),
            "metrics": [
                ("Employees", f"{summary['employee_count']:,}"),
                ("Attrition Rate", f"{summary['overall_attrition_rate']:.1f}%"),
                ("Avg Engagement", f"{summary['avg_engagement']:.2f}"),
                ("Average Salary", f"${summary['avg_salary']:,.0f}"),
            ],
        }
    if kind == "survey_sentiment":
        summary = analysis["summary"]
        return {
            "title": "Survey dataset detected",
            "blurb": (
                "This looks like respondent-level survey data, so Loom is surfacing hidden patterns around stakeholder gaps, "
                "onboarding friction, product pain points, and renewal risk."
            ),
            "metrics": [
                ("Respondents", f"{summary['respondent_count']:,}"),
                ("Overall NPS", f"{summary['overall_nps']:+.1f}"),
                ("Would Recommend", f"{summary['recommend_rate']:.1f}%"),
                ("Average CES", f"{summary['avg_ces']:.2f}"),
            ],
        }
    if kind == "web_app_analytics":
        summary = analysis["summary"]
        return {
            "title": "Web analytics dataset detected",
            "blurb": (
                "This looks like aggregated web or app funnel data, so Loom is surfacing mobile leakage, "
                "channel waste, campaign winners, and page-level funnel friction."
            ),
            "metrics": [
                ("Sessions", f"{summary['session_count']:,}"),
                ("Conversions", f"{summary['total_conversions']:,}"),
                ("Overall CVR", f"{summary['overall_conversion_rate']:.2f}%"),
                ("Best Campaign", summary["best_campaign"] or "Unknown"),
            ],
        }
    return {"title": "Dataset detected", "blurb": "", "metrics": []}


def default_dashboard_title(kind: str) -> str:
    if kind == "financial_timeseries":
        return "Hidden Market Structure"
    if kind == "ecommerce_orders":
        return "E-commerce Hidden Insights"
    if kind == "healthcare_medical":
        return "Healthcare Hidden Insights"
    if kind == "marketing_campaign":
        return "Marketing Hidden Insights"
    if kind == "hr_workforce":
        return "HR Workforce Hidden Insights"
    if kind == "survey_sentiment":
        return "Survey Sentiment Hidden Insights"
    if kind == "web_app_analytics":
        return "Web Analytics Hidden Insights"
    return "Business Dashboard"


def template_catalog() -> list[dict[str, Any]]:
    return [
        {
            "kind": "financial_timeseries",
            "label": "Financial Time Series",
            "description": "OHLCV-style stock or market data with long-horizon price behavior.",
            "implemented": True,
        },
        {
            "kind": "ecommerce_orders",
            "label": "E-commerce / Retail",
            "description": "Order-level revenue, discount, return, channel, and customer behavior data.",
            "implemented": True,
        },
        {
            "kind": "healthcare_medical",
            "label": "Healthcare / Medical",
            "description": "Patient, treatment, cost, and outcome datasets.",
            "implemented": True,
        },
        {
            "kind": "marketing_campaign",
            "label": "Marketing / Campaign",
            "description": "Impressions, clicks, conversions, and ROAS-style data.",
            "implemented": True,
        },
        {
            "kind": "hr_workforce",
            "label": "HR / Workforce",
            "description": "Employee, department, tenure, salary, and attrition data.",
            "implemented": True,
        },
        {
            "kind": "survey_sentiment",
            "label": "Survey / Sentiment",
            "description": "Response, rating, sentiment, and open-text feedback datasets.",
            "implemented": True,
        },
        {
            "kind": "web_app_analytics",
            "label": "Web / App Analytics",
            "description": "Session, page, event, and device funnel data.",
            "implemented": True,
        },
        {
            "kind": "generic",
            "label": "Generic CSV",
            "description": "Fallback template when no specialized business template is ready.",
            "implemented": True,
        },
    ]
