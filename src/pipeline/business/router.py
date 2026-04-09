from __future__ import annotations

from typing import Any, Optional

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


def detect_business_context(context: PipelineContext) -> Optional[dict[str, Any]]:
    financial = analyze_financial_context(context)
    if financial is not None:
        return {
            "kind": "financial_timeseries",
            "analysis": financial,
            "display_name": "Financial Time Series",
            "confidence": 0.94,
        }

    ecommerce = analyze_ecommerce_context(context)
    if ecommerce is not None:
        return {
            "kind": "ecommerce_orders",
            "analysis": ecommerce,
            "display_name": "E-commerce / Retail",
            "confidence": 0.92,
        }

    healthcare = analyze_healthcare_context(context)
    if healthcare is not None:
        return {
            "kind": "healthcare_medical",
            "analysis": healthcare,
            "display_name": "Healthcare / Medical",
            "confidence": 0.91,
        }

    hr = analyze_hr_context(context)
    if hr is not None:
        return {
            "kind": "hr_workforce",
            "analysis": hr,
            "display_name": "HR / Workforce",
            "confidence": 0.9,
        }

    return None


def analyze_for_kind(context: PipelineContext, kind: str) -> Optional[dict[str, Any]]:
    if kind == "financial_timeseries":
        analysis = analyze_financial_context(context)
        if analysis is None:
            return None
        return {
            "kind": "financial_timeseries",
            "analysis": analysis,
            "display_name": "Financial Time Series",
            "confidence": 0.94,
        }
    if kind == "ecommerce_orders":
        analysis = analyze_ecommerce_context(context)
        if analysis is None:
            return None
        return {
            "kind": "ecommerce_orders",
            "analysis": analysis,
            "display_name": "E-commerce / Retail",
            "confidence": 0.92,
        }
    if kind == "healthcare_medical":
        analysis = analyze_healthcare_context(context)
        if analysis is None:
            return None
        return {
            "kind": "healthcare_medical",
            "analysis": analysis,
            "display_name": "Healthcare / Medical",
            "confidence": 0.91,
        }
    if kind == "hr_workforce":
        analysis = analyze_hr_context(context)
        if analysis is None:
            return None
        return {
            "kind": "hr_workforce",
            "analysis": analysis,
            "display_name": "HR / Workforce",
            "confidence": 0.9,
        }
    if kind == "generic":
        return {
            "kind": "generic",
            "analysis": {},
            "display_name": "Generic CSV",
            "confidence": 0.5,
        }
    return None


def build_insight_candidates(kind: str, analysis: dict[str, Any], user_prompt: str = "") -> dict[str, Any]:
    if kind == "financial_timeseries":
        return build_financial_insight_candidates(analysis, user_prompt)
    if kind == "ecommerce_orders":
        return build_ecommerce_insight_candidates(analysis, user_prompt)
    if kind == "healthcare_medical":
        return build_healthcare_insight_candidates(analysis, user_prompt)
    if kind == "hr_workforce":
        return build_hr_insight_candidates(analysis, user_prompt)
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
    if kind == "hr_workforce":
        return build_hr_dashboard(
            analysis=analysis,
            approved_insight_ids=approved_insight_ids,
            user_prompt=user_prompt,
            settings=settings,
        )
    return None


def section_options(kind: str) -> dict[str, str]:
    if kind == "financial_timeseries":
        return financial_section_options()
    if kind == "ecommerce_orders":
        return ecommerce_section_options()
    if kind == "healthcare_medical":
        return healthcare_section_options()
    if kind == "hr_workforce":
        return hr_section_options()
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
    return {"title": "Dataset detected", "blurb": "", "metrics": []}


def default_dashboard_title(kind: str) -> str:
    if kind == "financial_timeseries":
        return "Hidden Market Structure"
    if kind == "ecommerce_orders":
        return "E-commerce Hidden Insights"
    if kind == "healthcare_medical":
        return "Healthcare Hidden Insights"
    if kind == "hr_workforce":
        return "HR Workforce Hidden Insights"
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
            "implemented": False,
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
            "implemented": False,
        },
        {
            "kind": "web_app_analytics",
            "label": "Web / App Analytics",
            "description": "Session, page, event, and device funnel data.",
            "implemented": False,
        },
        {
            "kind": "generic",
            "label": "Generic CSV",
            "description": "Fallback template when no specialized business template is ready.",
            "implemented": True,
        },
    ]
