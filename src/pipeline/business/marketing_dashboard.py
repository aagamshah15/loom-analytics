from __future__ import annotations

import json
import re
from typing import Any, Optional

import pandas as pd
from jinja2 import Template

from pipeline.common.contracts import PipelineContext


ATTRIBUTION_REQUIRED_COLUMN_ALIASES = {
    "campaign_id": ["campaign_id", "campaign", "campaign_name", "ad_id", "creative_id"],
    "channel": ["channel", "marketing_channel", "media_channel"],
    "spend": ["spend", "budget", "cost", "ad_spend"],
    "revenue": ["revenue", "return_value", "sales", "attributed_revenue"],
    "experiment_group": ["experiment_group", "variant", "ab_group", "test_group"],
    "device": ["device", "device_type", "platform"],
    "bounce_rate": ["bounce_rate", "bounce", "landing_bounce_rate"],
    "impressions": ["impressions", "impression_count"],
    "age_group": ["age_group", "audience_age", "age_band"],
}

ATTRIBUTION_OPTIONAL_COLUMN_ALIASES = {
    "campaign_type": ["campaign_type", "objective", "format", "creative_type"],
    "roas": ["roas", "return_on_ad_spend"],
}

CRM_REQUIRED_COLUMN_ALIASES = {
    "income": ["income"],
    "recency": ["recency"],
    "response": ["response"],
    "num_deals": ["numdealspurchases", "num_deals_purchases", "numdealspurchases"],
    "num_web": ["numwebpurchases", "num_web_purchases"],
    "num_catalog": ["numcatalogpurchases", "num_catalog_purchases"],
    "num_store": ["numstorepurchases", "num_store_purchases"],
    "num_web_visits": ["numwebvisitsmonth", "num_web_visits_month"],
}

CRM_OPTIONAL_COLUMN_ALIASES = {
    "accepted_overall": ["acceptedcmpoverall"],
    "age": ["age", "year_birth", "year_birth"],
    "kidhome": ["kidhome"],
    "teenhome": ["teenhome"],
    "mnt_total": ["mnttotal"],
}

LEAD_REQUIRED_COLUMN_ALIASES = {
    "mql_id": ["mql_id"],
    "first_contact_date": ["first_contact_date"],
    "landing_page_id": ["landing_page_id"],
    "origin": ["origin"],
}

DEAL_REQUIRED_COLUMN_ALIASES = {
    "mql_id": ["mql_id"],
    "won_date": ["won_date"],
    "business_segment": ["business_segment"],
    "lead_type": ["lead_type"],
    "lead_behaviour_profile": ["lead_behaviour_profile"],
    "business_type": ["business_type"],
    "declared_monthly_revenue": ["declared_monthly_revenue"],
}

FOCUS_KEYWORDS = {
    "channels": ["channel", "budget", "spend", "allocation", "roas"],
    "email": ["email", "crm", "owned", "campaign response"],
    "testing": ["test", "testing", "variant", "control", "creative"],
    "device": ["device", "mobile", "desktop", "connected tv", "ctv"],
    "audience": ["audience", "age", "targeting", "segment"],
    "funnel": ["bounce", "conversion", "landing page", "impressions"],
    "offers": ["offer", "campaign", "response", "discount"],
    "customers": ["customer", "income", "spend", "household"],
    "leads": ["lead", "origin", "landing page", "mql"],
    "revenue": ["revenue", "deal", "closed", "seller"],
}

PROMPT_STOP_WORDS = {
    "about",
    "across",
    "after",
    "before",
    "build",
    "campaign",
    "dashboard",
    "data",
    "emphasize",
    "focus",
    "from",
    "into",
    "look",
    "make",
    "marketing",
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

ATTRIBUTION_SECTION_CONFIG = {
    "overview": "KPI cards and key insights",
    "channels": "Charts: budget and ROAS by channel",
    "testing": "Charts: experiments and device ROAS",
    "audience": "Charts: audience targeting",
    "funnel": "Charts: bounce and impression leakage",
    "notes": "Insight notes",
}

CRM_SECTION_CONFIG = {
    "overview": "KPI cards and key insights",
    "offers": "Charts: offer response patterns",
    "customers": "Charts: customer value and intent",
    "segments": "Charts: household and income segments",
    "retention": "Charts: recency and campaign fatigue",
    "notes": "Insight notes",
}

LEAD_SECTION_CONFIG = {
    "overview": "KPI cards and key insights",
    "acquisition": "Charts: lead source mix",
    "landing_pages": "Charts: landing page concentration",
    "sources": "Charts: source quality and seasonality",
    "notes": "Insight notes",
}

DEAL_SECTION_CONFIG = {
    "overview": "KPI cards and key insights",
    "segments": "Charts: business segment concentration",
    "revenue": "Charts: revenue quality by lead type",
    "sales_motion": "Charts: behaviour and business model quality",
    "notes": "Insight notes",
}


def analyze_marketing_context(context: PipelineContext) -> Optional[dict[str, Any]]:
    df = context.clean_df if context.clean_df is not None else context.raw_df
    if df is None or df.empty:
        return None

    return (
        _analyze_attribution_context(df)
        or _analyze_crm_context(df)
        or _analyze_lead_context(df)
        or _analyze_deals_context(df)
    )


def _analyze_attribution_context(df: pd.DataFrame) -> Optional[dict[str, Any]]:
    detected = _detect_columns(df, ATTRIBUTION_REQUIRED_COLUMN_ALIASES, ATTRIBUTION_OPTIONAL_COLUMN_ALIASES)
    if detected is None:
        return None

    working = pd.DataFrame(
        {
            "campaign_id": df[detected["required"]["campaign_id"]].astype(str).str.strip(),
            "channel": df[detected["required"]["channel"]].astype(str).str.strip(),
            "spend": pd.to_numeric(df[detected["required"]["spend"]], errors="coerce"),
            "revenue": pd.to_numeric(df[detected["required"]["revenue"]], errors="coerce"),
            "experiment_group": df[detected["required"]["experiment_group"]].astype(str).str.strip(),
            "device": df[detected["required"]["device"]].astype(str).str.strip(),
            "bounce_rate": pd.to_numeric(df[detected["required"]["bounce_rate"]], errors="coerce"),
            "impressions": pd.to_numeric(df[detected["required"]["impressions"]], errors="coerce"),
            "age_group": df[detected["required"]["age_group"]].astype(str).str.strip(),
        }
    ).dropna(subset=["campaign_id", "spend", "revenue", "bounce_rate", "impressions"])

    if len(working) < 20:
        return None

    optional_roas = detected["optional"].get("roas")
    if optional_roas:
        roas = pd.to_numeric(df.loc[working.index, optional_roas], errors="coerce")
        working["roas"] = roas.where(roas.notna(), working["revenue"] / working["spend"].replace(0, pd.NA))
    else:
        working["roas"] = working["revenue"] / working["spend"].replace(0, pd.NA)

    working = working.dropna(subset=["roas"]).copy()
    if len(working) < 20:
        return None

    channel_summary = (
        working.groupby("channel")
        .agg(spend=("spend", "sum"), revenue=("revenue", "sum"), impressions=("impressions", "sum"))
        .assign(roas=lambda frame: frame["revenue"] / frame["spend"].replace(0, pd.NA))
        .sort_values("spend", ascending=False)
    )
    total_spend = float(channel_summary["spend"].sum())
    channel_summary["spend_share"] = channel_summary["spend"] / total_spend * 100 if total_spend else 0.0

    experiment_roas = (
        working.groupby("experiment_group")
        .agg(spend=("spend", "sum"), revenue=("revenue", "sum"))
        .assign(roas=lambda frame: frame["revenue"] / frame["spend"].replace(0, pd.NA))
        .sort_values("roas", ascending=False)
    )
    device_summary = (
        working.groupby("device")
        .agg(
            spend=("spend", "sum"),
            revenue=("revenue", "sum"),
            bounce_rate=("bounce_rate", "mean"),
            impressions=("impressions", "sum"),
        )
        .assign(roas=lambda frame: frame["revenue"] / frame["spend"].replace(0, pd.NA))
        .sort_values("impressions", ascending=False)
    )
    total_impressions = float(device_summary["impressions"].sum())
    device_summary["impression_share"] = device_summary["impressions"] / total_impressions * 100 if total_impressions else 0.0
    age_roas = (
        working.groupby("age_group")
        .agg(spend=("spend", "sum"), revenue=("revenue", "sum"))
        .assign(roas=lambda frame: frame["revenue"] / frame["spend"].replace(0, pd.NA))
        .sort_values("roas", ascending=False)
    )

    low_efficiency_channels = channel_summary.sort_values(["roas", "spend"], ascending=[True, False]).head(2)
    email_metrics = _matching_row(channel_summary, ["email"])
    paid_search_metrics = _matching_row(channel_summary, ["paid search", "search", "sem"])
    control_roas = _matching_value(experiment_roas["roas"], ["control"])
    variant_a_roas = _matching_value(experiment_roas["roas"], ["variant a", "a"])
    variant_b_roas = _matching_value(experiment_roas["roas"], ["variant b", "b"])
    ctv_roas = _matching_value(device_summary["roas"], ["connected tv", "ctv"])
    desktop_roas = _matching_value(device_summary["roas"], ["desktop"])
    ctv_bounce = _matching_value(device_summary["bounce_rate"], ["connected tv", "ctv"])
    desktop_bounce = _matching_value(device_summary["bounce_rate"], ["desktop"])
    mobile_bounce = _matching_value(device_summary["bounce_rate"], ["mobile"])
    mobile_impression_share = _matching_value(device_summary["impression_share"], ["mobile"])
    age_roas_spread = float(age_roas["roas"].max() - age_roas["roas"].min()) if not age_roas.empty else 0.0

    return {
        "kind": "marketing_campaign",
        "profile": "attribution",
        "column_map": detected,
        "dataset": {
            "row_count": int(len(working)),
            "column_count": int(len(df.columns)),
            "input_columns": list(df.columns),
            "campaign_count": int(working["campaign_id"].nunique()),
        },
        "summary": {
            "campaign_count": int(working["campaign_id"].nunique()),
            "total_spend": total_spend,
            "total_revenue": float(working["revenue"].sum()),
            "overall_roas": float(working["revenue"].sum() / working["spend"].sum()) if float(working["spend"].sum()) else 0.0,
            "black_hole_channels": low_efficiency_channels.index.tolist(),
            "black_hole_spend_share": float(low_efficiency_channels["spend_share"].sum()),
            "black_hole_roas": float(low_efficiency_channels["roas"].mean()),
            "email_roas": None if email_metrics is None else float(email_metrics["roas"]),
            "email_spend_share": None if email_metrics is None else float(email_metrics["spend_share"]),
            "paid_search_roas": None if paid_search_metrics is None else float(paid_search_metrics["roas"]),
            "control_roas": None if control_roas is None else float(control_roas),
            "variant_a_roas": None if variant_a_roas is None else float(variant_a_roas),
            "variant_b_roas": None if variant_b_roas is None else float(variant_b_roas),
            "ctv_roas": None if ctv_roas is None else float(ctv_roas),
            "desktop_roas": None if desktop_roas is None else float(desktop_roas),
            "ctv_bounce": None if ctv_bounce is None else float(ctv_bounce),
            "desktop_bounce": None if desktop_bounce is None else float(desktop_bounce),
            "age_roas_spread": age_roas_spread,
            "mobile_bounce": None if mobile_bounce is None else float(mobile_bounce),
            "desktop_bounce_reference": None if desktop_bounce is None else float(desktop_bounce),
            "mobile_impression_share": None if mobile_impression_share is None else float(mobile_impression_share),
        },
        "signals": {
            "channel_spend_share": {"labels": channel_summary.index.tolist(), "values": [round(float(value), 2) for value in channel_summary["spend_share"].tolist()]},
            "channel_roas": {"labels": channel_summary["roas"].sort_values(ascending=False).index.tolist(), "values": [round(float(value), 2) for value in channel_summary["roas"].sort_values(ascending=False).tolist()]},
            "experiment_roas": {"labels": experiment_roas.index.tolist(), "values": [round(float(value), 2) for value in experiment_roas["roas"].tolist()]},
            "device_roas": {"labels": device_summary.index.tolist(), "values": [round(float(value), 2) for value in device_summary["roas"].tolist()]},
            "device_bounce": {"labels": device_summary.index.tolist(), "values": [round(float(value), 2) for value in device_summary["bounce_rate"].tolist()]},
            "device_impression_share": {"labels": device_summary.index.tolist(), "values": [round(float(value), 2) for value in device_summary["impression_share"].tolist()]},
            "age_group_roas": {"labels": age_roas.index.tolist(), "values": [round(float(value), 2) for value in age_roas["roas"].tolist()]},
        },
    }


def _analyze_crm_context(df: pd.DataFrame) -> Optional[dict[str, Any]]:
    detected = _detect_columns(df, CRM_REQUIRED_COLUMN_ALIASES, CRM_OPTIONAL_COLUMN_ALIASES)
    if detected is None:
        return None

    working = pd.DataFrame(
        {
            "income": pd.to_numeric(df[detected["required"]["income"]], errors="coerce"),
            "recency": pd.to_numeric(df[detected["required"]["recency"]], errors="coerce"),
            "response": _normalize_binary(df[detected["required"]["response"]]),
            "num_deals": pd.to_numeric(df[detected["required"]["num_deals"]], errors="coerce"),
            "num_web": pd.to_numeric(df[detected["required"]["num_web"]], errors="coerce"),
            "num_catalog": pd.to_numeric(df[detected["required"]["num_catalog"]], errors="coerce"),
            "num_store": pd.to_numeric(df[detected["required"]["num_store"]], errors="coerce"),
            "num_web_visits": pd.to_numeric(df[detected["required"]["num_web_visits"]], errors="coerce"),
        }
    ).dropna()

    if len(working) < 20:
        return None

    working["household_children"] = 0.0
    for optional_name in ("kidhome", "teenhome"):
        column = detected["optional"].get(optional_name)
        if column:
            working[optional_name] = pd.to_numeric(df.loc[working.index, column], errors="coerce").fillna(0.0)
            working["household_children"] += working[optional_name]

    if detected["optional"].get("accepted_overall"):
        working["accepted_overall"] = pd.to_numeric(df.loc[working.index, detected["optional"]["accepted_overall"]], errors="coerce").fillna(0.0)
    else:
        accepted_columns = [column for column in df.columns if str(column).lower().startswith("acceptedcmp")]
        if accepted_columns:
            accepted_frame = df.loc[working.index, accepted_columns].apply(pd.to_numeric, errors="coerce").fillna(0.0)
            working["accepted_overall"] = accepted_frame.sum(axis=1)
        else:
            working["accepted_overall"] = 0.0

    if detected["optional"].get("mnt_total"):
        working["mnt_total"] = pd.to_numeric(df.loc[working.index, detected["optional"]["mnt_total"]], errors="coerce")
    else:
        spend_columns = [column for column in df.columns if str(column).lower().startswith("mnt")]
        if spend_columns:
            spend_frame = df.loc[working.index, spend_columns].apply(pd.to_numeric, errors="coerce").fillna(0.0)
            working["mnt_total"] = spend_frame.sum(axis=1)
        else:
            working["mnt_total"] = 0.0

    working["offer_history_band"] = pd.cut(working["accepted_overall"], bins=[-1, 0, 2, float("inf")], labels=["No prior wins", "Some prior wins", "Heavy prior winners"])
    working["value_band"] = pd.qcut(working["mnt_total"], 4, duplicates="drop")
    working["income_band"] = pd.qcut(working["income"], 4, duplicates="drop")
    working["deal_band"] = pd.cut(working["num_deals"], bins=[-1, 1, 4, float("inf")], labels=["Low deal use", "Mid deal use", "Heavy deal use"])
    working["web_visit_band"] = pd.cut(working["num_web_visits"], bins=[-1, 3, 6, float("inf")], labels=["Low visits", "Mid visits", "High visits"])
    working["household_band"] = pd.cut(working["household_children"], bins=[-1, 0, 1, float("inf")], labels=["No children", "One child", "Two or more"])

    response_by_offer_history = _crm_rate_table(working, "offer_history_band")
    response_by_value = _crm_rate_table(working, "value_band")
    response_by_income = _crm_rate_table(working, "income_band")
    response_by_deal = _crm_rate_table(working, "deal_band")
    response_by_web_visits = _crm_rate_table(working, "web_visit_band")
    response_by_household = _crm_rate_table(working, "household_band")

    no_prior_response = _matching_rate(response_by_offer_history, ["No prior wins"])
    heavy_prior_response = _matching_rate(response_by_offer_history, ["Heavy prior winners"])
    low_value_response = _first_rate(response_by_value)
    high_value_response = _last_rate(response_by_value)
    low_visit_response = _matching_rate(response_by_web_visits, ["Low visits"])
    high_visit_response = _matching_rate(response_by_web_visits, ["High visits"])
    no_children_response = _matching_rate(response_by_household, ["No children"])
    large_household_response = _matching_rate(response_by_household, ["Two or more"])

    return {
        "kind": "marketing_campaign",
        "profile": "crm",
        "column_map": detected,
        "dataset": {
            "row_count": int(len(working)),
            "column_count": int(len(df.columns)),
            "input_columns": list(df.columns),
            "customer_count": int(len(working)),
        },
        "summary": {
            "customer_count": int(len(working)),
            "response_rate": float(working["response"].mean() * 100),
            "avg_income": float(working["income"].mean()),
            "avg_recency": float(working["recency"].mean()),
            "no_prior_response": no_prior_response,
            "heavy_prior_response": heavy_prior_response,
            "low_value_response": low_value_response,
            "high_value_response": high_value_response,
            "low_visit_response": low_visit_response,
            "high_visit_response": high_visit_response,
            "no_children_response": no_children_response,
            "large_household_response": large_household_response,
        },
        "signals": {
            "response_by_offer_history": _rate_signal(response_by_offer_history),
            "response_by_value": _rate_signal(response_by_value),
            "response_by_income": _rate_signal(response_by_income),
            "response_by_deal": _rate_signal(response_by_deal),
            "response_by_web_visits": _rate_signal(response_by_web_visits),
            "response_by_household": _rate_signal(response_by_household),
        },
    }


def _analyze_lead_context(df: pd.DataFrame) -> Optional[dict[str, Any]]:
    detected = _detect_columns(df, LEAD_REQUIRED_COLUMN_ALIASES)
    if detected is None:
        return None

    working = pd.DataFrame(
        {
            "mql_id": df[detected["required"]["mql_id"]].astype(str).str.strip(),
            "first_contact_date": pd.to_datetime(df[detected["required"]["first_contact_date"]], errors="coerce"),
            "landing_page_id": df[detected["required"]["landing_page_id"]].astype(str).str.strip(),
            "origin": df[detected["required"]["origin"]].astype(str).str.strip(),
        }
    ).dropna()

    if len(working) < 20:
        return None

    origin_share = working["origin"].value_counts(normalize=True).mul(100)
    landing_page_counts = working["landing_page_id"].value_counts()
    leads_by_month = working.assign(month=working["first_contact_date"].dt.to_period("M").astype(str)).groupby("month").size()
    top_origin = str(origin_share.index[0])
    second_origin = str(origin_share.index[1]) if len(origin_share) > 1 else top_origin
    unknown_share = float(origin_share.get("unknown", 0.0))
    top_landing_page_share = float(landing_page_counts.iloc[:5].sum() / len(working) * 100)

    return {
        "kind": "marketing_campaign",
        "profile": "lead_generation",
        "column_map": detected,
        "dataset": {
            "row_count": int(len(working)),
            "column_count": int(len(df.columns)),
            "input_columns": list(df.columns),
            "lead_count": int(working["mql_id"].nunique()),
        },
        "summary": {
            "lead_count": int(working["mql_id"].nunique()),
            "top_origin": top_origin,
            "top_origin_share": float(origin_share.iloc[0]),
            "second_origin": second_origin,
            "second_origin_share": float(origin_share.iloc[1]) if len(origin_share) > 1 else float(origin_share.iloc[0]),
            "unknown_share": unknown_share,
            "top_landing_page_share": top_landing_page_share,
            "top_month_leads": int(leads_by_month.max()),
            "avg_monthly_leads": float(leads_by_month.mean()),
        },
        "signals": {
            "origin_share": {"labels": origin_share.index.tolist(), "values": [round(float(value), 2) for value in origin_share.tolist()]},
            "landing_page_counts": {"labels": landing_page_counts.head(6).index.tolist(), "values": [int(value) for value in landing_page_counts.head(6).tolist()]},
            "leads_by_month": {"labels": leads_by_month.index.tolist(), "values": [int(value) for value in leads_by_month.tolist()]},
        },
    }


def _analyze_deals_context(df: pd.DataFrame) -> Optional[dict[str, Any]]:
    detected = _detect_columns(df, DEAL_REQUIRED_COLUMN_ALIASES)
    if detected is None:
        return None

    working = pd.DataFrame(
        {
            "mql_id": df[detected["required"]["mql_id"]].astype(str).str.strip(),
            "won_date": pd.to_datetime(df[detected["required"]["won_date"]], errors="coerce"),
            "business_segment": df[detected["required"]["business_segment"]].astype(str).str.strip(),
            "lead_type": df[detected["required"]["lead_type"]].astype(str).str.strip(),
            "lead_behaviour_profile": df[detected["required"]["lead_behaviour_profile"]].astype(str).str.strip(),
            "business_type": df[detected["required"]["business_type"]].astype(str).str.strip(),
            "declared_monthly_revenue": pd.to_numeric(df[detected["required"]["declared_monthly_revenue"]], errors="coerce"),
        }
    ).dropna(subset=["mql_id", "declared_monthly_revenue"])

    if len(working) < 20:
        return None

    revenue_by_lead_type = working.groupby("lead_type")["declared_monthly_revenue"].mean().sort_values(ascending=False)
    revenue_by_behaviour = working.groupby("lead_behaviour_profile")["declared_monthly_revenue"].mean().sort_values(ascending=False)
    revenue_by_business_type = working.groupby("business_type")["declared_monthly_revenue"].mean().sort_values(ascending=False)
    segment_counts = working["business_segment"].value_counts().head(6)

    top_lead_type = str(revenue_by_lead_type.index[0])
    lowest_lead_type_revenue = float(revenue_by_lead_type.iloc[-1])
    top_behaviour = str(revenue_by_behaviour.index[0])

    return {
        "kind": "marketing_campaign",
        "profile": "closed_deals",
        "column_map": detected,
        "dataset": {
            "row_count": int(len(working)),
            "column_count": int(len(df.columns)),
            "input_columns": list(df.columns),
            "deal_count": int(working["mql_id"].nunique()),
        },
        "summary": {
            "deal_count": int(working["mql_id"].nunique()),
            "avg_declared_revenue": float(working["declared_monthly_revenue"].mean()),
            "top_lead_type": top_lead_type,
            "top_lead_type_revenue": float(revenue_by_lead_type.iloc[0]),
            "lead_type_revenue_gap": float(revenue_by_lead_type.iloc[0] - lowest_lead_type_revenue),
            "top_behaviour": top_behaviour,
            "top_behaviour_revenue": float(revenue_by_behaviour.iloc[0]),
            "manufacturer_revenue": _matching_value(revenue_by_business_type, ["manufacturer"]),
            "reseller_revenue": _matching_value(revenue_by_business_type, ["reseller"]),
            "top_segment": str(segment_counts.index[0]),
            "top_segment_count": int(segment_counts.iloc[0]),
        },
        "signals": {
            "revenue_by_lead_type": {"labels": revenue_by_lead_type.index.tolist(), "values": [round(float(value), 2) for value in revenue_by_lead_type.tolist()]},
            "revenue_by_behaviour": {"labels": revenue_by_behaviour.head(6).index.tolist(), "values": [round(float(value), 2) for value in revenue_by_behaviour.head(6).tolist()]},
            "revenue_by_business_type": {"labels": revenue_by_business_type.index.tolist(), "values": [round(float(value), 2) for value in revenue_by_business_type.tolist()]},
            "segment_counts": {"labels": segment_counts.index.tolist(), "values": [int(value) for value in segment_counts.tolist()]},
        },
    }


def build_marketing_insight_candidates(analysis: dict[str, Any], user_prompt: str = "") -> dict[str, Any]:
    profile = analysis.get("profile", "attribution")
    if profile == "crm":
        return _build_crm_insights(analysis, user_prompt)
    if profile == "lead_generation":
        return _build_lead_insights(analysis, user_prompt)
    if profile == "closed_deals":
        return _build_deal_insights(analysis, user_prompt)
    return _build_attribution_insights(analysis, user_prompt)


def _build_attribution_insights(analysis: dict[str, Any], user_prompt: str) -> dict[str, Any]:
    summary = analysis["summary"]
    focus_tags = extract_focus_tags(user_prompt)
    insights = [
        {
            "id": "channel_budget_black_holes",
            "title": "Big channels are acting like budget black holes",
            "category": "channels",
            "severity": "high",
            "summary": f"{', '.join(summary['black_hole_channels'])} absorb {summary['black_hole_spend_share']:.1f}% of spend while averaging only {summary['black_hole_roas']:.2f}x ROAS.",
            "detail": "These are the first reallocations to challenge, because they pair large budget weight with weak measurable return.",
            "metric_label": "Low-efficiency spend share",
            "metric_value": f"{summary['black_hole_spend_share']:.1f}%",
            "metric_sub": "combined share of the two weakest large channels",
            "tags": ["channels"],
            "section": "channels",
            "priority": 100,
        },
        {
            "id": "email_hidden_champion",
            "title": "Email is the hidden champion",
            "category": "email",
            "severity": "high",
            "summary": f"Email drives {summary['email_roas']:.2f}x ROAS on only {summary['email_spend_share']:.1f}% of spend, versus {summary['paid_search_roas']:.2f}x for Paid Search.",
            "detail": "That makes email one of the most efficient channels in the mix despite receiving a tiny budget allocation.",
            "metric_label": "Email efficiency edge",
            "metric_value": f"{(summary['email_roas'] or 0) - (summary['paid_search_roas'] or 0):.2f}x",
            "metric_sub": "email ROAS minus paid search ROAS",
            "tags": ["email", "channels"],
            "section": "channels",
            "priority": 98,
            "condition": summary["email_roas"] is not None and summary["paid_search_roas"] is not None,
        },
        {
            "id": "ab_tests_making_things_worse",
            "title": "Your A/B tests are making things worse",
            "category": "testing",
            "severity": "high",
            "summary": f"The control group returns {summary['control_roas']:.2f}x ROAS versus {summary['variant_a_roas']:.2f}x for Variant A and {summary['variant_b_roas']:.2f}x for Variant B.",
            "detail": "The current creative testing cycle is degrading performance relative to the baseline rather than improving it.",
            "metric_label": "Control advantage",
            "metric_value": f"{(summary['control_roas'] or 0) - max(summary['variant_a_roas'] or 0, summary['variant_b_roas'] or 0):.2f}x",
            "metric_sub": "control ROAS over best variant",
            "tags": ["testing"],
            "section": "testing",
            "priority": 97,
            "condition": summary["control_roas"] is not None and summary["variant_a_roas"] is not None and summary["variant_b_roas"] is not None,
        },
        {
            "id": "connected_tv_surprise",
            "title": "Connected TV is outperforming expectations",
            "category": "device",
            "severity": "medium",
            "summary": f"Connected TV delivers {summary['ctv_roas']:.2f}x ROAS versus {summary['desktop_roas']:.2f}x on Desktop with a similar bounce profile.",
            "detail": "That hints at higher-intent post-view behavior that simple attribution often undersells.",
            "metric_label": "CTV ROAS edge",
            "metric_value": f"{(summary['ctv_roas'] or 0) - (summary['desktop_roas'] or 0):.2f}x",
            "metric_sub": "connected TV over desktop",
            "tags": ["device", "channels"],
            "section": "testing",
            "priority": 92,
            "condition": summary["ctv_roas"] is not None and summary["desktop_roas"] is not None,
        },
        {
            "id": "age_targeting_barely_matters",
            "title": "Age targeting barely matters",
            "category": "audience",
            "severity": "medium",
            "summary": f"ROAS spread across age groups is only {summary['age_roas_spread']:.2f}x.",
            "detail": "Channel and campaign mechanics matter far more here than audience age bands.",
            "metric_label": "Age-group ROAS spread",
            "metric_value": f"{summary['age_roas_spread']:.2f}x",
            "metric_sub": "highest age band minus lowest age band",
            "tags": ["audience"],
            "section": "audience",
            "priority": 89,
        },
        {
            "id": "mobile_conversion_leak",
            "title": "Mobile is a conversion leak",
            "category": "funnel",
            "severity": "high",
            "summary": f"Mobile carries {summary['mobile_impression_share']:.1f}% of impressions and a {summary['mobile_bounce']:.2f} bounce rate, versus {summary['desktop_bounce_reference']:.2f} on Desktop.",
            "detail": "That points to landing-page or post-click experience friction as a higher-ROI fix than simply adding more top-of-funnel spend.",
            "metric_label": "Mobile bounce penalty",
            "metric_value": f"{((summary['mobile_bounce'] or 0) - (summary['desktop_bounce_reference'] or 0)) * 100:.1f} pts",
            "metric_sub": "mobile minus desktop bounce rate",
            "tags": ["funnel", "device"],
            "section": "funnel",
            "priority": 96,
            "condition": summary["mobile_bounce"] is not None and summary["desktop_bounce_reference"] is not None and summary["mobile_impression_share"] is not None,
        },
    ]
    return _score_insights(insights, focus_tags, user_prompt)


def _build_crm_insights(analysis: dict[str, Any], user_prompt: str) -> dict[str, Any]:
    summary = analysis["summary"]
    focus_tags = extract_focus_tags(user_prompt)
    insights = [
        {
            "id": "prior_winners_dominate_response",
            "title": "Past campaign winners are the real audience",
            "category": "offers",
            "severity": "high",
            "summary": f"Customers with heavy prior campaign wins respond at {summary['heavy_prior_response']:.1f}% versus {summary['no_prior_response']:.1f}% for customers with no previous wins.",
            "detail": "That means the current campaign engine is much better at reactivating proven responders than creating new ones.",
            "metric_label": "Prior-win lift",
            "metric_value": f"{(summary['heavy_prior_response'] or 0) - (summary['no_prior_response'] or 0):.1f} pts",
            "metric_sub": "heavy prior winners vs no prior wins",
            "tags": ["offers", "customers"],
            "section": "offers",
            "priority": 100,
        },
        {
            "id": "high_value_customers_drive_response",
            "title": "High-value customers respond disproportionately",
            "category": "customers",
            "severity": "high",
            "summary": f"Top-value customers respond at {summary['high_value_response']:.1f}% versus {summary['low_value_response']:.1f}% for the lowest-value tier.",
            "detail": "The campaign engine is working best when it reaches the customers already showing the strongest purchase depth.",
            "metric_label": "Value-tier lift",
            "metric_value": f"{(summary['high_value_response'] or 0) - (summary['low_value_response'] or 0):.1f} pts",
            "metric_sub": "top vs bottom value tier",
            "tags": ["customers"],
            "section": "customers",
            "priority": 97,
        },
        {
            "id": "high_site_visits_do_not_equal_intent",
            "title": "Heavy site visits are not converting into campaign response",
            "category": "retention",
            "severity": "medium",
            "summary": f"Low-visit customers respond at {summary['low_visit_response']:.1f}% versus {summary['high_visit_response']:.1f}% for high-visit customers.",
            "detail": "Traffic intensity alone is not a strong proxy for campaign readiness in this customer file.",
            "metric_label": "Visit-band delta",
            "metric_value": f"{(summary['low_visit_response'] or 0) - (summary['high_visit_response'] or 0):.1f} pts",
            "metric_sub": "low visits minus high visits",
            "tags": ["retention", "funnel"],
            "section": "retention",
            "priority": 92,
        },
        {
            "id": "households_with_children_underrespond",
            "title": "Households with children respond much less",
            "category": "segments",
            "severity": "medium",
            "summary": f"Customers with no children respond at {summary['no_children_response']:.1f}% versus {summary['large_household_response']:.1f}% for households with two or more children.",
            "detail": "That is a meaningful household-constraint signal the targeting model should not ignore.",
            "metric_label": "Household response gap",
            "metric_value": f"{(summary['no_children_response'] or 0) - (summary['large_household_response'] or 0):.1f} pts",
            "metric_sub": "no children vs two or more",
            "tags": ["segments", "audience"],
            "section": "segments",
            "priority": 90,
        },
    ]
    return _score_insights(insights, focus_tags, user_prompt)


def _build_lead_insights(analysis: dict[str, Any], user_prompt: str) -> dict[str, Any]:
    summary = analysis["summary"]
    focus_tags = extract_focus_tags(user_prompt)
    insights = [
        {
            "id": "lead_source_concentration",
            "title": "Lead generation is concentrated in a small set of origins",
            "category": "leads",
            "severity": "high",
            "summary": f"{summary['top_origin']} alone contributes {summary['top_origin_share']:.1f}% of leads, with {summary['second_origin']} adding another {summary['second_origin_share']:.1f}%.",
            "detail": "That concentration is efficient when it works, but it creates acquisition fragility if those sources soften.",
            "metric_label": "Top-source share",
            "metric_value": f"{summary['top_origin_share']:.1f}%",
            "metric_sub": summary["top_origin"],
            "tags": ["leads", "channels"],
            "section": "acquisition",
            "priority": 96,
        },
        {
            "id": "landing_page_concentration",
            "title": "A few landing pages control a huge share of leads",
            "category": "funnel",
            "severity": "high",
            "summary": f"The top five landing pages account for {summary['top_landing_page_share']:.1f}% of all captured leads.",
            "detail": "That makes landing-page resilience and testing far more important than the raw count of pages in the system.",
            "metric_label": "Top landing-page share",
            "metric_value": f"{summary['top_landing_page_share']:.1f}%",
            "metric_sub": "top five pages",
            "tags": ["funnel", "leads"],
            "section": "landing_pages",
            "priority": 99,
        },
        {
            "id": "unknown_source_leakage",
            "title": "Unknown source attribution is too large",
            "category": "sources",
            "severity": "medium",
            "summary": f"Unknown origin still makes up {summary['unknown_share']:.1f}% of qualified leads.",
            "detail": "That is enough unattributed demand to distort channel investment decisions if it is left unresolved.",
            "metric_label": "Unknown-source share",
            "metric_value": f"{summary['unknown_share']:.1f}%",
            "metric_sub": "of qualified leads",
            "tags": ["sources", "funnel"],
            "section": "sources",
            "priority": 91,
        },
        {
            "id": "lead_volume_spikes_are_lumpy",
            "title": "Lead volume is lumpy, not evenly distributed",
            "category": "sources",
            "severity": "medium",
            "summary": f"Peak month volume reaches {summary['top_month_leads']:,} leads versus an average of {summary['avg_monthly_leads']:.0f}.",
            "detail": "That means the acquisition engine is experiencing spikes rather than a smooth always-on rhythm.",
            "metric_label": "Peak month uplift",
            "metric_value": f"{summary['top_month_leads'] / summary['avg_monthly_leads']:.1f}x",
            "metric_sub": "peak month vs average month",
            "tags": ["sources", "leads"],
            "section": "sources",
            "priority": 88,
        },
    ]
    return _score_insights(insights, focus_tags, user_prompt)


def _build_deal_insights(analysis: dict[str, Any], user_prompt: str) -> dict[str, Any]:
    summary = analysis["summary"]
    focus_tags = extract_focus_tags(user_prompt)
    insights = [
        {
            "id": "lead_type_quality_gap",
            "title": "Lead type quality is massively uneven",
            "category": "revenue",
            "severity": "high",
            "summary": f"{summary['top_lead_type']} leads average {summary['top_lead_type_revenue']:,.0f} in declared monthly revenue, with a gap of {summary['lead_type_revenue_gap']:,.0f} to the weakest lead type.",
            "detail": "That means volume alone is a misleading KPI unless lead type quality is weighted into the funnel conversation.",
            "metric_label": "Lead-type revenue gap",
            "metric_value": f"{summary['lead_type_revenue_gap']:,.0f}",
            "metric_sub": summary["top_lead_type"],
            "tags": ["revenue", "leads"],
            "section": "revenue",
            "priority": 100,
        },
        {
            "id": "behaviour_profile_quality_signal",
            "title": "Behaviour profile is a strong revenue-quality signal",
            "category": "sales_motion",
            "severity": "high",
            "summary": f"The top behaviour profile, {summary['top_behaviour']}, averages {summary['top_behaviour_revenue']:,.0f} in declared monthly revenue.",
            "detail": "This suggests the behaviour profile is not just sales color commentary; it carries real quality signal.",
            "metric_label": "Top behaviour revenue",
            "metric_value": f"{summary['top_behaviour_revenue']:,.0f}",
            "metric_sub": summary["top_behaviour"],
            "tags": ["sales_motion", "revenue"],
            "section": "sales_motion",
            "priority": 96,
        },
        {
            "id": "manufacturers_outvalue_resellers",
            "title": "Manufacturers dramatically outvalue resellers",
            "category": "sales_motion",
            "severity": "medium",
            "summary": f"Manufacturers average {summary['manufacturer_revenue']:,.0f} in declared monthly revenue versus {summary['reseller_revenue']:,.0f} for resellers.",
            "detail": "That business-model gap is large enough that seller strategy should likely differentiate the pipeline by company type.",
            "metric_label": "Business-model revenue gap",
            "metric_value": f"{(summary['manufacturer_revenue'] or 0) - (summary['reseller_revenue'] or 0):,.0f}",
            "metric_sub": "manufacturer minus reseller",
            "tags": ["sales_motion", "revenue"],
            "section": "sales_motion",
            "priority": 92,
            "condition": summary["manufacturer_revenue"] is not None and summary["reseller_revenue"] is not None,
        },
        {
            "id": "segment_volume_concentrated",
            "title": "A few business segments dominate the wins",
            "category": "segments",
            "severity": "medium",
            "summary": f"{summary['top_segment']} is the largest won segment with {summary['top_segment_count']:,} deals.",
            "detail": "That concentration shapes where seller specialization and enablement likely matter most.",
            "metric_label": "Largest won segment",
            "metric_value": f"{summary['top_segment_count']:,}",
            "metric_sub": summary["top_segment"],
            "tags": ["segments"],
            "section": "segments",
            "priority": 87,
        },
    ]
    return _score_insights(insights, focus_tags, user_prompt)


def _score_insights(insights: list[dict[str, Any]], focus_tags: list[str], user_prompt: str) -> dict[str, Any]:
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
        analysis = analyze_marketing_context(context)
    if analysis is None:
        return None

    insight_bundle = build_marketing_insight_candidates(analysis, user_prompt)
    insights = insight_bundle["insights"]
    approved_set = set(approved_insight_ids or [item["id"] for item in insights if item.get("recommended", True)])
    approved = [item for item in insights if item["id"] in approved_set] or insights[:4]

    settings = settings or {}
    included_sections = settings.get("included_sections") or _default_sections(approved, analysis.get("profile"))
    title = settings.get("title") or "Marketing Hidden Insights"
    subtitle = settings.get("subtitle") or "Approved marketing insight narrative"
    metric_count = int(settings.get("metric_count", 4))
    show_notes = bool(settings.get("show_notes", True))

    payload = {
        "kind": "marketing_campaign",
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
        "kind": "marketing_campaign",
        "title": title,
        "html": html,
        "height": height,
        "payload": payload,
        "blueprint": payload,
        "download_name": "marketing_campaign_insights_dashboard.html",
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
    if profile == "crm":
        return CRM_SECTION_CONFIG
    if profile == "lead_generation":
        return LEAD_SECTION_CONFIG
    if profile == "closed_deals":
        return DEAL_SECTION_CONFIG
    return ATTRIBUTION_SECTION_CONFIG


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


def _detect_columns(
    df: pd.DataFrame,
    required_aliases: dict[str, list[str]],
    optional_aliases: Optional[dict[str, list[str]]] = None,
) -> Optional[dict[str, dict[str, str]]]:
    normalized = {str(column).lower().strip(): column for column in df.columns}
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


def _matching_row(frame: pd.DataFrame, labels: list[str]) -> Optional[pd.Series]:
    normalized = {str(index).strip().lower(): index for index in frame.index}
    for label in labels:
        if label.lower() in normalized:
            return frame.loc[normalized[label.lower()]]
    return None


def _matching_value(series: pd.Series, labels: list[str]) -> Optional[float]:
    normalized = {str(index).strip().lower(): float(value) for index, value in series.items()}
    for label in labels:
        if label.lower() in normalized:
            return normalized[label.lower()]
    return None


def _crm_rate_table(df: pd.DataFrame, column: str) -> dict[str, dict[str, float]]:
    grouped = (
        df.groupby(column, observed=False)
        .agg(response_rate=("response", "mean"), count=("response", "size"))
        .assign(response_rate=lambda frame: frame["response_rate"] * 100)
    )
    return {
        str(index): {"rate": float(row["response_rate"]), "count": int(row["count"])}
        for index, row in grouped.dropna(how="all").fillna(0).iterrows()
    }


def _rate_signal(table: dict[str, dict[str, float]]) -> dict[str, list[Any]]:
    return {
        "labels": list(table.keys()),
        "values": [round(item["rate"], 2) for item in table.values()],
    }


def _matching_rate(table: dict[str, dict[str, float]], labels: list[str]) -> Optional[float]:
    normalized = {key.strip().lower(): value["rate"] for key, value in table.items()}
    for label in labels:
        if label.lower() in normalized:
            return float(normalized[label.lower()])
    return None


def _first_rate(table: dict[str, dict[str, float]]) -> Optional[float]:
    return None if not table else float(next(iter(table.values()))["rate"])


def _last_rate(table: dict[str, dict[str, float]]) -> Optional[float]:
    return None if not table else float(list(table.values())[-1]["rate"])


def _normalize_binary(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.astype(float)
    if pd.api.types.is_numeric_dtype(series):
        numeric = pd.to_numeric(series, errors="coerce").fillna(0.0)
        if numeric.max() > 1:
            numeric = numeric > 0
        return numeric.astype(float)
    normalized = series.astype(str).str.strip().str.lower()
    positives = {"1", "true", "yes", "accepted", "responded"}
    return normalized.isin(positives).astype(float)


def _default_sections(insights: list[dict[str, Any]], profile: Optional[str]) -> list[str]:
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
    profile = analysis.get("profile", "attribution")
    signals = analysis["signals"]
    summary = analysis["summary"]

    if profile == "crm":
        section_map = {
            "overview": {
                "id": "overview",
                "title": "CRM Campaign Narrative",
                "description": "The fastest read on offer fatigue, customer value, and response quality.",
                "blocks": [
                    {"id": "overview-metrics", "kind": "metric_grid", "cards": [_metric_card_from_insight(card) for card in metric_cards]},
                    {"id": "overview-insights", "kind": "insight_grid", "insights": [_insight_card(insight) for insight in approved_insights[:4]]},
                ],
            },
            "offers": {
                "id": "offers",
                "title": "Offer History and Response",
                "description": "Campaign response is being concentrated among customers who have already proven they convert.",
                "blocks": [
                    {"id": "response-by-offer-history", "kind": "chart", "chart": {"id": "response-by-offer-history", "title": "Response by prior campaign wins", "subtitle": "Offer history is doing more predictive work than a generic blast strategy.", "type": "bar", "labels": signals["response_by_offer_history"]["labels"], "series": [{"name": "Response rate", "values": signals["response_by_offer_history"]["values"], "color": "#c2410c"}], "format": "percent"}},
                    {"id": "response-by-deal", "kind": "chart", "chart": {"id": "response-by-deal", "title": "Response by discount-deal intensity", "subtitle": "Discount exposure does not map cleanly to campaign readiness.", "type": "bar", "labels": signals["response_by_deal"]["labels"], "series": [{"name": "Response rate", "values": signals["response_by_deal"]["values"], "color": "#9a3412"}], "format": "percent"}},
                ],
            },
            "customers": {
                "id": "customers",
                "title": "Customer Value and Intent",
                "description": "The strongest responders are typically the customers with deeper existing value, not just the ones browsing most.",
                "blocks": [
                    {"id": "response-by-value", "kind": "chart", "chart": {"id": "response-by-value", "title": "Response by customer value tier", "subtitle": "High-value customers are carrying the campaign response curve.", "type": "bar", "labels": signals["response_by_value"]["labels"], "series": [{"name": "Response rate", "values": signals["response_by_value"]["values"], "color": "#166534"}], "format": "percent"}},
                    {"id": "response-by-web-visits", "kind": "chart", "chart": {"id": "response-by-web-visits", "title": "Response by web-visit intensity", "subtitle": "Site traffic is not a perfect proxy for campaign intent.", "type": "bar", "labels": signals["response_by_web_visits"]["labels"], "series": [{"name": "Response rate", "values": signals["response_by_web_visits"]["values"], "color": "#0f766e"}], "format": "percent"}},
                ],
            },
            "segments": {
                "id": "segments",
                "title": "Household and Income Segments",
                "description": "Household constraints and affluence shape who is actually reachable by the campaign engine.",
                "blocks": [
                    {"id": "response-by-income", "kind": "chart", "chart": {"id": "response-by-income", "title": "Response by income tier", "subtitle": "Top-income groups are often the only place where response meaningfully lifts.", "type": "bar", "labels": signals["response_by_income"]["labels"], "series": [{"name": "Response rate", "values": signals["response_by_income"]["values"], "color": "#b45309"}], "format": "percent"}},
                    {"id": "response-by-household", "kind": "chart", "chart": {"id": "response-by-household", "title": "Response by household child count", "subtitle": "Household load is a real segmentation variable, not just demographic decoration.", "type": "bar", "labels": signals["response_by_household"]["labels"], "series": [{"name": "Response rate", "values": signals["response_by_household"]["values"], "color": "#57534e"}], "format": "percent"}},
                ],
            },
            "retention": {
                "id": "retention",
                "title": "Recency and Campaign Fatigue",
                "description": "Engagement intensity and re-targeting assumptions need more nuance than standard CRM rules usually allow.",
                "blocks": [
                    {"id": "crm-stats", "kind": "stat_list", "title": "CRM markers", "items": [
                        {"label": "Customers", "value": f"{summary['customer_count']:,}", "tone": "default"},
                        {"label": "Response rate", "value": f"{summary['response_rate']:.1f}%", "tone": "positive"},
                        {"label": "Average income", "value": f"${summary['avg_income']:,.0f}", "tone": "default"},
                        {"label": "Average recency", "value": f"{summary['avg_recency']:.0f} days", "tone": "default"},
                    ]},
                ],
            },
            "notes": {"id": "notes", "title": "Approved Insight Notes", "description": "Narrative notes that help the dashboard travel well with CRM and lifecycle teams.", "blocks": [{"id": "approved-note-list", "kind": "note_list", "insights": [_insight_card(insight) for insight in approved_insights]}]},
        }
    elif profile == "lead_generation":
        section_map = {
            "overview": {
                "id": "overview",
                "title": "Lead Generation Narrative",
                "description": "The fastest read on source concentration, landing-page dependency, and top-of-funnel volatility.",
                "blocks": [
                    {"id": "overview-metrics", "kind": "metric_grid", "cards": [_metric_card_from_insight(card) for card in metric_cards]},
                    {"id": "overview-insights", "kind": "insight_grid", "insights": [_insight_card(insight) for insight in approved_insights[:4]]},
                ],
            },
            "acquisition": {
                "id": "acquisition",
                "title": "Lead Source Mix",
                "description": "A small set of origins is doing most of the acquisition work.",
                "blocks": [
                    {"id": "origin-share", "kind": "chart", "chart": {"id": "origin-share", "title": "Lead share by origin", "subtitle": "This is the fastest read on acquisition dependence.", "type": "bar", "labels": signals["origin_share"]["labels"], "series": [{"name": "Lead share", "values": signals["origin_share"]["values"], "color": "#c2410c"}], "format": "percent"}},
                ],
            },
            "landing_pages": {
                "id": "landing_pages",
                "title": "Landing Page Concentration",
                "description": "A few pages are carrying an outsized share of lead generation.",
                "blocks": [
                    {"id": "landing-page-counts", "kind": "chart", "chart": {"id": "landing-page-counts", "title": "Top landing pages by lead volume", "subtitle": "Landing page resilience matters more when the portfolio is this concentrated.", "type": "bar", "labels": signals["landing_page_counts"]["labels"], "series": [{"name": "Leads", "values": signals["landing_page_counts"]["values"], "color": "#166534"}], "format": "number"}},
                ],
            },
            "sources": {
                "id": "sources",
                "title": "Source Quality and Seasonality",
                "description": "Even with a strong source mix, unlabeled demand and monthly spikes can distort the acquisition story.",
                "blocks": [
                    {"id": "leads-by-month", "kind": "chart", "chart": {"id": "leads-by-month", "title": "Leads by month", "subtitle": "This reveals whether the machine is always-on or riding bursts.", "type": "line", "labels": signals["leads_by_month"]["labels"], "series": [{"name": "Leads", "values": signals["leads_by_month"]["values"], "color": "#0f766e"}], "format": "number"}},
                ],
            },
            "notes": {"id": "notes", "title": "Approved Insight Notes", "description": "Narrative notes that help the dashboard travel well with growth and acquisition teams.", "blocks": [{"id": "approved-note-list", "kind": "note_list", "insights": [_insight_card(insight) for insight in approved_insights]}]},
        }
    elif profile == "closed_deals":
        section_map = {
            "overview": {
                "id": "overview",
                "title": "Closed Deals Narrative",
                "description": "The fastest read on segment mix, lead quality, and revenue concentration inside closed marketing-sourced deals.",
                "blocks": [
                    {"id": "overview-metrics", "kind": "metric_grid", "cards": [_metric_card_from_insight(card) for card in metric_cards]},
                    {"id": "overview-insights", "kind": "insight_grid", "insights": [_insight_card(insight) for insight in approved_insights[:4]]},
                ],
            },
            "segments": {
                "id": "segments",
                "title": "Business Segment Concentration",
                "description": "Closed-won volume is not evenly distributed across the segment book.",
                "blocks": [
                    {"id": "segment-counts", "kind": "chart", "chart": {"id": "segment-counts", "title": "Won deals by business segment", "subtitle": "Segment concentration changes where specialization and enablement should land.", "type": "bar", "labels": signals["segment_counts"]["labels"], "series": [{"name": "Won deals", "values": signals["segment_counts"]["values"], "color": "#c2410c"}], "format": "number"}},
                ],
            },
            "revenue": {
                "id": "revenue",
                "title": "Revenue Quality by Lead Type",
                "description": "Lead quality is uneven enough that pipeline volume without revenue weighting is misleading.",
                "blocks": [
                    {"id": "revenue-by-lead-type", "kind": "chart", "chart": {"id": "revenue-by-lead-type", "title": "Declared monthly revenue by lead type", "subtitle": "Lead type carries major quality differences in the closed book.", "type": "bar", "labels": signals["revenue_by_lead_type"]["labels"], "series": [{"name": "Declared monthly revenue", "values": signals["revenue_by_lead_type"]["values"], "color": "#166534"}], "format": "currency"}},
                ],
            },
            "sales_motion": {
                "id": "sales_motion",
                "title": "Behaviour and Business Model Quality",
                "description": "Behaviour profiles and business models are signaling far more than just sales-color commentary.",
                "blocks": [
                    {"id": "revenue-by-behaviour", "kind": "chart", "chart": {"id": "revenue-by-behaviour", "title": "Declared monthly revenue by behaviour profile", "subtitle": "Behaviour labels are carrying real quality signal in the deal book.", "type": "bar", "labels": signals["revenue_by_behaviour"]["labels"], "series": [{"name": "Declared monthly revenue", "values": signals["revenue_by_behaviour"]["values"], "color": "#9a3412"}], "format": "currency"}},
                    {"id": "revenue-by-business-type", "kind": "chart", "chart": {"id": "revenue-by-business-type", "title": "Declared monthly revenue by business type", "subtitle": "Company model helps explain why similar win counts can produce very different value.", "type": "bar", "labels": signals["revenue_by_business_type"]["labels"], "series": [{"name": "Declared monthly revenue", "values": signals["revenue_by_business_type"]["values"], "color": "#0f766e"}], "format": "currency"}},
                ],
            },
            "notes": {"id": "notes", "title": "Approved Insight Notes", "description": "Narrative notes that help the dashboard travel well with demand gen and sales leadership.", "blocks": [{"id": "approved-note-list", "kind": "note_list", "insights": [_insight_card(insight) for insight in approved_insights]}]},
        }
    else:
        section_map = {
            "overview": {
                "id": "overview",
                "title": "Marketing Narrative",
                "description": "The fastest read on allocation efficiency, testing quality, and conversion leakage.",
                "blocks": [
                    {"id": "overview-metrics", "kind": "metric_grid", "cards": [_metric_card_from_insight(card) for card in metric_cards]},
                    {"id": "overview-insights", "kind": "insight_grid", "insights": [_insight_card(insight) for insight in approved_insights[:4]]},
                ],
            },
            "channels": {
                "id": "channels",
                "title": "Budget and Channel Efficiency",
                "description": "The budget story matters most when spend share and ROAS are read together.",
                "blocks": [
                    {"id": "channel-spend-share", "kind": "chart", "chart": {"id": "channel-spend-share", "title": "Spend share by channel", "subtitle": "Budget concentration is often the hidden source of portfolio underperformance.", "type": "bar", "labels": signals["channel_spend_share"]["labels"], "series": [{"name": "Spend share", "values": signals["channel_spend_share"]["values"], "color": "#c2410c"}], "format": "percent"}},
                    {"id": "channel-roas", "kind": "chart", "chart": {"id": "channel-roas", "title": "ROAS by channel", "subtitle": "This is the cleanest read on where the portfolio is over- or under-allocating.", "type": "bar", "labels": signals["channel_roas"]["labels"], "series": [{"name": "ROAS", "values": signals["channel_roas"]["values"], "color": "#166534"}], "format": "number"}},
                ],
            },
            "testing": {
                "id": "testing",
                "title": "Experiments and Device Performance",
                "description": "Creative testing and device context are shaping return more than surface-level reporting suggests.",
                "blocks": [
                    {"id": "experiment-roas", "kind": "chart", "chart": {"id": "experiment-roas", "title": "ROAS by experiment group", "subtitle": "If the control wins, the testing cycle is not creating lift.", "type": "bar", "labels": signals["experiment_roas"]["labels"], "series": [{"name": "ROAS", "values": signals["experiment_roas"]["values"], "color": "#9a3412"}], "format": "number"}},
                    {"id": "device-roas", "kind": "chart", "chart": {"id": "device-roas", "title": "ROAS by device", "subtitle": "Device context is carrying intent and post-view differences the attribution stack may miss.", "type": "bar", "labels": signals["device_roas"]["labels"], "series": [{"name": "ROAS", "values": signals["device_roas"]["values"], "color": "#0f766e"}], "format": "number"}},
                ],
            },
            "audience": {
                "id": "audience",
                "title": "Audience Targeting",
                "description": "If age targeting barely moves ROAS, the portfolio needs a different segmentation thesis.",
                "blocks": [
                    {"id": "age-group-roas", "kind": "chart", "chart": {"id": "age-group-roas", "title": "ROAS by age group", "subtitle": "Small spread here means audience age is not the biggest lever in the system.", "type": "bar", "labels": signals["age_group_roas"]["labels"], "series": [{"name": "ROAS", "values": signals["age_group_roas"]["values"], "color": "#b45309"}], "format": "number"}},
                ],
            },
            "funnel": {
                "id": "funnel",
                "title": "Bounce and Impression Leakage",
                "description": "When impressions pile up on weak post-click experiences, the leak matters more than adding new top-of-funnel volume.",
                "blocks": [
                    {"id": "device-bounce", "kind": "chart", "chart": {"id": "device-bounce", "title": "Bounce rate by device", "subtitle": "This is the best quick read on landing-page friction across the media mix.", "type": "bar", "labels": signals["device_bounce"]["labels"], "series": [{"name": "Bounce rate", "values": signals["device_bounce"]["values"], "color": "#292524"}], "format": "number"}},
                    {"id": "device-impression-share", "kind": "chart", "chart": {"id": "device-impression-share", "title": "Impression share by device", "subtitle": "Large device concentration amplifies whatever post-click quality problem exists there.", "type": "bar", "labels": signals["device_impression_share"]["labels"], "series": [{"name": "Impression share", "values": signals["device_impression_share"]["values"], "color": "#57534e"}], "format": "percent"}},
                    {"id": "marketing-stats", "kind": "stat_list", "title": "Portfolio markers", "items": [
                        {"label": "Total spend", "value": f"${summary['total_spend']:,.0f}", "tone": "default"},
                        {"label": "Total revenue", "value": f"${summary['total_revenue']:,.0f}", "tone": "default"},
                        {"label": "Overall ROAS", "value": f"{summary['overall_roas']:.2f}x", "tone": "positive"},
                        {"label": "Campaigns", "value": f"{summary['campaign_count']:,}", "tone": "default"},
                    ]},
                ],
            },
            "notes": {"id": "notes", "title": "Approved Insight Notes", "description": "Narrative notes that help the dashboard travel well with marketing and growth stakeholders.", "blocks": [{"id": "approved-note-list", "kind": "note_list", "insights": [_insight_card(insight) for insight in approved_insights]}]},
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
          <div class="kicker">Marketing insight dashboard</div>
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
