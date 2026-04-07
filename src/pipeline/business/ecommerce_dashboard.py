from __future__ import annotations

import json
from typing import Any, Optional

import pandas as pd
from jinja2 import Template

from pipeline.common.contracts import PipelineContext


REQUIRED_COLUMN_ALIASES = {
    "date": ["order_date", "date", "purchase_date", "transaction_date"],
    "revenue": ["order_value", "revenue", "sales", "order_total", "total_amount", "amount"],
    "category": ["category", "product_category", "department"],
    "channel": ["channel", "traffic_source", "source", "marketing_channel"],
    "payment": ["payment_method", "payment", "payment_type"],
    "device": ["device", "device_type"],
    "discount": ["discount_pct", "discount_percent", "discount", "discount_rate"],
    "returned": ["returned", "is_returned", "return_flag", "return_status"],
}

OPTIONAL_COLUMN_ALIASES = {
    "customer_id": ["customer_id", "customer", "user_id"],
    "customer_type": ["customer_type", "customer_segment", "buyer_type"],
    "order_id": ["order_id", "transaction_id"],
}

FOCUS_KEYWORDS = {
    "revenue": ["revenue", "sales", "growth", "holiday", "monthly"],
    "returns": ["returns", "refund", "return rate", "return"],
    "discounts": ["discount", "promo", "promotion", "coupon"],
    "channels": ["channel", "traffic", "source", "marketing"],
    "payments": ["payment", "paypal", "bnpl", "apple pay", "credit card"],
    "categories": ["category", "clothing", "electronics", "product mix"],
    "devices": ["device", "mobile", "desktop", "tablet"],
    "customers": ["repeat", "retention", "new customer", "loyalty"],
}

SECTION_CONFIG = {
    "overview": "Narrative KPI and standout findings",
    "revenue": "Monthly revenue and AOV timing",
    "returns": "Return behavior by category and payment",
    "channels": "Channel and device performance",
    "discounts": "Discount efficiency section",
    "notes": "Approved insight notes section",
}


def analyze_ecommerce_context(context: PipelineContext) -> Optional[dict[str, Any]]:
    df = context.clean_df if context.clean_df is not None else context.raw_df
    if df is None or df.empty:
        return None

    detected = _detect_columns(df)
    if detected is None:
        return None

    working = pd.DataFrame(
        {
            "date": pd.to_datetime(df[detected["required"]["date"]], errors="coerce"),
            "revenue": pd.to_numeric(df[detected["required"]["revenue"]], errors="coerce"),
            "category": df[detected["required"]["category"]].astype(str).str.strip(),
            "channel": df[detected["required"]["channel"]].astype(str).str.strip(),
            "payment": df[detected["required"]["payment"]].astype(str).str.strip(),
            "device": df[detected["required"]["device"]].astype(str).str.strip(),
            "discount_pct": _normalize_discount_series(df[detected["required"]["discount"]]),
            "returned": _normalize_return_series(df[detected["required"]["returned"]]),
        }
    ).dropna(subset=["date", "revenue"])

    if working.empty or len(working) < 20:
        return None

    if detected["optional"].get("customer_id"):
        working["customer_id"] = df.loc[working.index, detected["optional"]["customer_id"]].astype(str).str.strip()
    else:
        working["customer_id"] = None

    if detected["optional"].get("customer_type"):
        working["customer_type"] = df.loc[working.index, detected["optional"]["customer_type"]].astype(str).str.strip().str.lower()
    else:
        working["customer_type"] = None

    working = working.sort_values("date").reset_index(drop=True)
    working["year"] = working["date"].dt.year
    working["month"] = working["date"].dt.strftime("%b")
    working["month_num"] = working["date"].dt.month
    working["weekday"] = pd.Categorical(
        working["date"].dt.day_name(),
        categories=["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
        ordered=True,
    )
    working["discount_band"] = working["discount_pct"].apply(_discount_band)

    total_revenue = float(working["revenue"].sum())
    avg_order_value = float(working["revenue"].mean())
    overall_return_rate = float(working["returned"].mean() * 100)
    repeat_order_share = _repeat_order_share(working)

    monthly_revenue = (
        working.groupby(["year", "month_num", "month"])["revenue"].sum().reset_index().sort_values(["year", "month_num"])
    )
    latest_years = sorted(monthly_revenue["year"].unique())[-2:]
    monthly_chart = {
        str(year): (
            monthly_revenue[monthly_revenue["year"] == year]
            .set_index("month_num")
            .reindex(range(1, 13), fill_value=0)
            .assign(month=lambda frame: [pd.Timestamp(2000, idx, 1).strftime("%b") for idx in frame.index])
        )
        for year in latest_years
    }

    category_returns = (
        working.groupby("category")
        .agg(order_count=("revenue", "size"), return_rate=("returned", "mean"))
        .assign(return_rate=lambda frame: frame["return_rate"] * 100)
        .sort_values("return_rate", ascending=False)
    )
    channel_aov = working.groupby("channel")["revenue"].mean().sort_values(ascending=False)
    weekday_aov = working.groupby("weekday", observed=False)["revenue"].mean().reindex(
        ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    )
    discount_metrics = (
        working.groupby("discount_band")
        .agg(avg_revenue=("revenue", "mean"), return_rate=("returned", "mean"), orders=("revenue", "size"))
        .assign(return_rate=lambda frame: frame["return_rate"] * 100)
        .reindex(["No discount", "1-10%", "11-20%", "21%+"])
        .fillna(0)
    )
    payment_returns = (
        working.groupby("payment")["returned"].mean().mul(100).sort_values(ascending=False)
    )
    device_mix = working["device"].value_counts(normalize=True).mul(100).sort_values(ascending=False)
    category_mix = working["category"].value_counts().sort_values(ascending=False)

    direct_referral_aov = channel_aov[channel_aov.index.str.lower().isin(["direct", "referral"])]
    organic_aov = channel_aov[channel_aov.index.str.lower().isin(["organic", "organic search"])]
    best_day = weekday_aov.idxmax() if not weekday_aov.empty else "Friday"
    worst_day = weekday_aov.idxmin() if not weekday_aov.empty else "Sunday"
    best_payment = payment_returns.idxmin() if not payment_returns.empty else "BNPL"
    worst_payment = payment_returns.idxmax() if not payment_returns.empty else "PayPal"
    clothing_return_rate = _match_series_value(category_returns["return_rate"], "clothing")
    discount_return_spread = float(discount_metrics["return_rate"].max() - discount_metrics["return_rate"].min())

    start_date = working["date"].min()
    end_date = working["date"].max()

    return {
        "kind": "ecommerce_orders",
        "column_map": detected,
        "dataset": {
            "row_count": int(len(working)),
            "column_count": int(len(df.columns)),
            "start_date": start_date,
            "end_date": end_date,
            "start_year": int(start_date.year),
            "end_year": int(end_date.year),
            "input_columns": list(df.columns),
        },
        "summary": {
            "total_revenue": total_revenue,
            "total_revenue_display": _compact_currency(total_revenue),
            "avg_order_value": avg_order_value,
            "avg_order_value_display": _currency(avg_order_value),
            "return_rate": overall_return_rate,
            "repeat_order_share": repeat_order_share,
            "best_day": best_day,
            "best_day_aov": float(weekday_aov.max()) if not weekday_aov.empty else 0.0,
            "worst_day": worst_day,
            "worst_day_aov": float(weekday_aov.min()) if not weekday_aov.empty else 0.0,
            "direct_referral_aov": float(direct_referral_aov.mean()) if not direct_referral_aov.empty else None,
            "organic_aov": float(organic_aov.mean()) if not organic_aov.empty else None,
            "best_payment": best_payment,
            "best_payment_return": float(payment_returns.min()) if not payment_returns.empty else None,
            "worst_payment": worst_payment,
            "worst_payment_return": float(payment_returns.max()) if not payment_returns.empty else None,
            "clothing_return_rate": clothing_return_rate,
            "discount_return_spread": discount_return_spread,
        },
        "signals": {
            "monthly_revenue": {
                "labels": [pd.Timestamp(2000, month, 1).strftime("%b") for month in range(1, 13)],
                "series": {
                    str(year): [round(float(value), 2) for value in frame["revenue"].tolist()]
                    for year, frame in {
                        year: data.assign(revenue=lambda x: x["revenue"].fillna(0.0))
                        for year, data in monthly_chart.items()
                    }.items()
                },
            },
            "category_returns": {
                "labels": category_returns.index.tolist()[:7],
                "values": [round(float(value), 2) for value in category_returns["return_rate"].tolist()[:7]],
            },
            "channel_aov": {
                "labels": channel_aov.index.tolist(),
                "values": [round(float(value), 2) for value in channel_aov.tolist()],
            },
            "weekday_aov": {
                "labels": weekday_aov.index.tolist(),
                "values": [round(float(value), 2) for value in weekday_aov.fillna(0).tolist()],
            },
            "discount_revenue": {
                "labels": discount_metrics.index.tolist(),
                "values": [round(float(value), 2) for value in discount_metrics["avg_revenue"].tolist()],
                "return_values": [round(float(value), 2) for value in discount_metrics["return_rate"].tolist()],
            },
            "category_mix": {
                "labels": category_mix.index.tolist()[:7],
                "values": [int(value) for value in category_mix.tolist()[:7]],
            },
            "payment_returns": {
                "labels": payment_returns.index.tolist(),
                "values": [round(float(value), 2) for value in payment_returns.tolist()],
            },
            "device_mix": {
                "labels": device_mix.index.tolist(),
                "values": [round(float(value), 1) for value in device_mix.tolist()],
            },
        },
    }


def build_ecommerce_insight_candidates(analysis: dict[str, Any], user_prompt: str = "") -> dict[str, Any]:
    focus_tags = extract_focus_tags(user_prompt)
    summary = analysis["summary"]
    dataset = analysis["dataset"]
    signals = analysis["signals"]

    insights = [
        {
            "id": "discount_paradox",
            "title": "Discounts may be destroying margin without reducing returns",
            "category": "discounts",
            "severity": "high",
            "summary": "Return rate barely moves across discount bands, while average order value drops as discounts deepen.",
            "detail": f"Return-rate spread across discount bands is only {summary['discount_return_spread']:.2f} percentage points.",
            "metric_label": "Discount return spread",
            "metric_value": f"{summary['discount_return_spread']:.2f} pts",
            "metric_sub": "flat returns across discount levels",
            "tags": ["discounts", "returns"],
            "section": "discounts",
            "priority": 98,
            "condition": summary["discount_return_spread"] <= 2.5,
        },
        {
            "id": "friday_effect",
            "title": "Late-week shoppers are worth more",
            "category": "revenue",
            "severity": "medium",
            "summary": f"{summary['best_day']} has the highest average order value at {_currency(summary['best_day_aov'])}.",
            "detail": f"{summary['worst_day']} is the weakest day at {_currency(summary['worst_day_aov'])}.",
            "metric_label": "Best weekday AOV",
            "metric_value": _currency(summary["best_day_aov"]),
            "metric_sub": f"{summary['best_day']} vs {summary['worst_day']}",
            "tags": ["revenue"],
            "section": "revenue",
            "priority": 90,
        },
        {
            "id": "channel_winner",
            "title": "High-intent traffic is outperforming discovery traffic",
            "category": "channels",
            "severity": "medium",
            "summary": (
                f"Direct/referral traffic averages {_currency(summary['direct_referral_aov']) if summary['direct_referral_aov'] is not None else 'n/a'} "
                f"per order versus {_currency(summary['organic_aov']) if summary['organic_aov'] is not None else 'n/a'} for organic."
            ),
            "detail": "This suggests your highest-quality traffic may be coming from the channels that already know you.",
            "metric_label": "Direct/referral AOV",
            "metric_value": _currency(summary["direct_referral_aov"]) if summary["direct_referral_aov"] is not None else "n/a",
            "metric_sub": "vs organic search",
            "tags": ["channels", "revenue"],
            "section": "channels",
            "priority": 88,
            "condition": summary["direct_referral_aov"] is not None and summary["organic_aov"] is not None,
        },
        {
            "id": "payment_myth",
            "title": "Payment-method risk looks different from intuition",
            "category": "payments",
            "severity": "medium",
            "summary": (
                f"{summary['best_payment']} has the lowest return rate, while {summary['worst_payment']} has the highest."
            ),
            "detail": "That suggests payment preference is carrying behavioral signal, not just checkout friction.",
            "metric_label": "Best payment return rate",
            "metric_value": f"{summary['best_payment_return']:.1f}%" if summary["best_payment_return"] is not None else "n/a",
            "metric_sub": f"{summary['best_payment']} best, {summary['worst_payment']} worst",
            "tags": ["payments", "returns"],
            "section": "returns",
            "priority": 86,
            "condition": summary["best_payment_return"] is not None and summary["worst_payment_return"] is not None,
        },
        {
            "id": "clothing_return_bomb",
            "title": "One category is carrying disproportionate return pain",
            "category": "categories",
            "severity": "high",
            "summary": (
                f"Clothing returns at {summary['clothing_return_rate']:.1f}% versus a store-wide average of {summary['return_rate']:.1f}%."
                if summary["clothing_return_rate"] is not None
                else "Category-level returns are uneven and worth dashboard attention."
            ),
            "detail": "This is the kind of hidden cost that can erase margin even when top-line revenue looks healthy.",
            "metric_label": "Clothing return rate",
            "metric_value": f"{summary['clothing_return_rate']:.1f}%" if summary["clothing_return_rate"] is not None else "n/a",
            "metric_sub": "vs store average",
            "tags": ["categories", "returns"],
            "section": "returns",
            "priority": 99,
            "condition": summary["clothing_return_rate"] is not None and summary["clothing_return_rate"] >= summary["return_rate"] + 4,
        },
        {
            "id": "repeat_base",
            "title": "Revenue is being carried by a repeat-heavy customer base",
            "category": "customers",
            "severity": "low",
            "summary": f"Repeat orders account for {summary['repeat_order_share']:.1f}% of the dataset.",
            "detail": "That gives you a strong customer base, but also means retention assumptions should be tested carefully.",
            "metric_label": "Repeat-order share",
            "metric_value": f"{summary['repeat_order_share']:.1f}%",
            "metric_sub": f"{dataset['row_count']:,} total orders",
            "tags": ["customers"],
            "section": "overview",
            "priority": 78,
            "condition": summary["repeat_order_share"] is not None,
        },
    ]

    filtered = [item for item in insights if item.get("condition", True)]
    for item in filtered:
        focus_bonus = 15 if any(tag in focus_tags for tag in item["tags"]) else 0
        item["score"] = item["priority"] + focus_bonus
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
        analysis = analyze_ecommerce_context(context)
    if analysis is None:
        return None

    insight_bundle = build_ecommerce_insight_candidates(analysis, user_prompt)
    insights = insight_bundle["insights"]
    approved_set = set(approved_insight_ids or [item["id"] for item in insights if item.get("recommended", True)])
    approved = [item for item in insights if item["id"] in approved_set] or insights[:4]

    settings = settings or {}
    included_sections = settings.get("included_sections") or _default_sections(approved)
    title = settings.get("title") or "E-commerce Hidden Insights"
    subtitle = settings.get("subtitle") or (
        f"Approved insight blueprint across {analysis['dataset']['start_year']} to {analysis['dataset']['end_year']}"
    )
    metric_count = int(settings.get("metric_count", 4))
    show_notes = bool(settings.get("show_notes", True))

    payload = {
        "kind": "ecommerce_orders",
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
    height = 1080 + (140 * len(included_sections)) + (160 if show_notes else 0)
    return {
        "kind": "ecommerce_orders",
        "title": title,
        "html": html,
        "height": height,
        "blueprint": payload,
        "payload": payload,
        "download_name": "ecommerce_insights_dashboard.html",
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


def dashboard_section_options() -> dict[str, str]:
    return SECTION_CONFIG


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


def _normalize_discount_series(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").fillna(0.0)
    if numeric.max() <= 1.0:
        numeric = numeric * 100
    return numeric.clip(lower=0.0)


def _normalize_return_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.astype(float)
    if pd.api.types.is_numeric_dtype(series):
        numeric = pd.to_numeric(series, errors="coerce").fillna(0.0)
        if numeric.max() > 1:
            numeric = numeric > 0
        return numeric.astype(float)
    normalized = series.astype(str).str.strip().str.lower()
    positives = {"1", "true", "yes", "returned", "return", "complete_return"}
    return normalized.isin(positives).astype(float)


def _discount_band(value: float) -> str:
    if value <= 0:
        return "No discount"
    if value <= 10:
        return "1-10%"
    if value <= 20:
        return "11-20%"
    return "21%+"


def _repeat_order_share(df: pd.DataFrame) -> Optional[float]:
    if "customer_type" in df.columns and df["customer_type"].notna().any():
        lowered = df["customer_type"].fillna("").astype(str).str.lower()
        repeat_mask = lowered.str.contains("repeat|returning|existing")
        if repeat_mask.any():
            return float(repeat_mask.mean() * 100)
    if "customer_id" in df.columns and df["customer_id"].notna().any():
        counts = df.groupby("customer_id").size()
        repeat_ids = set(counts[counts > 1].index)
        return float(df["customer_id"].isin(repeat_ids).mean() * 100)
    return None


def _match_series_value(series: pd.Series, label: str) -> Optional[float]:
    normalized = {str(index).strip().lower(): float(value) for index, value in series.items()}
    return normalized.get(label.lower())


def _currency(value: float) -> str:
    return f"${value:,.0f}"


def _compact_currency(value: float) -> str:
    if value >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value/1_000:.0f}K"
    return _currency(value)


def _default_sections(insights: list[dict[str, Any]]) -> list[str]:
    sections = ["overview"]
    for section in ["revenue", "returns", "channels", "discounts", "notes"]:
        if any(item["section"] == section or section in item["tags"] for item in insights):
            sections.append(section)
    return list(dict.fromkeys(sections))


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

    latest_years = list(signals["monthly_revenue"]["series"].keys())
    monthly_series = [
        {"name": year, "values": values, "color": "#3B82F6" if index == 0 else "#0F172A"}
        for index, (year, values) in enumerate(signals["monthly_revenue"]["series"].items())
    ]

    section_map = {
        "overview": {
            "id": "overview",
            "title": "Commerce Narrative",
            "description": "A quick read on the commercial story before anyone opens raw order tables.",
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
        "revenue": {
            "id": "revenue",
            "title": "Revenue Patterns",
            "description": "Timing effects that influence top-line performance and buyer quality.",
            "blocks": [
                {
                    "id": "monthly-revenue",
                    "kind": "chart",
                    "chart": {
                        "id": "monthly-revenue",
                        "title": "Monthly revenue",
                        "subtitle": f"Comparing the latest years in the dataset: {', '.join(latest_years)}.",
                        "type": "line",
                        "labels": signals["monthly_revenue"]["labels"],
                        "series": monthly_series,
                        "format": "currency",
                    },
                },
                {
                    "id": "weekday-aov",
                    "kind": "chart",
                    "chart": {
                        "id": "weekday-aov",
                        "title": "Average order value by weekday",
                        "subtitle": f"{summary['best_day']} leads while {summary['worst_day']} lags.",
                        "type": "bar",
                        "labels": signals["weekday_aov"]["labels"],
                        "series": [
                            {
                                "name": "AOV",
                                "values": signals["weekday_aov"]["values"],
                                "color": "#10B981",
                            }
                        ],
                        "format": "currency",
                    },
                },
            ],
        },
        "returns": {
            "id": "returns",
            "title": "Return Risk",
            "description": "Where return behavior is quietly eroding margin.",
            "blocks": [
                {
                    "id": "category-returns",
                    "kind": "chart",
                    "chart": {
                        "id": "category-returns",
                        "title": "Return rate by category",
                        "subtitle": "Some categories carry a disproportionate share of return pain.",
                        "type": "bar",
                        "labels": signals["category_returns"]["labels"],
                        "series": [
                            {
                                "name": "Return rate",
                                "values": signals["category_returns"]["values"],
                                "color": "#EF4444",
                            }
                        ],
                        "format": "percent",
                    },
                },
                {
                    "id": "payment-returns",
                    "kind": "chart",
                    "chart": {
                        "id": "payment-returns",
                        "title": "Return rate by payment method",
                        "subtitle": "Payment mix can signal underlying customer behavior, not just checkout preference.",
                        "type": "bar",
                        "labels": signals["payment_returns"]["labels"],
                        "series": [
                            {
                                "name": "Return rate",
                                "values": signals["payment_returns"]["values"],
                                "color": "#F59E0B",
                            }
                        ],
                        "format": "percent",
                    },
                },
            ],
        },
        "channels": {
            "id": "channels",
            "title": "Channel and Device Quality",
            "description": "Where high-intent customers are showing up and how they behave.",
            "blocks": [
                {
                    "id": "channel-aov",
                    "kind": "chart",
                    "chart": {
                        "id": "channel-aov",
                        "title": "Average order value by channel",
                        "subtitle": "High-intent traffic often looks better here than it does in raw volume metrics.",
                        "type": "bar",
                        "labels": signals["channel_aov"]["labels"],
                        "series": [
                            {
                                "name": "AOV",
                                "values": signals["channel_aov"]["values"],
                                "color": "#3B82F6",
                            }
                        ],
                        "format": "currency",
                    },
                },
                {
                    "id": "device-mix",
                    "kind": "chart",
                    "chart": {
                        "id": "device-mix",
                        "title": "Device mix",
                        "subtitle": "Device share helps frame acquisition and checkout behavior.",
                        "type": "pie",
                        "labels": signals["device_mix"]["labels"],
                        "series": [
                            {
                                "name": "Share",
                                "values": signals["device_mix"]["values"],
                                "color": "#0F172A",
                            }
                        ],
                        "format": "percent",
                    },
                },
                {
                    "id": "category-mix",
                    "kind": "chart",
                    "chart": {
                        "id": "category-mix",
                        "title": "Order mix by category",
                        "subtitle": "This shows what is driving the store, not just what is loudest in revenue headlines.",
                        "type": "bar",
                        "labels": signals["category_mix"]["labels"],
                        "series": [
                            {
                                "name": "Orders",
                                "values": signals["category_mix"]["values"],
                                "color": "#8B5CF6",
                            }
                        ],
                        "format": "number",
                    },
                },
            ],
        },
        "discounts": {
            "id": "discounts",
            "title": "Discount Efficiency",
            "description": "Promotional behavior viewed through revenue quality instead of top-line activity alone.",
            "blocks": [
                {
                    "id": "discount-revenue",
                    "kind": "chart",
                    "chart": {
                        "id": "discount-revenue",
                        "title": "Average revenue by discount band",
                        "subtitle": "This is the quickest read on whether deeper discounts are earning their place.",
                        "type": "bar",
                        "labels": signals["discount_revenue"]["labels"],
                        "series": [
                            {
                                "name": "Average revenue",
                                "values": signals["discount_revenue"]["values"],
                                "color": "#F59E0B",
                            }
                        ],
                        "format": "currency",
                    },
                },
                {
                    "id": "discount-return-spread",
                    "kind": "stat_list",
                    "title": "Promotion signals",
                    "items": [
                        {"label": "Discount return spread", "value": f"{summary['discount_return_spread']:.2f} pts", "tone": "warning"},
                        {"label": "Best weekday AOV", "value": _currency(summary["best_day_aov"]), "tone": "positive"},
                        {"label": "Worst weekday AOV", "value": _currency(summary["worst_day_aov"]), "tone": "default"},
                    ],
                },
            ],
        },
        "notes": {
            "id": "notes",
            "title": "Approved Insight Notes",
            "description": "Context notes that help the dashboard narrative travel well with stakeholders.",
            "blocks": [
                {
                    "id": "approved-note-list",
                    "kind": "note_list",
                    "insights": [_insight_card(insight) for insight in approved_insights],
                }
            ],
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


def _render_dashboard_html(payload: dict[str, Any]) -> str:
    template = Template(
        """
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
      :root {
        --bg: #f6fafc;
        --panel: rgba(255,255,255,0.95);
        --panel-strong: #ffffff;
        --text: #16324f;
        --text-soft: #587189;
        --border: rgba(22,50,79,0.12);
      }
      body { margin:0; background: linear-gradient(180deg, #f7fafc 0%, #edf3f7 100%); font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color:var(--text); }
      .db { padding: 1rem 0; }
      .headline h2 { margin:0; font-size:28px; }
      .headline p { margin:0.4rem 0 0.9rem; color:var(--text-soft); font-size:14px; }
      .focus-tags { display:flex; flex-wrap:wrap; gap:8px; margin-bottom:1rem; }
      .focus-tag { font-size:11px; color:#1f5f8b; background:#e3f1fb; border-radius:999px; padding:0.3rem 0.55rem; font-weight:700; }
      .sec-label { font-size: 11px; font-weight: 700; letter-spacing: 0.07em; text-transform: uppercase; color: var(--text-soft); margin: 1.5rem 0 0.6rem; }
      .kpi-grid { display: grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap: 10px; margin-bottom: 0.5rem; }
      .kpi { background: var(--panel); border-radius: 18px; padding: 0.8rem 1rem; border:1px solid var(--border); box-shadow: 0 12px 28px rgba(22,50,79,0.04); }
      .kpi .lbl { font-size: 11px; color: var(--text-soft); margin-bottom: 3px; text-transform:uppercase; letter-spacing:0.06em; }
      .kpi .val { font-size: 22px; font-weight: 700; color: var(--text); }
      .kpi .sub { font-size: 11px; color: var(--text-soft); margin-top: 2px; }
      .chart-row { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; margin-bottom: 14px; }
      .chart-row-3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 14px; margin-bottom: 14px; }
      .card { background: var(--panel-strong); border: 1px solid var(--border); border-radius: 20px; padding: 1rem; box-shadow: 0 12px 28px rgba(22,50,79,0.04); }
      .card-title { font-size: 13px; font-weight: 700; color: var(--text); margin-bottom: 4px; }
      .card-sub { font-size: 11px; color: var(--text-soft); margin-bottom: 10px; }
      .leg { display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 8px; font-size: 11px; color: var(--text-soft); }
      .leg span { display: flex; align-items: center; gap: 4px; }
      .leg-sq { width: 9px; height: 9px; border-radius: 2px; }
      .insight-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 1rem; }
      .insight { border-radius: 16px; padding: 0.7rem 0.9rem; border: 1px solid var(--border); }
      .insight .i-label { font-size: 10px; font-weight: 700; letter-spacing: 0.06em; text-transform: uppercase; margin-bottom: 3px; }
      .insight .i-val { font-size: 18px; font-weight: 700; }
      .insight .i-desc { font-size: 11px; margin-top: 2px; line-height:1.45; }
      .warn { background: #FEF9E7; }
      .warn .i-label, .warn .i-val, .warn .i-desc { color: #854F0B; }
      .good { background: #EAF3DE; }
      .good .i-label, .good .i-val, .good .i-desc { color: #3B6D11; }
      .info { background: #E6F1FB; }
      .info .i-label, .info .i-val, .info .i-desc { color: #185FA5; }
      .alert { background: #FCEBEB; }
      .alert .i-label, .alert .i-val, .alert .i-desc { color: #A32D2D; }
      .note-grid { display:grid; grid-template-columns:1fr 1fr; gap:10px; }
      @media (max-width: 900px) { .kpi-grid, .chart-row, .chart-row-3, .insight-grid, .note-grid { grid-template-columns:1fr; } }
    </style>
  </head>
  <body>
      <div class="db">
        <div class="headline">
          <h2>{{ payload.headline.title }}</h2>
          <p>{{ payload.headline.subtitle }}</p>
        </div>
        {% if payload.focus_tags %}
        <div class="focus-tags">
          {% for tag in payload.focus_tags %}
          <span class="focus-tag">{{ tag }}</span>
          {% endfor %}
        </div>
        {% endif %}

        {% if 'overview' in payload.sections %}
        <div class="kpi-grid">
          {% for card in payload.metric_cards %}
          <div class="kpi"><div class="lbl">{{ card.metric_label }}</div><div class="val">{{ card.metric_value }}</div><div class="sub">{{ card.metric_sub }}</div></div>
          {% endfor %}
        </div>

        <div class="sec-label">Approved non-obvious insights</div>
        <div class="insight-grid">
          {% for insight in payload.approved_insights[:4] %}
          <div class="insight {{ ['warn','alert','good','info'][loop.index0 % 4] }}">
            <div class="i-label">{{ insight.title }}</div>
            <div class="i-val">{{ insight.metric_value }}</div>
            <div class="i-desc">{{ insight.summary }}</div>
          </div>
          {% endfor %}
        </div>
        {% endif %}

        {% if 'revenue' in payload.sections %}
        <div class="sec-label">Revenue patterns</div>
        <div class="card" style="margin-bottom:14px;">
          <div class="card-title">Monthly revenue — latest years</div>
          <div class="card-sub">The dashboard compares the most recent years available in the dataset.</div>
          <div style="position:relative;height:200px;"><canvas id="monthlyChart"></canvas></div>
        </div>
        <div class="chart-row">
          <div class="card">
            <div class="card-title">AOV by day of week</div>
            <div class="card-sub">{{ payload.summary.best_day }} is peak, {{ payload.summary.worst_day }} is the weakest day</div>
            <div style="position:relative;height:200px;"><canvas id="dowChart"></canvas></div>
          </div>
        {% endif %}

        {% if 'discounts' in payload.sections %}
          <div class="card">
            <div class="card-title">Discount band vs avg revenue</div>
            <div class="card-sub">Higher discounts should earn their place; this shows whether they actually do.</div>
            <div style="position:relative;height:200px;"><canvas id="discountChart"></canvas></div>
          </div>
        {% endif %}
        {% if 'revenue' in payload.sections or 'discounts' in payload.sections %}
        </div>
        {% endif %}

        {% if 'returns' in payload.sections or 'channels' in payload.sections %}
        <div class="chart-row">
          {% if 'returns' in payload.sections %}
          <div class="card">
            <div class="card-title">Return rate by category</div>
            <div class="card-sub">Some categories can quietly absorb most of the margin pain.</div>
            <div style="position:relative;height:230px;"><canvas id="returnChart"></canvas></div>
          </div>
          {% endif %}
          {% if 'channels' in payload.sections %}
          <div class="card">
            <div class="card-title">Avg order value by channel</div>
            <div class="card-sub">High-intent traffic often looks better here than it does in headline volume metrics.</div>
            <div style="position:relative;height:230px;"><canvas id="channelChart"></canvas></div>
          </div>
          {% endif %}
        </div>
        {% endif %}

        {% if 'returns' in payload.sections or 'channels' in payload.sections %}
        <div class="chart-row-3">
          {% if 'channels' in payload.sections %}
          <div class="card">
            <div class="card-title">Orders by category</div>
            <div class="card-sub">This shows what mix is driving the store, not just which category is loudest.</div>
            <div style="position:relative;height:160px;"><canvas id="catChart"></canvas></div>
          </div>
          {% endif %}
          {% if 'returns' in payload.sections %}
          <div class="card">
            <div class="card-title">Return rate by payment</div>
            <div class="card-sub">Payment behavior can reveal a customer-quality signal.</div>
            <div style="position:relative;height:180px;"><canvas id="paymentChart"></canvas></div>
          </div>
          {% endif %}
          {% if 'channels' in payload.sections %}
          <div class="card">
            <div class="card-title">Device split</div>
            <div class="card-sub">Useful for judging where order volume is really coming from.</div>
            <div style="position:relative;height:160px;"><canvas id="deviceChart"></canvas></div>
          </div>
          {% endif %}
        </div>
        {% endif %}

        {% if payload.show_notes and 'notes' in payload.sections %}
        <div class="sec-label">Approved insight notes</div>
        <div class="note-grid">
          {% for insight in payload.approved_insights %}
          <div class="card">
            <div class="card-title">{{ insight.title }}</div>
            <div class="card-sub">{{ insight.summary }}</div>
            <div style="font-size:12px; color: var(--text-soft);">{{ insight.detail }}</div>
          </div>
          {% endfor %}
        </div>
        {% endif %}
      </div>

      <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
      <script>
      const payload = {{ payload_json | safe }};
      const grid = 'rgba(0,0,0,0.06)';
      const tick = '#666';
      const baseOpts = { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } };

      if (document.getElementById('monthlyChart')) {
        const years = Object.keys(payload.signals.monthly_revenue.series);
        new Chart(document.getElementById('monthlyChart'), {
          type: 'bar',
          data: {
            labels: payload.signals.monthly_revenue.labels,
            datasets: years.map((year, idx) => ({
              label: year,
              data: payload.signals.monthly_revenue.series[year],
              backgroundColor: idx === 0 ? '#B5D4F4' : '#378ADD',
              borderRadius: 3
            }))
          },
          options: { ...baseOpts,
            scales: {
              x: { grid: { display: false }, ticks: { color: tick, font: { size: 10 } } },
              y: { grid: { color: grid }, ticks: { color: tick, font: { size: 10 }, callback: v => '$' + (v/1000).toFixed(0) + 'k' } }
            }
          }
        });
      }

      if (document.getElementById('returnChart')) {
        new Chart(document.getElementById('returnChart'), {
          type: 'bar',
          data: {
            labels: payload.signals.category_returns.labels,
            datasets: [{ data: payload.signals.category_returns.values, backgroundColor: ['#E24B4A','#F09595','#EF9F27','#97C459','#5DCAA5','#85B7EB','#AFA9EC'], borderRadius: 4 }]
          },
          options: { ...baseOpts, indexAxis: 'y',
            scales: {
              x: { grid: { color: grid }, ticks: { color: tick, font: { size: 10 }, callback: v => v + '%' } },
              y: { grid: { display: false }, ticks: { color: tick, font: { size: 10 } } }
            }
          }
        });
      }

      if (document.getElementById('channelChart')) {
        new Chart(document.getElementById('channelChart'), {
          type: 'bar',
          data: {
            labels: payload.signals.channel_aov.labels,
            datasets: [{ data: payload.signals.channel_aov.values, backgroundColor: ['#1D9E75','#5DCAA5','#9FE1CB','#B5D4F4','#85B7EB','#D3D1C7'], borderRadius: 4 }]
          },
          options: { ...baseOpts, indexAxis: 'y',
            scales: {
              x: { grid: { color: grid }, ticks: { color: tick, font: { size: 10 }, callback: v => '$' + v } },
              y: { grid: { display: false }, ticks: { color: tick, font: { size: 10 } } }
            }
          }
        });
      }

      if (document.getElementById('dowChart')) {
        new Chart(document.getElementById('dowChart'), {
          type: 'bar',
          data: {
            labels: payload.signals.weekday_aov.labels.map(label => label.slice(0,3)),
            datasets: [{ data: payload.signals.weekday_aov.values, backgroundColor: payload.signals.weekday_aov.labels.map(label => label === payload.summary.best_day ? '#1D9E75' : (label === payload.summary.worst_day ? '#D3D1C7' : '#B5D4F4')), borderRadius: 4 }]
          },
          options: { ...baseOpts,
            scales: {
              x: { grid: { display: false }, ticks: { color: tick, font: { size: 11 } } },
              y: { grid: { color: grid }, ticks: { color: tick, font: { size: 10 }, callback: v => '$' + v } }
            }
          }
        });
      }

      if (document.getElementById('discountChart')) {
        new Chart(document.getElementById('discountChart'), {
          type: 'bar',
          data: {
            labels: payload.signals.discount_revenue.labels,
            datasets: [{ data: payload.signals.discount_revenue.values, backgroundColor: ['#1D9E75','#EF9F27','#F09595','#E24B4A'], borderRadius: 4 }]
          },
          options: { ...baseOpts,
            scales: {
              x: { grid: { display: false }, ticks: { color: tick, font: { size: 10 } } },
              y: { grid: { color: grid }, ticks: { color: tick, font: { size: 10 }, callback: v => '$' + v } }
            }
          }
        });
      }

      if (document.getElementById('catChart')) {
        new Chart(document.getElementById('catChart'), {
          type: 'doughnut',
          data: {
            labels: payload.signals.category_mix.labels,
            datasets: [{ data: payload.signals.category_mix.values, backgroundColor: ['#378ADD','#1D9E75','#BA7517','#D4537E','#7F77DD','#888780','#D85A30'], borderWidth: 0 }]
          },
          options: { ...baseOpts, cutout: '62%' }
        });
      }

      if (document.getElementById('paymentChart')) {
        new Chart(document.getElementById('paymentChart'), {
          type: 'bar',
          data: {
            labels: payload.signals.payment_returns.labels,
            datasets: [{ data: payload.signals.payment_returns.values, backgroundColor: ['#E24B4A','#F09595','#EF9F27','#85B7EB','#1D9E75'], borderRadius: 4 }]
          },
          options: { ...baseOpts, indexAxis: 'y',
            scales: {
              x: { grid: { color: grid }, ticks: { color: tick, font: { size: 10 }, callback: v => v + '%' } },
              y: { grid: { display: false }, ticks: { color: tick, font: { size: 10 } } }
            }
          }
        });
      }

      if (document.getElementById('deviceChart')) {
        new Chart(document.getElementById('deviceChart'), {
          type: 'doughnut',
          data: {
            labels: payload.signals.device_mix.labels,
            datasets: [{ data: payload.signals.device_mix.values, backgroundColor: ['#378ADD','#1D9E75','#BA7517','#D85A30'], borderWidth: 0 }]
          },
          options: { ...baseOpts, cutout: '62%' }
        });
      }
      </script>
  </body>
</html>
        """
    )
    return template.render(payload=payload, payload_json=json.dumps(payload, default=_json_default))


def _json_default(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return str(value)
