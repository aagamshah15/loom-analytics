from __future__ import annotations

import json
import re
from typing import Any, Optional

import pandas as pd
from jinja2 import Template

from pipeline.common.contracts import PipelineContext


REQUIRED_COLUMN_ALIASES = {
    "device": ["device", "device_type", "platform"],
    "channel": ["channel", "source", "traffic_source", "marketing_channel"],
    "page": ["page", "page_name", "landing_page", "page_type"],
    "sessions": ["sessions", "session_count", "visits"],
    "conversions": ["conversions", "conversion_count", "orders", "goal_completions"],
    "bounce_rate": ["bounce_rate", "bounce", "bounce_percent"],
    "load_time": ["load_time", "page_load_time", "avg_load_time", "load_seconds"],
}

OPTIONAL_COLUMN_ALIASES = {
    "campaign": ["campaign", "campaign_name", "utm_campaign"],
    "scroll_depth": ["scroll_depth", "avg_scroll_depth", "scroll_depth_percent"],
    "time_on_page": ["avg_time_on_page", "time_on_page", "avg_session_duration", "duration_seconds"],
    "visitor_type": ["visitor_type", "visitor_segment", "new_vs_returning", "user_type"],
    "exit_count": ["exit_count", "exits", "exit_sessions"],
}

FOCUS_KEYWORDS = {
    "mobile": ["mobile", "device", "speed", "load", "performance"],
    "channels": ["channel", "traffic", "social", "source"],
    "campaigns": ["campaign", "email", "onboarding", "paid"],
    "content": ["content", "blog", "page", "homepage", "exit", "cta"],
    "retention": ["returning", "new visitor", "remarketing", "repeat", "visitor"],
}

PROMPT_STOP_WORDS = {
    "about",
    "across",
    "after",
    "analytics",
    "before",
    "build",
    "dashboard",
    "data",
    "focus",
    "from",
    "into",
    "look",
    "more",
    "need",
    "please",
    "show",
    "site",
    "story",
    "that",
    "them",
    "these",
    "this",
    "those",
    "want",
    "web",
    "with",
}

SECTION_CONFIG = {
    "overview": "KPI cards and key insights",
    "devices": "Charts: device performance and speed",
    "channels": "Charts: channel quality",
    "campaigns": "Charts: campaign efficiency",
    "content": "Charts: content and exit paths",
    "retention": "Charts: new vs returning visitors",
    "notes": "Insight notes",
}


def analyze_web_analytics_context(context: PipelineContext) -> Optional[dict[str, Any]]:
    df = context.clean_df if context.clean_df is not None else context.raw_df
    if df is None or df.empty:
        return None

    detected = _detect_columns(df)
    if detected is None:
        return None

    working = pd.DataFrame(
        {
            "device": df[detected["required"]["device"]].astype(str).str.strip(),
            "channel": df[detected["required"]["channel"]].astype(str).str.strip(),
            "page": df[detected["required"]["page"]].astype(str).str.strip(),
            "sessions": pd.to_numeric(df[detected["required"]["sessions"]], errors="coerce"),
            "conversions": pd.to_numeric(df[detected["required"]["conversions"]], errors="coerce"),
            "bounce_rate": pd.to_numeric(df[detected["required"]["bounce_rate"]], errors="coerce"),
            "load_time": pd.to_numeric(df[detected["required"]["load_time"]], errors="coerce"),
        }
    ).dropna(subset=["sessions", "conversions", "bounce_rate", "load_time"])

    if len(working) < 20:
        return None

    optional = detected["optional"]
    working["campaign"] = (
        df.loc[working.index, optional["campaign"]].astype(str).str.strip()
        if optional.get("campaign")
        else "Unknown"
    )
    working["scroll_depth"] = (
        pd.to_numeric(df.loc[working.index, optional["scroll_depth"]], errors="coerce")
        if optional.get("scroll_depth")
        else pd.NA
    )
    working["time_on_page"] = (
        pd.to_numeric(df.loc[working.index, optional["time_on_page"]], errors="coerce")
        if optional.get("time_on_page")
        else pd.NA
    )
    working["visitor_type"] = (
        df.loc[working.index, optional["visitor_type"]].astype(str).str.strip()
        if optional.get("visitor_type")
        else "Unknown"
    )
    if optional.get("exit_count"):
        working["exit_count"] = pd.to_numeric(df.loc[working.index, optional["exit_count"]], errors="coerce")
    else:
        working["exit_count"] = working["sessions"] * (working["bounce_rate"] / 100.0)

    working["conversion_rate"] = (
        working["conversions"] / working["sessions"].replace(0, pd.NA) * 100
    )
    working = working.dropna(subset=["conversion_rate"]).copy()
    if len(working) < 20:
        return None

    device_summary = _weighted_summary(working, "device", include_scroll=True)
    channel_summary = _weighted_summary(working, "channel", include_scroll=True)
    campaign_summary = _weighted_summary(working, "campaign", include_scroll=False)
    page_summary = _weighted_summary(working, "page", include_scroll=True, include_time=True, include_exits=True)
    visitor_summary = _weighted_summary(working, "visitor_type", include_scroll=False)

    mobile = _matching_metrics(device_summary, ["mobile"])
    desktop = _matching_metrics(device_summary, ["desktop"])
    social = _matching_metrics(channel_summary, ["social"])
    onboarding_email = _matching_metrics(campaign_summary, ["onboarding email", "onboarding_email", "email onboarding"])
    blog = _matching_metrics(page_summary, ["blog", "blog posts", "blog_post"])
    home = _matching_metrics(page_summary, ["home", "homepage", "home page"])
    returning = _matching_metrics(visitor_summary, ["returning", "return visitor"])
    new_visitor = _matching_metrics(visitor_summary, ["new", "new visitor"])

    return {
        "kind": "web_app_analytics",
        "dataset": {
            "row_count": int(len(working)),
            "column_count": int(len(df.columns)),
            "input_columns": list(df.columns),
            "session_count": int(round(float(working["sessions"].sum()))),
        },
        "column_map": detected,
        "summary": {
            "session_count": int(round(float(working["sessions"].sum()))),
            "total_conversions": int(round(float(working["conversions"].sum()))),
            "overall_conversion_rate": float(working["conversions"].sum() / working["sessions"].sum() * 100)
            if float(working["sessions"].sum())
            else 0.0,
            "mobile_sessions_share": None if mobile is None else float(mobile["session_share"]),
            "mobile_conversion_rate": None if mobile is None else float(mobile["conversion_rate"]),
            "desktop_conversion_rate": None if desktop is None else float(desktop["conversion_rate"]),
            "mobile_load_time": None if mobile is None else float(mobile["load_time"]),
            "desktop_load_time": None if desktop is None else float(desktop["load_time"]),
            "social_bounce_rate": None if social is None else float(social["bounce_rate"]),
            "social_scroll_depth": None if social is None else float(social.get("scroll_depth", 0.0)),
            "social_conversion_rate": None if social is None else float(social["conversion_rate"]),
            "best_campaign": None if campaign_summary.empty else str(campaign_summary["conversion_rate"].idxmax()),
            "best_campaign_conversion_rate": None if onboarding_email is None else float(onboarding_email["conversion_rate"]),
            "best_campaign_bounce_rate": None if onboarding_email is None else float(onboarding_email["bounce_rate"]),
            "blog_time_on_page": None if blog is None else float(blog.get("time_on_page", 0.0)),
            "blog_conversion_rate": None if blog is None else float(blog["conversion_rate"]),
            "home_exit_count": None if home is None else float(home.get("exit_count", 0.0)),
            "top_exit_page": None if page_summary.empty else str(page_summary["exit_count"].idxmax()),
            "returning_conversion_rate": None if returning is None else float(returning["conversion_rate"]),
            "new_conversion_rate": None if new_visitor is None else float(new_visitor["conversion_rate"]),
        },
        "signals": {
            "device_conversion_rate": _signal_from_frame(device_summary, "conversion_rate"),
            "device_load_time": _signal_from_frame(device_summary, "load_time"),
            "channel_bounce_rate": _signal_from_frame(channel_summary, "bounce_rate"),
            "channel_conversion_rate": _signal_from_frame(channel_summary, "conversion_rate"),
            "campaign_conversion_rate": _signal_from_frame(campaign_summary.head(8), "conversion_rate"),
            "page_conversion_rate": _signal_from_frame(page_summary.head(8), "conversion_rate"),
            "page_exit_count": _signal_from_frame(page_summary.head(8), "exit_count"),
            "visitor_conversion_rate": _signal_from_frame(visitor_summary, "conversion_rate"),
        },
    }


def build_web_analytics_insight_candidates(analysis: dict[str, Any], user_prompt: str = "") -> dict[str, Any]:
    summary = analysis["summary"]
    focus_tags = extract_focus_tags(user_prompt)
    insights = [
        {
            "id": "mobile_conversion_leak",
            "title": "Mobile is the biggest conversion leak",
            "category": "mobile",
            "severity": "high",
            "summary": (
                f"Mobile drives {summary['mobile_sessions_share']:.1f}% of sessions but converts at {summary['mobile_conversion_rate']:.2f}% "
                f"versus {summary['desktop_conversion_rate']:.2f}% on desktop."
            ),
            "detail": (
                f"The speed gap is likely the culprit: mobile loads at {summary['mobile_load_time']:.2f}s versus "
                f"{summary['desktop_load_time']:.2f}s on desktop."
            ),
            "metric_label": "Mobile CVR gap",
            "metric_value": f"{((summary['desktop_conversion_rate'] or 0) - (summary['mobile_conversion_rate'] or 0)):.2f} pts",
            "metric_sub": "desktop minus mobile conversion rate",
            "tags": ["mobile"],
            "section": "devices",
            "priority": 100,
            "condition": summary["mobile_sessions_share"] is not None and summary["desktop_conversion_rate"] is not None,
        },
        {
            "id": "social_traffic_wasted",
            "title": "Social traffic is largely wasted",
            "category": "channels",
            "severity": "high",
            "summary": (
                f"Social traffic shows {summary['social_bounce_rate']:.1f}% bounce, {summary['social_scroll_depth']:.1f}% scroll depth, "
                f"and only {summary['social_conversion_rate']:.2f}% conversion."
            ),
            "detail": "That pattern looks like curiosity traffic, not intent. The landing experience appears misaligned with the acquisition promise.",
            "metric_label": "Social CVR",
            "metric_value": f"{summary['social_conversion_rate']:.2f}%",
            "metric_sub": "social conversion rate",
            "tags": ["channels"],
            "section": "channels",
            "priority": 97,
            "condition": summary["social_conversion_rate"] is not None,
        },
        {
            "id": "onboarding_email_winner",
            "title": "Onboarding email is the hidden champion",
            "category": "campaigns",
            "severity": "high",
            "summary": (
                f"Onboarding email converts at {summary['best_campaign_conversion_rate']:.2f}% with only "
                f"{summary['best_campaign_bounce_rate']:.1f}% bounce."
            ),
            "detail": "That is the strongest-performing campaign pattern in the dataset and likely one of the most underinvested levers in the mix.",
            "metric_label": "Onboarding email CVR",
            "metric_value": f"{summary['best_campaign_conversion_rate']:.2f}%",
            "metric_sub": "conversion rate",
            "tags": ["campaigns"],
            "section": "campaigns",
            "priority": 96,
            "condition": summary["best_campaign_conversion_rate"] is not None,
        },
        {
            "id": "blog_black_hole",
            "title": "Blog is a conversion black hole",
            "category": "content",
            "severity": "medium",
            "summary": (
                f"Blog content holds attention for {summary['blog_time_on_page']:.0f} seconds but converts at only "
                f"{summary['blog_conversion_rate']:.2f}%."
            ),
            "detail": "That is reading intent without a strong commercial bridge. A better CTA path is likely the fastest content win available.",
            "metric_label": "Blog CVR",
            "metric_value": f"{summary['blog_conversion_rate']:.2f}%",
            "metric_sub": "blog conversion rate",
            "tags": ["content"],
            "section": "content",
            "priority": 92,
            "condition": summary["blog_conversion_rate"] is not None and summary["blog_time_on_page"] is not None,
        },
        {
            "id": "home_is_top_exit",
            "title": "The homepage is acting like the main exit page",
            "category": "content",
            "severity": "high",
            "summary": f"{summary['top_exit_page']} is the top exit page, with home driving about {summary['home_exit_count']:.0f} exits.",
            "detail": "The front door is also the back door. The homepage is not reliably pulling people deeper into the funnel.",
            "metric_label": "Home exits",
            "metric_value": f"{summary['home_exit_count']:.0f}",
            "metric_sub": "estimated exit sessions",
            "tags": ["content"],
            "section": "content",
            "priority": 95,
            "condition": summary["home_exit_count"] is not None and summary["top_exit_page"] is not None,
        },
        {
            "id": "returning_visitors_underperform",
            "title": "Returning visitors barely convert better than new ones",
            "category": "retention",
            "severity": "medium",
            "summary": (
                f"Returning visitors convert at {summary['returning_conversion_rate']:.2f}% versus "
                f"{summary['new_conversion_rate']:.2f}% for new visitors."
            ),
            "detail": "That suggests remarketing and repeat traffic are not finding meaningfully better conversion paths than first-time sessions.",
            "metric_label": "Return visitor lift",
            "metric_value": f"{((summary['returning_conversion_rate'] or 0) - (summary['new_conversion_rate'] or 0)):.2f} pts",
            "metric_sub": "returning minus new conversion rate",
            "tags": ["retention"],
            "section": "retention",
            "priority": 88,
            "condition": summary["returning_conversion_rate"] is not None and summary["new_conversion_rate"] is not None,
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
        analysis = analyze_web_analytics_context(context)
    if analysis is None:
        return None

    insight_bundle = build_web_analytics_insight_candidates(analysis, user_prompt)
    insights = insight_bundle["insights"]
    approved_set = set(approved_insight_ids or [item["id"] for item in insights if item.get("recommended", True)])
    approved = [item for item in insights if item["id"] in approved_set] or insights[:4]

    settings = settings or {}
    included_sections = settings.get("included_sections") or _default_sections(approved)
    title = settings.get("title") or "Web Analytics Hidden Insights"
    subtitle = settings.get("subtitle") or "Approved web analytics narrative"
    metric_count = int(settings.get("metric_count", 4))
    show_notes = bool(settings.get("show_notes", True))

    payload = {
        "kind": "web_app_analytics",
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
        "kind": "web_app_analytics",
        "title": title,
        "html": html,
        "height": 1120 + (150 * len(included_sections)),
        "payload": payload,
        "blueprint": payload,
        "download_name": "web_analytics_insights_dashboard.html",
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


def _weighted_summary(
    df: pd.DataFrame,
    column: str,
    *,
    include_scroll: bool,
    include_time: bool = False,
    include_exits: bool = False,
) -> pd.DataFrame:
    groups = []
    for label, subset in df.groupby(column):
        sessions = float(subset["sessions"].sum())
        conversions = float(subset["conversions"].sum())
        if sessions <= 0:
            continue
        row = {
            "label": label,
            "sessions": sessions,
            "conversion_rate": conversions / sessions * 100,
            "bounce_rate": float((subset["bounce_rate"] * subset["sessions"]).sum() / sessions),
            "load_time": float((subset["load_time"] * subset["sessions"]).sum() / sessions),
        }
        if include_scroll:
            scroll = pd.to_numeric(subset["scroll_depth"], errors="coerce")
            valid = scroll.notna()
            if valid.any():
                row["scroll_depth"] = float((scroll[valid] * subset.loc[valid, "sessions"]).sum() / subset.loc[valid, "sessions"].sum())
        if include_time:
            timing = pd.to_numeric(subset["time_on_page"], errors="coerce")
            valid = timing.notna()
            if valid.any():
                row["time_on_page"] = float((timing[valid] * subset.loc[valid, "sessions"]).sum() / subset.loc[valid, "sessions"].sum())
        if include_exits:
            exits = pd.to_numeric(subset["exit_count"], errors="coerce").fillna(0.0)
            row["exit_count"] = float(exits.sum())
        groups.append(row)
    frame = pd.DataFrame(groups)
    if frame.empty:
        return frame
    frame["session_share"] = frame["sessions"] / frame["sessions"].sum() * 100
    return frame.set_index("label").sort_values("sessions", ascending=False)


def _signal_from_frame(frame: pd.DataFrame, column: str) -> dict[str, list[Any]]:
    if frame.empty or column not in frame.columns:
        return {"labels": [], "values": []}
    return {
        "labels": frame.index.tolist(),
        "values": [round(float(value), 2) for value in frame[column].fillna(0.0).tolist()],
    }


def _matching_metrics(frame: pd.DataFrame, labels: list[str]) -> Optional[pd.Series]:
    if frame.empty:
        return None
    normalized = {str(index).strip().lower(): index for index in frame.index}
    for label in labels:
        if label.lower() in normalized:
            return frame.loc[normalized[label.lower()]]
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
            "title": "Web Analytics Narrative",
            "description": "The fastest read on funnel leakage, acquisition quality, and high-leverage site fixes.",
            "blocks": [
                {"id": "overview-metrics", "kind": "metric_grid", "cards": [_metric_card_from_insight(card) for card in metric_cards]},
                {"id": "overview-insights", "kind": "insight_grid", "insights": [_insight_card(insight) for insight in approved_insights[:4]]},
            ],
        },
        "devices": {
            "id": "devices",
            "title": "Device Performance",
            "description": "Speed and conversion should be read together because mobile friction compounds quickly.",
            "blocks": [
                {"id": "device-cvr", "kind": "chart", "chart": {"id": "device-cvr", "title": "Conversion rate by device", "subtitle": "Device mix should not hide performance inequality.", "type": "bar", "labels": signals["device_conversion_rate"]["labels"], "series": [{"name": "Conversion rate", "values": signals["device_conversion_rate"]["values"], "color": "#c2410c"}], "format": "percent"}},
                {"id": "device-load", "kind": "chart", "chart": {"id": "device-load", "title": "Load time by device", "subtitle": "Speed gaps are often the upstream cause of conversion gaps.", "type": "bar", "labels": signals["device_load_time"]["labels"], "series": [{"name": "Load time", "values": signals["device_load_time"]["values"], "color": "#9a3412"}], "format": "number"}},
            ],
        },
        "channels": {
            "id": "channels",
            "title": "Channel Quality",
            "description": "Traffic volume only matters when it lands with intent and depth.",
            "blocks": [
                {"id": "channel-bounce", "kind": "chart", "chart": {"id": "channel-bounce", "title": "Bounce rate by channel", "subtitle": "High bounce channels are usually telling a message-match story.", "type": "bar", "labels": signals["channel_bounce_rate"]["labels"], "series": [{"name": "Bounce rate", "values": signals["channel_bounce_rate"]["values"], "color": "#b45309"}], "format": "percent"}},
                {"id": "channel-cvr", "kind": "chart", "chart": {"id": "channel-cvr", "title": "Conversion rate by channel", "subtitle": "Traffic efficiency is rarely evenly distributed across sources.", "type": "bar", "labels": signals["channel_conversion_rate"]["labels"], "series": [{"name": "Conversion rate", "values": signals["channel_conversion_rate"]["values"], "color": "#166534"}], "format": "percent"}},
            ],
        },
        "campaigns": {
            "id": "campaigns",
            "title": "Campaign Efficiency",
            "description": "Campaigns should be compared on commercial output, not just clicks or opens.",
            "blocks": [
                {"id": "campaign-cvr", "kind": "chart", "chart": {"id": "campaign-cvr", "title": "Conversion rate by campaign", "subtitle": "The highest-quality campaigns often deserve far more budget than they receive.", "type": "bar", "labels": signals["campaign_conversion_rate"]["labels"], "series": [{"name": "Conversion rate", "values": signals["campaign_conversion_rate"]["values"], "color": "#0f766e"}], "format": "percent"}},
            ],
        },
        "content": {
            "id": "content",
            "title": "Content and Exit Paths",
            "description": "Attention without a strong next step becomes a sinkhole instead of a funnel step.",
            "blocks": [
                {"id": "page-cvr", "kind": "chart", "chart": {"id": "page-cvr", "title": "Conversion rate by page", "subtitle": "Long attention spans only matter when the page gives visitors a meaningful next move.", "type": "bar", "labels": signals["page_conversion_rate"]["labels"], "series": [{"name": "Conversion rate", "values": signals["page_conversion_rate"]["values"], "color": "#7c2d12"}], "format": "percent"}},
                {"id": "page-exits", "kind": "chart", "chart": {"id": "page-exits", "title": "Exit count by page", "subtitle": "Exit-heavy pages deserve immediate funnel-design attention.", "type": "bar", "labels": signals["page_exit_count"]["labels"], "series": [{"name": "Exit sessions", "values": signals["page_exit_count"]["values"], "color": "#292524"}], "format": "number"}},
            ],
        },
        "retention": {
            "id": "retention",
            "title": "New vs Returning Visitors",
            "description": "Repeat traffic only matters if the experience meaningfully improves conversion intent.",
            "blocks": [
                {"id": "visitor-cvr", "kind": "chart", "chart": {"id": "visitor-cvr", "title": "Conversion rate by visitor type", "subtitle": "Remarketing value should show up in visitor behavior, not just media assumptions.", "type": "bar", "labels": signals["visitor_conversion_rate"]["labels"], "series": [{"name": "Conversion rate", "values": signals["visitor_conversion_rate"]["values"], "color": "#57534e"}], "format": "percent"}},
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
        <div class="eyebrow">Loom Web Analytics Narrative</div>
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
