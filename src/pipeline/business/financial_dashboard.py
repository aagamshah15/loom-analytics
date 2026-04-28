from __future__ import annotations

import json
import math
import re
from typing import Any, Optional

import pandas as pd
from jinja2 import Template

from pipeline.common.contracts import PipelineContext


REQUIRED_COLUMN_ALIASES = {
    "date": ["date", "timestamp", "datetime"],
    "open": ["open"],
    "high": ["high"],
    "low": ["low"],
    "close": ["close", "close/last", "close last", "last", "adj close", "adjusted close"],
    "volume": ["volume"],
}

OPTIONAL_COLUMN_ALIASES = {
    "dividends": ["dividends", "dividend"],
    "stock_splits": ["stock splits", "split", "stock_splits"],
}

FOCUS_KEYWORDS = {
    "growth": ["growth", "trend", "appreciation", "return", "long term", "long-term", "recovery"],
    "quality": ["quality", "cleaning", "adjusted", "quirk", "backfilled", "inconsistency", "data issue"],
    "volatility": ["volatility", "drawdown", "crash", "correction", "risk", "range"],
    "seasonality": ["weekday", "month", "seasonality", "calendar", "pattern"],
    "gaps": ["gap", "overnight", "open"],
    "volume": ["volume", "liquidity", "trading activity"],
    "dividends": ["dividend", "income", "payout"],
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
    "overview": "Narrative headline cards",
    "seasonality": "Weekday and month pattern section",
    "volatility": "Drawdown and volatility regime section",
    "gaps": "Overnight gap behavior section",
    "volume": "Extreme volume section",
    "data_notes": "Approved insight notes section",
}


def analyze_financial_context(context: PipelineContext) -> Optional[dict[str, Any]]:
    df = context.clean_df if context.clean_df is not None else context.raw_df
    if df is None or df.empty:
        return None

    detected = _detect_financial_columns(df)
    if detected is None:
        return None

    working = pd.DataFrame(
        {
            "date": pd.to_datetime(df[detected["required"]["date"]], errors="coerce"),
            "open": _coerce_numeric_series(df[detected["required"]["open"]]),
            "high": _coerce_numeric_series(df[detected["required"]["high"]]),
            "low": _coerce_numeric_series(df[detected["required"]["low"]]),
            "close": _coerce_numeric_series(df[detected["required"]["close"]]),
            "volume": _coerce_numeric_series(df[detected["required"]["volume"]]),
        }
    )

    if detected["optional"].get("dividends"):
        working["dividends"] = _coerce_numeric_series(df[detected["optional"]["dividends"]]).fillna(0.0)
    else:
        working["dividends"] = 0.0

    if detected["optional"].get("stock_splits"):
        working["stock_splits"] = _coerce_numeric_series(df[detected["optional"]["stock_splits"]]).fillna(0.0)
    else:
        working["stock_splits"] = 0.0

    working = working.dropna(subset=["date", "open", "high", "low", "close"]).sort_values("date").reset_index(drop=True)
    if len(working) < 10:
        return None

    working["year"] = working["date"].dt.year
    working["month_num"] = working["date"].dt.month
    working["month"] = working["date"].dt.strftime("%b")
    working["weekday"] = pd.Categorical(
        working["date"].dt.day_name(),
        categories=["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
        ordered=True,
    )
    working["decade"] = ((working["year"] // 10) * 10).astype(int).astype(str) + "s"
    working["intraday_return"] = ((working["close"] - working["open"]) / working["open"]) * 100
    working["daily_range_pct"] = ((working["high"] - working["low"]) / working["open"]) * 100
    working["daily_return"] = working["close"].pct_change() * 100
    working["prev_close"] = working["close"].shift(1)
    working["gap_pct"] = ((working["open"] - working["prev_close"]) / working["prev_close"]) * 100
    working["drawdown_pct"] = ((working["close"] / working["close"].cummax()) - 1) * 100

    annual_close = working.groupby("year")["close"].last()
    annual_returns = (annual_close.pct_change() * 100).dropna()
    weekday_series = (
        working.groupby("weekday", observed=False)["intraday_return"]
        .mean()
        .reindex(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"])
        .fillna(0.0)
    )
    month_series = (
        working.groupby(["month_num", "month"])["intraday_return"]
        .mean()
        .reset_index()
        .sort_values("month_num")
    )
    decade_series = working.groupby("decade")["daily_range_pct"].mean().sort_index()
    top_volume = working.nlargest(10, "volume").copy()
    top_volume["label"] = top_volume["date"].dt.strftime("%b %Y")

    start_close = float(working["close"].iloc[0])
    latest_close = float(working["close"].iloc[-1])
    total_return_pct = ((latest_close / start_close) - 1) * 100 if start_close else 0.0

    recent_window = working[working["date"] >= (working["date"].max() - pd.Timedelta(days=365))]
    if recent_window.empty:
        recent_window = working.tail(min(252, len(working)))
    recent_peak = float(recent_window["close"].max())
    recent_correction_pct = ((latest_close / recent_peak) - 1) * 100 if recent_peak else 0.0

    early_flat_prefix = _leading_flat_rows(working)
    first_nonzero_volume = _first_nonzero_volume_date(working)
    dividend_events = int((working["dividends"] > 0).sum())
    split_events = int((working["stock_splits"] > 0).sum())

    gap_up = int((working["gap_pct"] > 0.5).sum())
    gap_down = int((working["gap_pct"] < -0.5).sum())
    flat_gap = int((working["gap_pct"].between(-0.5, 0.5, inclusive="both")).sum())

    top_volume_year_span = _cluster_span_years(top_volume["date"])
    dominant_volume_period = _dominant_volume_period(top_volume["date"])

    dataset_start = working["date"].min()
    dataset_end = working["date"].max()

    return {
        "kind": "financial_timeseries",
        "column_map": detected,
        "dataset": {
            "row_count": int(len(working)),
            "column_count": int(len(df.columns)),
            "start_date": dataset_start,
            "end_date": dataset_end,
            "start_year": int(dataset_start.year),
            "end_year": int(dataset_end.year),
            "years_covered": int(dataset_end.year - dataset_start.year + 1),
            "input_columns": list(df.columns),
        },
        "summary": {
            "start_close": start_close,
            "latest_close": latest_close,
            "latest_close_display": _currency(latest_close),
            "total_return_pct": total_return_pct,
            "recent_correction_pct": recent_correction_pct,
            "recent_peak": recent_peak,
            "worst_year": int(annual_returns.idxmin()) if not annual_returns.empty else int(dataset_start.year),
            "worst_year_return_pct": float(annual_returns.min()) if not annual_returns.empty else 0.0,
            "best_year": int(annual_returns.idxmax()) if not annual_returns.empty else int(dataset_end.year),
            "best_year_return_pct": float(annual_returns.max()) if not annual_returns.empty else 0.0,
            "deepest_drawdown_pct": float(working["drawdown_pct"].min()),
            "deepest_drawdown_date": working.loc[working["drawdown_pct"].idxmin(), "date"],
            "winning_streak": _streaks(working["daily_return"])[0],
            "losing_streak": _streaks(working["daily_return"])[1],
            "first_nonzero_volume": first_nonzero_volume,
            "early_flat_prefix": early_flat_prefix,
            "dividend_events": dividend_events,
            "split_events": split_events,
            "gap_up": gap_up,
            "gap_down": gap_down,
            "gap_flat": flat_gap,
            "top_volume_period": dominant_volume_period,
            "top_volume_span_years": top_volume_year_span,
        },
        "signals": {
            "weekday": {
                "labels": weekday_series.index.tolist(),
                "values": [round(float(value), 3) for value in weekday_series.tolist()],
                "best_label": weekday_series.idxmax() if not weekday_series.empty else "Wednesday",
                "worst_label": weekday_series.idxmin() if not weekday_series.empty else "Friday",
                "spread": round(float(weekday_series.max() - weekday_series.min()), 3) if not weekday_series.empty else 0.0,
            },
            "month": {
                "labels": month_series["month"].tolist(),
                "values": [round(float(value), 3) for value in month_series["intraday_return"].tolist()],
                "best_label": month_series.loc[month_series["intraday_return"].idxmax(), "month"] if not month_series.empty else "Apr",
                "worst_labels": month_series.nsmallest(3, "intraday_return")["month"].tolist() if not month_series.empty else [],
                "spread": round(float(month_series["intraday_return"].max() - month_series["intraday_return"].min()), 3) if not month_series.empty else 0.0,
            },
            "decade": {
                "labels": decade_series.index.tolist(),
                "values": [round(float(value), 2) for value in decade_series.tolist()],
                "most_volatile": decade_series.idxmax() if not decade_series.empty else "2000s",
                "calmest": decade_series.idxmin() if not decade_series.empty else "2010s",
                "spread": round(float(decade_series.max() - decade_series.min()), 2) if not decade_series.empty else 0.0,
            },
            "gap": {
                "labels": ["Gap up", "Gap down", "Flat"],
                "values": [gap_up, gap_down, flat_gap],
            },
            "volume": {
                "labels": top_volume["label"].tolist(),
                "values": [round(float(value) / 1_000_000, 1) for value in top_volume["volume"].fillna(0).tolist()],
            },
        },
        "optional_features": {
            "has_dividends": bool(detected["optional"].get("dividends")),
            "has_stock_splits": bool(detected["optional"].get("stock_splits")),
        },
    }


def build_financial_insight_candidates(
    analysis: dict[str, Any],
    user_prompt: str = "",
) -> dict[str, Any]:
    focus_tags = extract_focus_tags(user_prompt)
    summary = analysis["summary"]
    dataset = analysis["dataset"]
    signals = analysis["signals"]
    optional_features = analysis["optional_features"]
    first_nonzero_volume = _coerce_timestamp(summary.get("first_nonzero_volume"))

    insights = [
        {
            "id": "long_term_growth",
            "title": "Massive long-term price appreciation",
            "category": "growth",
            "severity": "high",
            "summary": (
                f"The stock moved from {_currency(summary['start_close'])} at the start of the dataset "
                f"to {summary['latest_close_display']} by the latest observation."
            ),
            "detail": f"That is a cumulative move of {_pct(summary['total_return_pct'])} across {dataset['years_covered']} years.",
            "metric_label": "Long-term appreciation",
            "metric_value": _pct(summary["total_return_pct"]),
            "metric_sub": f"{dataset['start_year']} to {dataset['end_year']}",
            "tags": ["growth", "overview"],
            "section": "overview",
            "priority": 100,
        },
        {
            "id": "early_data_quirk",
            "title": "Early historical rows look adjusted or backfilled",
            "category": "quality",
            "severity": "medium",
            "summary": (
                f"The first {summary['early_flat_prefix']:,} rows have Open = High = Low = Close, "
                "with flat or missing trading activity."
            ),
            "detail": "That usually points to split-adjusted history or backfilled records rather than full intraday detail.",
            "metric_label": "Backfilled-style rows",
            "metric_value": f"{summary['early_flat_prefix']:,}",
            "metric_sub": "from the start of history",
            "tags": ["quality", "overview"],
            "section": "data_notes",
            "priority": 96,
            "condition": summary["early_flat_prefix"] >= 20,
        },
        {
            "id": "volume_appears_later",
            "title": "Real trading volume appears later in the series",
            "category": "volume",
            "severity": "medium",
            "summary": (
                f"Non-zero volume does not begin until "
                f"{first_nonzero_volume.strftime('%b %d, %Y') if first_nonzero_volume is not None else 'later in the dataset'}."
            ),
            "detail": "That means the earliest rows are usable for price direction, but not for true activity analysis.",
            "metric_label": "First non-zero volume",
            "metric_value": first_nonzero_volume.strftime("%Y") if first_nonzero_volume is not None else "n/a",
            "metric_sub": "activity begins later",
            "tags": ["volume", "quality"],
            "section": "volume",
            "priority": 88,
            "condition": first_nonzero_volume is not None and first_nonzero_volume.year > dataset["start_year"] + 1,
        },
        {
            "id": "recent_correction",
            "title": "Recent correction stands out against the longer trend",
            "category": "volatility",
            "severity": "high",
            "summary": (
                f"The latest price is {_pct(summary['recent_correction_pct'])} below the trailing 12-month peak of "
                f"{_currency(summary['recent_peak'])}."
            ),
            "detail": "This is the clearest recent regime shift in the dataset and is worth centering in the dashboard.",
            "metric_label": "Recent correction",
            "metric_value": _pct(summary["recent_correction_pct"]),
            "metric_sub": "vs trailing 12M peak",
            "tags": ["volatility", "growth"],
            "section": "volatility",
            "priority": 97,
            "condition": summary["recent_correction_pct"] <= -10,
        },
        {
            "id": "sparse_dividends",
            "title": "Dividend events are sparse relative to the full history",
            "category": "dividends",
            "severity": "low",
            "summary": f"The dataset records only {summary['dividend_events']} non-zero dividend rows across {dataset['row_count']:,} trading days.",
            "detail": "That makes dividends useful as timeline markers, but not a dense signal for pattern mining.",
            "metric_label": "Dividend events",
            "metric_value": f"{summary['dividend_events']:,}",
            "metric_sub": "non-zero payout rows",
            "tags": ["dividends", "overview"],
            "section": "data_notes",
            "priority": 76,
            "condition": optional_features["has_dividends"] and summary["dividend_events"] > 0,
        },
        {
            "id": "weekday_edge",
            "title": "Intraday returns are not evenly distributed across weekdays",
            "category": "seasonality",
            "severity": "medium",
            "summary": (
                f"{signals['weekday']['best_label']} is the strongest weekday on average, "
                f"while {signals['weekday']['worst_label']} is the weakest."
            ),
            "detail": f"The spread between the best and worst weekday is {signals['weekday']['spread']:.3f} percentage points.",
            "metric_label": "Weekday spread",
            "metric_value": f"{signals['weekday']['spread']:.3f}%",
            "metric_sub": f"{signals['weekday']['best_label']} vs {signals['weekday']['worst_label']}",
            "tags": ["seasonality"],
            "section": "seasonality",
            "priority": 85,
            "condition": signals["weekday"]["spread"] >= 0.02,
        },
        {
            "id": "month_effect",
            "title": "Monthly seasonality is stronger than most users would expect",
            "category": "seasonality",
            "severity": "medium",
            "summary": (
                f"{signals['month']['best_label']} is the strongest month on average, while "
                f"{', '.join(signals['month']['worst_labels'])} sit at the bottom."
            ),
            "detail": f"The spread between the strongest and weakest month is {signals['month']['spread']:.3f} percentage points.",
            "metric_label": "Month spread",
            "metric_value": f"{signals['month']['spread']:.3f}%",
            "metric_sub": f"best month: {signals['month']['best_label']}",
            "tags": ["seasonality"],
            "section": "seasonality",
            "priority": 83,
            "condition": signals["month"]["spread"] >= 0.05,
        },
        {
            "id": "volatility_regime",
            "title": "Volatility clusters by decade",
            "category": "volatility",
            "severity": "medium",
            "summary": (
                f"{signals['decade']['most_volatile']} were the most volatile decade, while "
                f"{signals['decade']['calmest']} were the calmest."
            ),
            "detail": f"Average daily range differs by {signals['decade']['spread']:.2f} percentage points across decades.",
            "metric_label": "Volatility spread",
            "metric_value": f"{signals['decade']['spread']:.2f}%",
            "metric_sub": f"{signals['decade']['most_volatile']} vs {signals['decade']['calmest']}",
            "tags": ["volatility"],
            "section": "volatility",
            "priority": 84,
            "condition": signals["decade"]["spread"] >= 0.25,
        },
        {
            "id": "overnight_gap_bias",
            "title": "Overnight gaps reveal a directional bias",
            "category": "gaps",
            "severity": "medium",
            "summary": (
                f"The stock opened with a positive gap {summary['gap_up']:,} times, "
                f"versus {summary['gap_down']:,} negative gap days."
            ),
            "detail": f"Another {summary['gap_flat']:,} sessions opened essentially flat.",
            "metric_label": "Gap-up days",
            "metric_value": f"{summary['gap_up']:,}",
            "metric_sub": f"vs {summary['gap_down']:,} gap-down days",
            "tags": ["gaps"],
            "section": "gaps",
            "priority": 81,
            "condition": (summary["gap_up"] + summary["gap_down"]) > 0,
        },
        {
            "id": "volume_cluster",
            "title": "Extreme volume is clustered, not evenly spread",
            "category": "volume",
            "severity": "medium",
            "summary": (
                f"The ten highest-volume sessions cluster around {summary['top_volume_period']}, "
                f"within roughly a {summary['top_volume_span_years']}-year span."
            ),
            "detail": "That usually indicates a stress regime, crisis window, or major company event rather than normal trading.",
            "metric_label": "Volume cluster span",
            "metric_value": f"{summary['top_volume_span_years']}y",
            "metric_sub": str(summary["top_volume_period"]),
            "tags": ["volume", "volatility"],
            "section": "volume",
            "priority": 82,
            "condition": summary["top_volume_span_years"] <= 5,
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
        analysis = analyze_financial_context(context)
    if analysis is None:
        return None

    insight_bundle = build_financial_insight_candidates(analysis, user_prompt)
    insights = insight_bundle["insights"]
    approved_set = set(approved_insight_ids or [item["id"] for item in insights if item.get("recommended", True)])
    approved_insights = [item for item in insights if item["id"] in approved_set]
    if not approved_insights:
        approved_insights = insights[:4]

    settings = settings or {}
    included_sections = settings.get("included_sections") or _default_sections(approved_insights)
    title = settings.get("title") or "Hidden Market Structure"
    subtitle = settings.get("subtitle") or (
        f"Approved insight blueprint across {analysis['dataset']['start_year']} to {analysis['dataset']['end_year']}"
    )
    metric_count = int(settings.get("metric_count", 4))
    show_notes = bool(settings.get("show_notes", True))

    payload = {
        "kind": "financial_timeseries",
        "title": title,
        "subtitle": subtitle,
        "headline": {"title": title, "subtitle": subtitle},
        "approved_insights": approved_insights,
        "metric_cards": approved_insights[:metric_count],
        "sections": included_sections,
        "layout_sections": _build_layout_sections(
            approved_insights=approved_insights,
            metric_cards=approved_insights[:metric_count],
            analysis=analysis,
            included_sections=included_sections,
            show_notes=show_notes,
        ),
        "all_layout_sections": _build_layout_sections(
            approved_insights=approved_insights,
            metric_cards=approved_insights,
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
    height = 880 + (120 * len(included_sections)) + (180 if show_notes else 0)
    return {
        "kind": "financial_timeseries",
        "title": title,
        "html": html,
        "height": height,
        "blueprint": payload,
        "payload": payload,
        "download_name": "financial_insights_dashboard.html",
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


def dashboard_section_options() -> dict[str, str]:
    return SECTION_CONFIG


def _coerce_numeric_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")
    cleaned = (
        series.astype(str)
        .str.strip()
        .str.replace(r"[$,%]", "", regex=True)
        .str.replace(",", "", regex=False)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def _detect_financial_columns(df: pd.DataFrame) -> Optional[dict[str, dict[str, str]]]:
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


def _leading_flat_rows(df: pd.DataFrame) -> int:
    count = 0
    for _, row in df.iterrows():
        is_flat = (
            math.isclose(row["open"], row["high"], rel_tol=0, abs_tol=1e-9)
            and math.isclose(row["open"], row["low"], rel_tol=0, abs_tol=1e-9)
            and math.isclose(row["open"], row["close"], rel_tol=0, abs_tol=1e-9)
            and float(row["volume"] or 0.0) == 0.0
        )
        if not is_flat:
            break
        count += 1
    return count


def _first_nonzero_volume_date(df: pd.DataFrame) -> Optional[pd.Timestamp]:
    subset = df[df["volume"] > 0]
    if subset.empty:
        return None
    return subset.iloc[0]["date"]


def _coerce_timestamp(value: Any) -> Optional[pd.Timestamp]:
    if value in (None, "", "null"):
        return None
    if isinstance(value, pd.Timestamp):
        return value
    coerced = pd.to_datetime(value, errors="coerce")
    if pd.isna(coerced):
        return None
    return pd.Timestamp(coerced)


def _cluster_span_years(series: pd.Series) -> int:
    if series.empty:
        return 0
    return int(series.dt.year.max() - series.dt.year.min())


def _dominant_volume_period(series: pd.Series) -> str:
    if series.empty:
        return "n/a"
    years = series.dt.year
    if years.empty:
        return "n/a"
    dominant_year = int(years.mode().iloc[0])
    return f"{dominant_year-1} to {dominant_year+1}"


def _streaks(daily_return: pd.Series) -> tuple[int, int]:
    longest_win = 0
    longest_loss = 0
    current_win = 0
    current_loss = 0
    for value in daily_return.fillna(0.0):
        if value > 0:
            current_win += 1
            current_loss = 0
        elif value < 0:
            current_loss += 1
            current_win = 0
        else:
            current_win = 0
            current_loss = 0
        longest_win = max(longest_win, current_win)
        longest_loss = max(longest_loss, current_loss)
    return longest_win, longest_loss


def _currency(value: float) -> str:
    return f"${value:,.2f}"


def _pct(value: float) -> str:
    return f"{value:+.1f}%"


def _default_sections(insights: list[dict[str, Any]]) -> list[str]:
    sections = ["overview"]
    for section in ["seasonality", "volatility", "gaps", "volume", "data_notes"]:
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

    section_map = {
        "overview": {
            "id": "overview",
            "title": "Market Narrative",
            "description": "The fastest read on what matters before someone opens the raw chart history.",
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
        "seasonality": {
            "id": "seasonality",
            "title": "Calendar Effects",
            "description": "Weekday and monthly behavior that is easy to miss in a plain price chart.",
            "blocks": [
                {
                    "id": "weekday-effect",
                    "kind": "chart",
                    "chart": {
                        "id": "weekday-effect",
                        "title": "Average intraday return by weekday",
                        "subtitle": f"{signals['weekday']['best_label']} leads while {signals['weekday']['worst_label']} trails.",
                        "type": "bar",
                        "labels": signals["weekday"]["labels"],
                        "series": [
                            {
                                "name": "Avg intraday return",
                                "values": signals["weekday"]["values"],
                                "color": "#3B82F6",
                            }
                        ],
                        "format": "percent",
                    },
                },
                {
                    "id": "month-effect",
                    "kind": "chart",
                    "chart": {
                        "id": "month-effect",
                        "title": "Average intraday return by month",
                        "subtitle": f"{signals['month']['best_label']} is strongest; {', '.join(signals['month']['worst_labels'])} are weakest.",
                        "type": "bar",
                        "labels": signals["month"]["labels"],
                        "series": [
                            {
                                "name": "Avg monthly effect",
                                "values": signals["month"]["values"],
                                "color": "#1D4ED8",
                            }
                        ],
                        "format": "percent",
                    },
                },
            ],
        },
        "volatility": {
            "id": "volatility",
            "title": "Volatility Regime",
            "description": "Stress periods and recovery markers that deserve a place in the story.",
            "blocks": [
                {
                    "id": "decade-volatility",
                    "kind": "chart",
                    "chart": {
                        "id": "decade-volatility",
                        "title": "Average daily range by decade",
                        "subtitle": f"{signals['decade']['most_volatile']} were the most volatile; {signals['decade']['calmest']} were the calmest.",
                        "type": "bar",
                        "labels": signals["decade"]["labels"],
                        "series": [
                            {
                                "name": "Daily range",
                                "values": signals["decade"]["values"],
                                "color": "#0F172A",
                            }
                        ],
                        "format": "percent",
                    },
                },
                {
                    "id": "volatility-stats",
                    "kind": "stat_list",
                    "title": "Stress markers",
                    "items": [
                        {"label": "Worst year", "value": f"{summary['worst_year']} ({_pct(summary['worst_year_return_pct'])})", "tone": "danger"},
                        {"label": "Best year", "value": f"{summary['best_year']} ({_pct(summary['best_year_return_pct'])})", "tone": "positive"},
                        {"label": "Deepest drawdown", "value": _pct(summary["deepest_drawdown_pct"]), "tone": "warning"},
                        {"label": "Winning streak", "value": f"{summary['winning_streak']} sessions", "tone": "default"},
                        {"label": "Losing streak", "value": f"{summary['losing_streak']} sessions", "tone": "default"},
                    ],
                },
            ],
        },
        "gaps": {
            "id": "gaps",
            "title": "Overnight Gap Behavior",
            "description": "Opening behavior often reveals hidden directional bias and stress clustering.",
            "blocks": [
                {
                    "id": "gap-distribution",
                    "kind": "chart",
                    "chart": {
                        "id": "gap-distribution",
                        "title": "Gap distribution",
                        "subtitle": "How often the stock opens up, down, or effectively flat.",
                        "type": "pie",
                        "labels": signals["gap"]["labels"],
                        "series": [
                            {
                                "name": "Sessions",
                                "values": signals["gap"]["values"],
                                "color": "#3B82F6",
                            }
                        ],
                        "format": "number",
                    },
                },
                {
                    "id": "gap-stats",
                    "kind": "stat_list",
                    "title": "Gap counts",
                    "items": [
                        {"label": "Gap up (>0.5%)", "value": f"{summary['gap_up']:,}", "tone": "positive"},
                        {"label": "Gap down (<-0.5%)", "value": f"{summary['gap_down']:,}", "tone": "danger"},
                        {"label": "Flat open", "value": f"{summary['gap_flat']:,}", "tone": "default"},
                    ],
                },
            ],
        },
        "volume": {
            "id": "volume",
            "title": "Volume Clustering",
            "description": "Highest-volume periods usually signal stress regimes rather than normal activity.",
            "blocks": [
                {
                    "id": "top-volume",
                    "kind": "chart",
                    "chart": {
                        "id": "top-volume",
                        "title": "Top-volume sessions",
                        "subtitle": f"The biggest sessions cluster around {summary['top_volume_period']}.",
                        "type": "bar",
                        "labels": signals["volume"]["labels"],
                        "series": [
                            {
                                "name": "Volume (millions)",
                                "values": signals["volume"]["values"],
                                "color": "#F59E0B",
                            }
                        ],
                        "format": "number",
                    },
                }
            ],
        },
        "data_notes": {
            "id": "data_notes",
            "title": "Approved Insight Notes",
            "description": "Quality and context notes that should travel with the dashboard narrative.",
            "blocks": [
                {
                    "id": "data-note-list",
                    "kind": "note_list",
                    "insights": [_insight_card(insight) for insight in approved_insights],
                }
            ],
        },
    }

    layout_sections: list[dict[str, Any]] = []
    for section_id in included_sections:
        if section_id == "data_notes" and not show_notes:
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
        --panel: rgba(255,255,255,0.94);
        --panel-strong: #ffffff;
        --text: #16324f;
        --text-soft: #587189;
        --border: rgba(22,50,79,0.12);
      }
      body { margin: 0; background: linear-gradient(180deg, #f7fafc 0%, #edf3f7 100%); font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: var(--text); }
      .dash { padding: 1.2rem 0.2rem; }
      .headline { margin-bottom: 1rem; }
      .headline h2 { margin: 0; font-size: 28px; color: var(--text); }
      .headline p { margin: 0.4rem 0 0; color: var(--text-soft); font-size: 14px; }
      .focus-tags { display:flex; flex-wrap:wrap; gap:8px; margin-top:0.75rem; }
      .focus-tag { font-size:11px; color:#1f5f8b; background:#e3f1fb; border-radius:999px; padding:0.3rem 0.55rem; font-weight:700; }
      .section-label { font-size: 11px; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase; color: var(--text-soft); margin: 1.4rem 0 0.6rem; }
      .metric-grid { display: grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap: 10px; margin-bottom: 0.5rem; }
      .metric { background: var(--panel); border-radius: 18px; padding: 0.85rem 1rem; border: 1px solid var(--border); box-shadow: 0 12px 30px rgba(22,50,79,0.05); }
      .metric .label { font-size: 11px; color: var(--text-soft); margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.06em; }
      .metric .value { font-size: 22px; font-weight: 700; }
      .metric .sub { font-size: 12px; color: var(--text-soft); margin-top: 4px; }
      .chart-row { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 1rem; }
      .chart-box, .wide-box { background: var(--panel-strong); border: 1px solid var(--border); border-radius: 20px; padding: 1rem; box-shadow: 0 12px 28px rgba(22,50,79,0.04); }
      .wide-box { margin-bottom: 1rem; }
      .chart-title { font-size: 14px; font-weight: 700; color: var(--text); margin-bottom: 12px; }
      .chart-sub { font-size: 12px; color: var(--text-soft); margin-top: 8px; line-height: 1.45; }
      .legend { display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 8px; font-size: 11px; color: var(--text-soft); }
      .legend span { display: flex; align-items: center; gap: 4px; }
      .leg-dot { width: 10px; height: 10px; border-radius: 2px; }
      .gap-grid { display:grid; grid-template-columns:1fr 1fr; gap:16px; align-items:center; }
      .gap-stats { display:flex; flex-direction:column; gap:8px; }
      .gap-row { display:flex; justify-content:space-between; font-size:12px; color: var(--text); }
      .gap-row strong { font-weight:700; }
      .note-list { display:grid; grid-template-columns:1fr 1fr; gap:12px; }
      .note-card { background: var(--panel); border:1px solid var(--border); border-radius:16px; padding:0.9rem; }
      .note-card h4 { margin:0 0 0.4rem; font-size:13px; color:var(--text); }
      .note-card p { margin:0; font-size:12px; line-height:1.45; color:var(--text-soft); }
      @media (max-width: 900px) {
        .metric-grid, .chart-row, .gap-grid, .note-list { grid-template-columns: 1fr; }
      }
    </style>
  </head>
  <body>
    <div class="dash">
      <div class="headline">
        <h2>{{ payload.headline.title }}</h2>
        <p>{{ payload.headline.subtitle }}</p>
        {% if payload.focus_tags %}
        <div class="focus-tags">
          {% for tag in payload.focus_tags %}
          <span class="focus-tag">{{ tag }}</span>
          {% endfor %}
        </div>
        {% endif %}
      </div>

      {% if 'overview' in payload.sections %}
      <div class="section-label">Approved insight blueprint</div>
      <div class="metric-grid">
        {% for metric in payload.metric_cards %}
        <div class="metric">
          <div class="label">{{ metric.metric_label }}</div>
          <div class="value">{{ metric.metric_value }}</div>
          <div class="sub">{{ metric.metric_sub }}</div>
        </div>
        {% endfor %}
      </div>
      {% endif %}

      {% if 'seasonality' in payload.sections or 'volatility' in payload.sections %}
      <div class="section-label">Patterns in the tape</div>
      {% endif %}
      {% if 'seasonality' in payload.sections %}
      <div class="wide-box">
        <div class="chart-title">Avg intraday return by weekday</div>
        <div style="position:relative; height:120px;"><canvas id="dowChart"></canvas></div>
        <div class="chart-sub">{{ payload.signals.weekday.best_label }} is strongest on average; {{ payload.signals.weekday.worst_label }} is weakest.</div>
      </div>
      {% endif %}

      {% if 'seasonality' in payload.sections or 'volatility' in payload.sections %}
      <div class="chart-row">
        {% if 'seasonality' in payload.sections %}
        <div class="chart-box">
          <div class="chart-title">Avg intraday return by month</div>
          <div class="legend">
            <span><span class="leg-dot" style="background:#27ae60;"></span>Positive</span>
            <span><span class="leg-dot" style="background:#c0392b;"></span>Negative</span>
          </div>
          <div style="position:relative; height:200px;"><canvas id="monthChart"></canvas></div>
          <div class="chart-sub">{{ payload.signals.month.best_label }} leads, while {{ payload.signals.month.worst_labels | join(', ') }} trail.</div>
        </div>
        {% endif %}
        {% if 'volatility' in payload.sections %}
        <div class="chart-box">
          <div class="chart-title">Avg daily volatility by decade</div>
          <div style="position:relative; height:200px;"><canvas id="decadeChart"></canvas></div>
          <div class="chart-sub">{{ payload.signals.decade.most_volatile }} were the most volatile. {{ payload.signals.decade.calmest }} were the calmest.</div>
        </div>
        {% endif %}
      </div>
      {% endif %}

      {% if 'gaps' in payload.sections %}
      <div class="section-label">Overnight gap behavior</div>
      <div class="wide-box">
        <div class="chart-title">How often does price gap up vs. gap down vs. open flat?</div>
        <div class="gap-grid">
          <div style="position:relative; height:180px;"><canvas id="gapChart"></canvas></div>
          <div>
            <div style="margin-bottom:10px; font-size:13px; color:var(--text-soft);">Gap structure often reveals overnight sentiment and event risk that is invisible in end-of-day close prices alone.</div>
            <div class="gap-stats">
              <div class="gap-row"><span style="color:#27ae60; font-weight:600;">Gap Up (&gt;0.5%)</span><strong>{{ "{:,}".format(payload.summary.gap_up) }} days</strong></div>
              <div class="gap-row"><span style="color:#c0392b; font-weight:600;">Gap Down (&lt;-0.5%)</span><strong>{{ "{:,}".format(payload.summary.gap_down) }} days</strong></div>
              <div class="gap-row"><span style="color:#7b8794; font-weight:600;">Flat open</span><strong>{{ "{:,}".format(payload.summary.gap_flat) }} days</strong></div>
            </div>
          </div>
        </div>
      </div>
      {% endif %}

      {% if 'volume' in payload.sections %}
      <div class="section-label">Extreme volume</div>
      <div class="wide-box">
        <div class="chart-title">Top high-volume sessions</div>
        <div style="position:relative; height:150px;"><canvas id="volChart"></canvas></div>
        <div class="chart-sub">The highest-volume sessions are compressed into {{ payload.summary.top_volume_period }}, which is a strong sign of regime clustering.</div>
      </div>
      {% endif %}

      {% if payload.show_notes and 'data_notes' in payload.sections %}
      <div class="section-label">Approved insights behind this dashboard</div>
      <div class="note-list">
        {% for insight in payload.approved_insights %}
        <div class="note-card">
          <h4>{{ insight.title }}</h4>
          <p>{{ insight.summary }} {{ insight.detail }}</p>
        </div>
        {% endfor %}
      </div>
      {% endif %}
    </div>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
    <script>
      const payload = {{ payload_json | safe }};
      const gridColor = 'rgba(0,0,0,0.06)';
      const textColor = '#587189';

      if (document.getElementById('dowChart')) {
        new Chart(document.getElementById('dowChart'), {
          type: 'bar',
          data: {
            labels: payload.signals.weekday.labels,
            datasets: [{
              data: payload.signals.weekday.values,
              backgroundColor: payload.signals.weekday.values.map(v => v >= 0 ? '#27ae60' : '#c0392b'),
              borderRadius: 4
            }]
          },
          options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
              x: { grid: { display: false }, ticks: { color: textColor, font: { size: 11 } } },
              y: { grid: { color: gridColor }, ticks: { color: textColor, font: { size: 10 }, callback: v => Number(v).toFixed(2) + '%' } }
            }
          }
        });
      }

      if (document.getElementById('monthChart')) {
        new Chart(document.getElementById('monthChart'), {
          type: 'bar',
          data: {
            labels: payload.signals.month.labels,
            datasets: [{
              data: payload.signals.month.values,
              backgroundColor: payload.signals.month.values.map(v => v >= 0 ? '#27ae60cc' : '#c0392bcc'),
              borderRadius: 3
            }]
          },
          options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
              x: { grid: { display: false }, ticks: { color: textColor, font: { size: 10 } } },
              y: { grid: { color: gridColor }, ticks: { color: textColor, font: { size: 10 }, callback: v => Number(v).toFixed(2) + '%' } }
            }
          }
        });
      }

      if (document.getElementById('decadeChart')) {
        new Chart(document.getElementById('decadeChart'), {
          type: 'bar',
          data: {
            labels: payload.signals.decade.labels,
            datasets: [{
              data: payload.signals.decade.values,
              backgroundColor: '#378ADD99',
              borderRadius: 4
            }]
          },
          options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
              x: { grid: { display: false }, ticks: { color: textColor, font: { size: 10 } } },
              y: { grid: { color: gridColor }, ticks: { color: textColor, font: { size: 10 }, callback: v => Number(v).toFixed(1) + '%' } }
            }
          }
        });
      }

      if (document.getElementById('gapChart')) {
        new Chart(document.getElementById('gapChart'), {
          type: 'doughnut',
          data: {
            labels: payload.signals.gap.labels,
            datasets: [{ data: payload.signals.gap.values, backgroundColor: ['#27ae60', '#c0392b', '#b0b0b0'], borderWidth: 0 }]
          },
          options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            cutout: '65%'
          }
        });
      }

      if (document.getElementById('volChart')) {
        new Chart(document.getElementById('volChart'), {
          type: 'bar',
          data: {
            labels: payload.signals.volume.labels,
            datasets: [{
              data: payload.signals.volume.values,
              backgroundColor: payload.signals.volume.values.map((_, idx) => idx === 0 ? '#EF9F27' : '#378ADD'),
              borderRadius: 3
            }]
          },
          options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
              x: { grid: { display: false }, ticks: { color: textColor, font: { size: 9 }, maxRotation: 35 } },
              y: { grid: { color: gridColor }, ticks: { color: textColor, font: { size: 10 }, callback: v => v + 'M' } }
            }
          }
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
