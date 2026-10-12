from __future__ import annotations

import json
import re
from typing import Any, Callable, Iterable, Optional

import pandas as pd
from jinja2 import Environment, Template
from markupsafe import Markup

from pipeline.common.contracts import PipelineContext

_HTML_ENV = Environment(autoescape=True)


def collect_insights(builders: Iterable[Callable[[], dict[str, Any]]]) -> list[dict[str, Any]]:
    """Build each insight candidate, keeping only the ones this dataset can support.

    Candidates are passed as callables so that one built on a metric the dataset
    does not have (a ``None`` summary value) is skipped instead of failing the
    whole review. A missing key still raises: that means the analysis payload
    does not belong to this template.
    """
    insights: list[dict[str, Any]] = []
    for build in builders:
        try:
            insight = build()
        except (AttributeError, TypeError, ValueError, ZeroDivisionError):
            continue
        if insight.get("condition", True):
            insights.append(insight)
    return insights


def html_template(source: str) -> Template:
    """Compile an export template with HTML autoescaping on.

    Dashboard exports render dataset values (category labels, column names) and
    user-entered titles, so nothing may reach the page unescaped.
    """
    return _HTML_ENV.from_string(source)


def script_json(value: Any, *, default: Optional[Callable[[Any], Any]] = None) -> Markup:
    """Serialize ``value`` for embedding inside a ``<script>`` block.

    Escaping ``<``, ``>`` and ``&`` keeps a string such as ``</script>`` in the
    data from closing the block or opening markup.
    """
    text = json.dumps(value, default=default)
    return Markup(text.replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026"))


def template_frame(context: PipelineContext) -> Optional[pd.DataFrame]:
    """The dataframe a business template should read.

    Prefers the cleaned data with constant columns restored, then the cleaned
    data, then the raw upload.
    """
    for frame in (context.template_df, context.clean_df, context.raw_df):
        if frame is not None:
            return frame
    return None


def extract_prompt_terms(prompt: str, stop_words: Iterable[str] = ()) -> list[str]:
    """Distinct words of four or more letters from the user's steering prompt."""
    ignored = set(stop_words)
    terms: list[str] = []
    for token in re.findall(r"[a-z0-9]+", prompt.strip().lower()):
        if len(token) >= 4 and token not in ignored and token not in terms:
            terms.append(token)
    return terms


def score_insights(
    insights: list[dict[str, Any]],
    focus_tags: list[str],
    prompt_terms: list[str],
    *,
    minimum_recommended: int = 0,
) -> list[dict[str, Any]]:
    """Rank insights by priority plus how well they match the user's prompt.

    ``minimum_recommended`` keeps a few insights pre-approved on a dataset where
    nothing stands out, so the dashboard is never empty by default.
    """
    for item in insights:
        bonus = 15 if any(tag in focus_tags for tag in item["tags"]) else 0
        if prompt_terms:
            haystack = " ".join(
                [item["title"], item["summary"], item["detail"], item["category"], item["metric_label"], " ".join(item["tags"])]
            ).lower()
            bonus += min(sum(1 for term in prompt_terms if term in haystack), 3) * 6
        item["score"] = item["priority"] + bonus
        item["recommended"] = item["score"] >= 85
    insights.sort(key=lambda item: (-item["score"], item["title"]))
    for item in insights[:minimum_recommended]:
        item["recommended"] = True
    return insights


def metric_card(insight: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": insight["id"],
        "label": insight["metric_label"],
        "value": insight["metric_value"],
        "sub": insight["metric_sub"],
        "tone": insight["severity"],
    }


def insight_card(insight: dict[str, Any]) -> dict[str, Any]:
    keys = ("id", "title", "category", "severity", "summary", "detail", "metric_label", "metric_value", "metric_sub", "section")
    return {key: insight[key] for key in keys}


def format_number(value: Optional[float], number_format: str = "number") -> str:
    if value is None:
        return "n/a"
    if number_format == "percent":
        return f"{value:.1f}%"
    prefix = "$" if number_format == "currency" else ""
    magnitude = abs(value)
    if magnitude >= 1_000_000_000:
        return f"{prefix}{value / 1_000_000_000:.1f}B"
    if magnitude >= 1_000_000:
        return f"{prefix}{value / 1_000_000:.1f}M"
    if magnitude >= 10_000:
        return f"{prefix}{value / 1_000:.1f}K"
    if magnitude >= 100 or float(value).is_integer():
        return f"{prefix}{value:,.0f}"
    return f"{prefix}{value:,.2f}"


def render_blueprint_html(payload: dict[str, Any], eyebrow: str = "Loom dashboard") -> str:
    """Render a dashboard payload as a self-contained page: no scripts, no network requests.

    Draws exactly the sections and blocks of ``payload["layout_sections"]``, so the
    export shows what the in-app preview shows. Charts become CSS bars or an inline
    SVG line; multi-series charts are drawn from their first series.
    """
    sections = []
    for section in payload["layout_sections"]:
        blocks = [dict(block, view=_chart_view(block["chart"])) if block["kind"] == "chart" else block for block in section["blocks"]]
        sections.append(dict(section, blocks=blocks))
    return _BLUEPRINT_TEMPLATE.render(payload=payload, sections=sections, eyebrow=eyebrow)


def _chart_view(chart: dict[str, Any]) -> dict[str, Any]:
    series = chart["series"][0] if chart.get("series") else {"values": []}
    number_format = chart.get("format", "number")
    points = [
        (str(label), float(value))
        for label, value in zip(chart.get("labels", []), series["values"])
        if value is not None
    ]
    if not points:
        return {"kind": "empty"}
    values = [value for _, value in points]

    if chart.get("type") in {"line", "area"} and len(points) > 2:
        low, high = min(values), max(values)
        span = (high - low) or 1.0
        step = 600 / (len(points) - 1)
        coordinates = " ".join(
            f"{index * step:.1f},{150 - (value - low) / span * 140:.1f}" for index, value in enumerate(values)
        )
        return {
            "kind": "line",
            "points": coordinates,
            "first_label": points[0][0],
            "last_label": points[-1][0],
            "low": format_number(low, number_format),
            "high": format_number(high, number_format),
        }

    largest = max(abs(value) for value in values) or 1.0
    return {
        "kind": "bars",
        "rows": [
            {
                "label": label,
                "value": format_number(value, number_format),
                "width": round(abs(value) / largest * 100, 1),
                "negative": value < 0,
            }
            for label, value in points
        ],
    }


_BLUEPRINT_TEMPLATE = html_template(
    """<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{{ payload.title }}</title>
    <style>
      body { font-family: Arial, Helvetica, sans-serif; background: #fafaf9; color: #1c1917; margin: 0; }
      .wrap { max-width: 1100px; margin: 0 auto; padding: 32px 16px 48px; }
      .hero, .section { background: #fff; border: 1px solid #e7e5e4; border-radius: 20px; padding: 24px; margin-bottom: 20px; }
      .eyebrow { text-transform: uppercase; letter-spacing: 0.16em; font-size: 12px; color: #78716c; }
      h1 { margin: 8px 0 12px; font-size: 32px; }
      h2 { margin: 6px 0 8px; font-size: 22px; }
      h3 { margin: 6px 0 8px; font-size: 17px; }
      p { line-height: 1.5; margin: 0 0 12px; }
      .muted { color: #78716c; }
      .grid { display: grid; gap: 16px; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); margin-bottom: 16px; }
      .card { border: 1px solid #e7e5e4; border-radius: 16px; padding: 16px; }
      .insight { border-left: 4px solid #c2410c; background: #fff7ed; }
      .note { background: #f5f5f4; }
      .metric { font-size: 26px; font-weight: bold; margin: 8px 0 4px; }
      .chart { margin-bottom: 16px; }
      .bar-row { display: grid; grid-template-columns: minmax(90px, 220px) 1fr auto; gap: 10px; align-items: center; margin: 6px 0; font-size: 14px; }
      .bar-label { overflow-wrap: anywhere; }
      .bar-track { background: #f5f5f4; border-radius: 6px; height: 14px; }
      .bar-fill { background: #c2410c; border-radius: 6px; height: 14px; }
      .bar-fill.negative { background: #57534e; }
      svg { width: 100%; height: 170px; display: block; }
      .axis { display: flex; justify-content: space-between; font-size: 12px; color: #78716c; }
    </style>
  </head>
  <body>
    <div class="wrap">
      <section class="hero">
        <div class="eyebrow">{{ eyebrow }}</div>
        <h1>{{ payload.title }}</h1>
        <p class="muted">{{ payload.subtitle }}</p>
      </section>
      {% for section in sections %}
      <section class="section">
        <div class="eyebrow">{{ section.title }}</div>
        <p class="muted">{{ section.description }}</p>
        {% for block in section.blocks %}
          {% if block.kind == "metric_grid" %}
          <div class="grid">
            {% for card in block.cards %}
            <div class="card">
              <div class="eyebrow">{{ card.label }}</div>
              <div class="metric">{{ card.value }}</div>
              <div class="muted">{{ card.sub }}</div>
            </div>
            {% endfor %}
          </div>
          {% elif block.kind == "insight_grid" or block.kind == "note_list" %}
          <div class="grid">
            {% for insight in block.insights %}
            <div class="card {{ 'insight' if block.kind == 'insight_grid' else 'note' }}">
              <div class="eyebrow">{{ insight.category }}</div>
              <h3>{{ insight.title }}</h3>
              <p>{{ insight.summary if block.kind == "insight_grid" else insight.detail }}</p>
            </div>
            {% endfor %}
          </div>
          {% elif block.kind == "stat_list" %}
          <h3>{{ block.title }}</h3>
          <div class="grid">
            {% for item in block["items"] %}
            <div class="card">
              <div class="muted">{{ item.label }}</div>
              <div class="metric">{{ item.value }}</div>
            </div>
            {% endfor %}
          </div>
          {% elif block.kind == "chart" %}
          <div class="card chart">
            <h3>{{ block.chart.title }}</h3>
            <p class="muted">{{ block.chart.subtitle }}</p>
            {% if block.view.kind == "line" %}
            <svg viewBox="0 0 600 160" preserveAspectRatio="none" role="img" aria-label="{{ block.chart.title }}">
              <polyline fill="none" stroke="#c2410c" stroke-width="2" points="{{ block.view.points }}" />
            </svg>
            <div class="axis"><span>{{ block.view.first_label }}</span><span>low {{ block.view.low }} · high {{ block.view.high }}</span><span>{{ block.view.last_label }}</span></div>
            {% elif block.view.kind == "bars" %}
            {% for row in block.view.rows %}
            <div class="bar-row">
              <div class="bar-label">{{ row.label }}</div>
              <div class="bar-track"><div class="bar-fill{{ ' negative' if row.negative else '' }}" style="width: {{ row.width }}%"></div></div>
              <div>{{ row.value }}</div>
            </div>
            {% endfor %}
            {% else %}
            <p class="muted">No data for this chart.</p>
            {% endif %}
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
