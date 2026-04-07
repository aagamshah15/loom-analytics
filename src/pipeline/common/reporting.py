from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from jinja2 import Template

from pipeline.common.contracts import PipelineContext


def write_reports(context: PipelineContext) -> dict[str, str]:
    if context.output_dir is None:
        raise ValueError("PipelineContext.output_dir is required to write reports.")

    context.output_dir.mkdir(parents=True, exist_ok=True)
    report = build_report_dict(context)

    json_path = context.output_dir / "report.json"
    json_path.write_text(json.dumps(report, indent=2, default=_json_default))

    markdown_path = context.output_dir / "summary.md"
    markdown_path.write_text(build_markdown_summary(context))

    html_path = context.output_dir / "summary.html"
    html_path.write_text(build_html_summary(context))

    return {
        "json": str(json_path),
        "markdown": str(markdown_path),
        "html": str(html_path),
    }


def build_report_dict(context: PipelineContext) -> dict[str, Any]:
    active_df = context.clean_df if context.clean_df is not None else context.raw_df
    return {
        "run_id": context.run_id,
        "input_path": str(context.input_path) if context.input_path else None,
        "output_dir": str(context.output_dir) if context.output_dir else None,
        "metadata": _normalize(context.metadata),
        "schema": _normalize(context.schema),
        "quality_report": _normalize(context.quality_report),
        "transform_log": _normalize(context.transform_log),
        "analysis_results": _normalize(context.analysis_results),
        "insights": _normalize(context.insights),
        "charts": context.charts,
        "warnings": context.warnings,
        "errors": context.errors,
        "row_count": int(active_df.shape[0]) if active_df is not None else 0,
        "column_count": int(active_df.shape[1]) if active_df is not None else 0,
    }


def build_markdown_summary(context: PipelineContext) -> str:
    quality_score = context.quality_report.get("score", "n/a")
    insight_items = context.insights.get("items", [])
    lines = [
        "# Loom Summary",
        "",
        f"- Run ID: `{context.run_id}`",
        f"- Input: `{context.input_path}`" if context.input_path else "- Input: `n/a`",
        f"- Quality score: `{quality_score}`",
        f"- Charts generated: `{len(context.charts)}`",
        f"- Warnings: `{len(context.warnings)}`",
        f"- Errors: `{len(context.errors)}`",
        "",
        "## Key Insights",
    ]

    if insight_items:
        for item in insight_items:
            lines.append(f"- {item['message']} ({item['severity']})")
    else:
        lines.append("- No rule-based insights were triggered.")

    lines.extend(["", "## Recommendations"])
    recommendations = context.insights.get("recommendations", [])
    if recommendations:
        for item in recommendations:
            lines.append(f"- {item}")
    else:
        lines.append("- No follow-up actions recommended.")

    if context.warnings:
        lines.extend(["", "## Warnings"])
        for warning in context.warnings:
            lines.append(f"- {warning}")

    if context.errors:
        lines.extend(["", "## Errors"])
        for error in context.errors:
            lines.append(f"- {error['stage']}: {error['message']}")

    if context.charts:
        lines.extend(["", "## Chart Artifacts"])
        for chart in context.charts:
            lines.append(f"- `{chart}`")

    return "\n".join(lines) + "\n"


def build_html_summary(context: PipelineContext) -> str:
    template = Template(
        """
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>Loom Summary</title>
    <style>
      body { font-family: Georgia, serif; margin: 2rem auto; max-width: 900px; color: #1b1b1b; }
      h1, h2 { color: #16324f; }
      .pill { display: inline-block; padding: 0.2rem 0.6rem; margin-right: 0.5rem; background: #eef4f8; border-radius: 999px; }
      .card { border: 1px solid #d7e2ea; border-radius: 12px; padding: 1rem; margin: 1rem 0; background: #fbfdfe; }
      ul { padding-left: 1.25rem; }
      code { background: #f1f4f6; padding: 0.1rem 0.3rem; border-radius: 4px; }
    </style>
  </head>
  <body>
    <h1>Loom Summary</h1>
    <p>
      <span class="pill">Run: {{ run_id }}</span>
      <span class="pill">Quality score: {{ quality_score }}</span>
      <span class="pill">Charts: {{ chart_count }}</span>
    </p>
    <div class="card">
      <strong>Input:</strong> <code>{{ input_path }}</code>
    </div>
    <h2>Insights</h2>
    <ul>
      {% if insights %}
        {% for item in insights %}
        <li>{{ item.message }} ({{ item.severity }})</li>
        {% endfor %}
      {% else %}
        <li>No rule-based insights were triggered.</li>
      {% endif %}
    </ul>
    <h2>Recommendations</h2>
    <ul>
      {% if recommendations %}
        {% for item in recommendations %}
        <li>{{ item }}</li>
        {% endfor %}
      {% else %}
        <li>No follow-up actions recommended.</li>
      {% endif %}
    </ul>
    {% if warnings %}
    <h2>Warnings</h2>
    <ul>
      {% for item in warnings %}
      <li>{{ item }}</li>
      {% endfor %}
    </ul>
    {% endif %}
    {% if charts %}
    <h2>Charts</h2>
    <ul>
      {% for chart in charts %}
      <li><code>{{ chart }}</code></li>
      {% endfor %}
    </ul>
    {% endif %}
  </body>
</html>
        """
    )
    return template.render(
        run_id=context.run_id,
        quality_score=context.quality_report.get("score", "n/a"),
        chart_count=len(context.charts),
        input_path=str(context.input_path) if context.input_path else "n/a",
        insights=context.insights.get("items", []),
        recommendations=context.insights.get("recommendations", []),
        warnings=context.warnings,
        charts=context.charts,
    )


def _normalize(value: Any) -> Any:
    if isinstance(value, pd.DataFrame):
        return value.to_dict(orient="records")
    if isinstance(value, pd.Series):
        return value.to_dict()
    if isinstance(value, dict):
        return {str(key): _normalize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    if isinstance(value, tuple):
        return [_normalize(item) for item in value]
    if isinstance(value, np.generic):
        return value.item()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except TypeError:
            return str(value)
    return value


def _json_default(value: Any) -> Any:
    normalized = _normalize(value)
    if normalized is value:
        return str(value)
    return normalized
