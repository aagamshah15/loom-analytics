from __future__ import annotations

import json
from typing import Any, Callable, Iterable, Optional

from jinja2 import Environment, Template
from markupsafe import Markup

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
