from __future__ import annotations

from typing import Any, Callable, Iterable


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
