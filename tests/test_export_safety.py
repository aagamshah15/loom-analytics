from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
TESTS_ROOT = PROJECT_ROOT / "tests"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))

from pipeline.business import (
    ecommerce_dashboard,
    financial_dashboard,
    healthcare_dashboard,
    hr_dashboard,
    marketing_dashboard,
    survey_dashboard,
    web_analytics_dashboard,
)
from pipeline.business.router import build_dashboard, build_insight_candidates, detect_business_context
from pipeline.common.contracts import PipelineContext
from template_generators import PROFILE_GENERATORS

RENDERERS = {
    "financial_timeseries": financial_dashboard._render_dashboard_html,
    "ecommerce_orders": ecommerce_dashboard._render_dashboard_html,
    "healthcare_medical": healthcare_dashboard._render_dashboard_html,
    "hr_workforce": hr_dashboard._render_dashboard_html,
    "marketing_campaign": marketing_dashboard._render_dashboard_html,
    "survey_sentiment": survey_dashboard._render_dashboard_html,
    "web_app_analytics": web_analytics_dashboard._render_dashboard_html,
}

SCRIPT_TAG = "<script>alert(1)</script>"
IMG_TAG = "<img src=x onerror=alert(2)>"
PAYLOAD = f'"> {SCRIPT_TAG}{IMG_TAG}'

# Keys the export templates branch on; changing them would hide content instead of testing it.
STRUCTURAL_KEYS = {"kind", "id", "type", "format", "section", "sections", "severity", "tone", "color"}


def poison(value: Any) -> Any:
    """Append markup to every displayable string in a dashboard payload."""
    if isinstance(value, str):
        return value + PAYLOAD
    if isinstance(value, list):
        return [poison(item) for item in value]
    if isinstance(value, dict):
        return {key: item if key in STRUCTURAL_KEYS else poison(item) for key, item in value.items()}
    return value


class ExportSafetyTests(unittest.TestCase):
    def test_exports_never_emit_dataset_or_user_text_as_markup(self) -> None:
        for name, generator in PROFILE_GENERATORS.items():
            with self.subTest(profile=name):
                frame = generator(rows=120)
                detected = detect_business_context(PipelineContext(clean_df=frame, raw_df=frame))
                self.assertIsNotNone(detected)
                review = build_insight_candidates(detected["kind"], detected["analysis"], "")
                dashboard = build_dashboard(
                    detected["kind"],
                    detected["analysis"],
                    approved_insight_ids=[item["id"] for item in review["insights"]],
                    settings={},
                )

                html = RENDERERS[detected["kind"]](poison(dashboard["payload"]))

                self.assertIn("alert(1)", html, "the poisoned text should still be rendered, escaped")
                self.assertNotIn(SCRIPT_TAG, html)
                self.assertNotIn("<img src=x", html)
                # Exports that build markup in the browser must escape every interpolated field.
                self.assertIsNone(re.search(r"\$\{(?!esc\()(?:card|insight|payload)\.", html))


if __name__ == "__main__":
    unittest.main()
