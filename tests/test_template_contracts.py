from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from pipeline.business.router import (
    analyze_for_kind,
    build_dashboard,
    build_insight_candidates,
    detect_business_context,
    section_options,
)
from pipeline.run import run_pipeline


class TemplateContractTests(unittest.TestCase):
    fixtures_root = PROJECT_ROOT / "tests" / "fixtures"
    manifest_path = fixtures_root / "template_manifest.json"

    @classmethod
    def setUpClass(cls) -> None:
        manifest = json.loads(cls.manifest_path.read_text())
        cls.fixture_entries = [
            entry
            for template in manifest["templates"]
            for entry in template["fixtures"]
        ]

    def _context_for(self, manifest_path: str):
        return run_pipeline(
            input_path=self.fixtures_root / manifest_path,
            persist_outputs=False,
            include_visualizations=False,
        )

    def test_positive_template_fixtures_satisfy_router_contract(self) -> None:
        for entry in self.fixture_entries:
            with self.subTest(path=entry["path"]):
                context = self._context_for(entry["path"])
                self.assertFalse(context.errors, f"{entry['path']} should ingest without pipeline errors")

                detected = detect_business_context(context)
                self.assertIsNotNone(detected, f"{entry['path']} should detect a business template")
                self.assertEqual(detected["kind"], entry["expected_kind"])

                analysis_payload = analyze_for_kind(context, entry["expected_kind"])
                self.assertIsNotNone(analysis_payload)
                if entry["expected_profile"] is not None:
                    self.assertEqual(analysis_payload["analysis"].get("profile"), entry["expected_profile"])

                insights = build_insight_candidates(entry["expected_kind"], analysis_payload["analysis"], "")
                self.assertGreater(len(insights["insights"]), 0, f"{entry['path']} should produce insight candidates")

                approved_ids = [item["id"] for item in insights["insights"][:3]]
                included_sections = list(section_options(entry["expected_kind"]).keys())[:4]
                dashboard = build_dashboard(
                    entry["expected_kind"],
                    analysis_payload["analysis"],
                    approved_insight_ids=approved_ids,
                    settings={
                        "title": f"{entry['expected_kind']} contract check",
                        "subtitle": entry["schema_family"],
                        "included_sections": included_sections,
                        "metric_count": 3,
                        "show_notes": True,
                    },
                )
                self.assertIsNotNone(dashboard)
                self.assertTrue(dashboard.get("html"))
                blueprint = dashboard.get("blueprint", {})
                layout_sections = blueprint.get("layout_sections", [])
                self.assertGreater(len(layout_sections), 0, f"{entry['path']} should yield blueprint sections")
                allowed_section_ids = set(section_options(entry["expected_kind"]).keys())
                self.assertTrue(
                    set(section["id"] for section in layout_sections).issubset(allowed_section_ids),
                    f"{entry['path']} produced unexpected section ids",
                )


if __name__ == "__main__":
    unittest.main()
