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
                profile_sections = section_options(entry["expected_kind"], analysis_payload["analysis"])
                included_sections = list(profile_sections.keys())[:4]
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
                self.assertEqual(analysis_payload["sections"], profile_sections)
                self.assertEqual([section["id"] for section in layout_sections], included_sections)
                allowed_section_ids = set(profile_sections.keys())
                self.assertTrue(
                    set(section["id"] for section in layout_sections).issubset(allowed_section_ids),
                    f"{entry['path']} produced unexpected section ids",
                )

    def test_generic_template_covers_files_no_specialized_template_claims(self) -> None:
        generic_fixtures = sorted(
            path
            for pattern in ("*.csv", "stress/generic/*.csv", "stress/*/ambiguous_schema.csv", "stress/*/partial_invalid.csv")
            for path in self.fixtures_root.glob(pattern)
        )
        self.assertGreater(len(generic_fixtures), 15)

        for path in generic_fixtures:
            with self.subTest(path=str(path.relative_to(self.fixtures_root))):
                context = run_pipeline(input_path=path, persist_outputs=False, include_visualizations=False)
                if context.raw_df is None:
                    continue  # empty and header-only files are rejected before any template runs
                self.assertIsNone(detect_business_context(context))

                generic = analyze_for_kind(context, "generic")
                self.assertIsNotNone(generic, "every readable file should get a generic dashboard")
                self.assertEqual(generic["default_sections"], list(generic["sections"]))
                # The analysis travels to the browser and back, so it must be plain JSON: no NaN, no numpy types.
                json.dumps(generic["analysis"], allow_nan=False)

                insights = build_insight_candidates("generic", generic["analysis"], "")["insights"]
                self.assertGreater(len(insights), 0)
                self.assertTrue(any(item["recommended"] for item in insights))

                dashboard = build_dashboard(
                    "generic",
                    generic["analysis"],
                    approved_insight_ids=[item["id"] for item in insights if item["recommended"]],
                    settings={"included_sections": generic["default_sections"], "show_notes": True},
                )
                section_ids = [section["id"] for section in dashboard["blueprint"]["layout_sections"]]
                self.assertEqual(section_ids, generic["default_sections"])
                self.assertGreaterEqual(len(section_ids), 3)
                self.assertTrue(dashboard["html"])


if __name__ == "__main__":
    unittest.main()
