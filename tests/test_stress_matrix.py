from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
TESTS_ROOT = PROJECT_ROOT / "tests"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))

from pipeline.business.router import analyze_for_kind, build_insight_candidates, detect_business_context
from pipeline.common.contracts import PipelineContext
from pipeline.run import run_pipeline
from template_generators import (
    generate_ecommerce_fixture,
    generate_financial_fixture,
    generate_healthcare_admissions_fixture,
    generate_healthcare_insurance_fixture,
    generate_healthcare_outcomes_fixture,
    generate_hr_fixture,
    generate_large_fixture,
    generate_marketing_closed_deals_fixture,
    generate_marketing_crm_fixture,
    generate_marketing_lead_fixture,
    generate_survey_fixture,
    generate_web_analytics_fixture,
)


class StressFixtureTests(unittest.TestCase):
    fixtures_root = PROJECT_ROOT / "tests" / "fixtures"
    fixtures_dir = fixtures_root / "stress"
    manifest_path = fixtures_root / "template_manifest.json"

    @classmethod
    def setUpClass(cls) -> None:
        manifest = json.loads(cls.manifest_path.read_text())
        cls.positive_entries = [
            entry
            for template in manifest["templates"]
            for entry in template["fixtures"]
            if entry["fixture_class"] in {"happy_path", "noisy_valid"}
        ]

    def _context_from_fixture(self, relative_path: str) -> PipelineContext:
        return run_pipeline(
            input_path=self.fixtures_root / relative_path,
            persist_outputs=False,
            include_visualizations=False,
        )

    def test_happy_and_noisy_fixtures_detect_expected_templates(self) -> None:
        for entry in self.positive_entries:
            with self.subTest(relative_path=entry["path"]):
                detected = detect_business_context(self._context_from_fixture(entry["path"]))
                self.assertIsNotNone(detected)
                self.assertEqual(detected["kind"], entry["expected_kind"])

    def test_ambiguous_and_partial_fixtures_fall_back_to_generic(self) -> None:
        ambiguous_paths = [
            "stress/financial/ambiguous_schema.csv",
            "stress/financial/partial_invalid.csv",
            "stress/ecommerce/ambiguous_schema.csv",
            "stress/ecommerce/partial_invalid.csv",
            "stress/healthcare/ambiguous_schema.csv",
            "stress/healthcare/partial_invalid.csv",
            "stress/hr/ambiguous_schema.csv",
            "stress/hr/partial_invalid.csv",
            "stress/marketing/ambiguous_schema.csv",
            "stress/marketing/partial_invalid.csv",
            "stress/survey/ambiguous_schema.csv",
            "stress/survey/partial_invalid.csv",
            "stress/web_analytics/ambiguous_schema.csv",
            "stress/web_analytics/partial_invalid.csv",
            "stress/generic/header_only.csv",
            "stress/generic/duplicate_headers.csv",
            "stress/generic/mixed_types.csv",
            "stress/generic/long_text.csv",
            "stress/generic/sparse_columns.csv",
        ]

        for relative_path in ambiguous_paths:
            with self.subTest(relative_path=relative_path):
                detected = detect_business_context(self._context_from_fixture(relative_path))
                self.assertIsNone(detected)

    def test_prompt_reweighting_is_stable_across_specialized_templates(self) -> None:
        financial_analysis = analyze_for_kind(
            self._context_from_fixture("stress/financial/happy_path.csv"),
            "financial_timeseries",
        )["analysis"]
        financial_baseline = build_insight_candidates("financial_timeseries", financial_analysis, "")
        financial_prompted = build_insight_candidates("financial_timeseries", financial_analysis, "focus on volatility and gaps")
        self.assertLess(
            [item["id"] for item in financial_prompted["insights"]].index("overnight_gap_bias"),
            [item["id"] for item in financial_baseline["insights"]].index("overnight_gap_bias"),
        )

        healthcare_analysis = analyze_for_kind(
            self._context_from_fixture("stress/healthcare/happy_path.csv"),
            "healthcare_medical",
        )["analysis"]
        healthcare_baseline = build_insight_candidates("healthcare_medical", healthcare_analysis, "")
        healthcare_prompted = build_insight_candidates("healthcare_medical", healthcare_analysis, "focus on telehealth and adherence")
        self.assertLess(
            [item["id"] for item in healthcare_prompted["insights"]].index("follow_up_placebo"),
            [item["id"] for item in healthcare_baseline["insights"]].index("follow_up_placebo"),
        )

        hr_analysis = analyze_for_kind(
            self._context_from_fixture("stress/hr/happy_path.csv"),
            "hr_workforce",
        )["analysis"]
        hr_baseline = build_insight_candidates("hr_workforce", hr_analysis, "")
        hr_prompted = build_insight_candidates("hr_workforce", hr_analysis, "focus on pay equity and remote attrition")
        self.assertLess(
            [item["id"] for item in hr_prompted["insights"]].index("remote_retention_risk"),
            [item["id"] for item in hr_baseline["insights"]].index("remote_retention_risk"),
        )

    def test_generated_profile_variants_detect_expected_specialized_templates(self) -> None:
        scenarios = [
            (generate_financial_fixture(), "financial_timeseries", None),
            (generate_ecommerce_fixture(), "ecommerce_orders", None),
            (generate_healthcare_outcomes_fixture(), "healthcare_medical", "outcomes"),
            (generate_healthcare_admissions_fixture(), "healthcare_medical", "admissions"),
            (generate_healthcare_insurance_fixture(), "healthcare_medical", "insurance_risk"),
            (generate_hr_fixture(), "hr_workforce", None),
            (generate_marketing_crm_fixture(), "marketing_campaign", "crm"),
            (generate_marketing_lead_fixture(), "marketing_campaign", "lead_generation"),
            (generate_marketing_closed_deals_fixture(), "marketing_campaign", "closed_deals"),
            (generate_survey_fixture(), "survey_sentiment", None),
            (generate_web_analytics_fixture(), "web_app_analytics", None),
        ]

        for frame, expected_kind, expected_profile in scenarios:
            with self.subTest(kind=expected_kind, expected_profile=expected_profile):
                context = PipelineContext(clean_df=frame, raw_df=frame)
                detected = detect_business_context(context)
                self.assertIsNotNone(detected)
                self.assertEqual(detected["kind"], expected_kind)
                if expected_profile is not None:
                    self.assertEqual(detected["analysis"].get("profile"), expected_profile)

    def test_generated_large_fixtures_cover_every_specialized_template(self) -> None:
        for kind in [
            "financial_timeseries",
            "ecommerce_orders",
            "healthcare_medical",
            "hr_workforce",
            "marketing_campaign",
            "survey_sentiment",
            "web_app_analytics",
        ]:
            with self.subTest(kind=kind):
                frame = generate_large_fixture(kind, rows=512)
                context = PipelineContext(clean_df=frame, raw_df=frame)
                detected = detect_business_context(context)
                self.assertIsNotNone(detected)
                self.assertEqual(detected["kind"], kind)


if __name__ == "__main__":
    unittest.main()
