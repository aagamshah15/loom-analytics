from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from pipeline.business.ecommerce_dashboard import analyze_ecommerce_context, build_ecommerce_insight_candidates
from pipeline.business.financial_dashboard import analyze_financial_context, build_financial_insight_candidates
from pipeline.business.healthcare_dashboard import analyze_healthcare_context, build_healthcare_insight_candidates
from pipeline.business.hr_dashboard import analyze_hr_context, build_hr_insight_candidates
from pipeline.business.router import detect_business_context
from pipeline.common.contracts import PipelineContext


class StressFixtureTests(unittest.TestCase):
    fixtures_dir = PROJECT_ROOT / "tests" / "fixtures" / "stress"

    def _context(self, relative_path: str) -> PipelineContext:
        frame = pd.read_csv(self.fixtures_dir / relative_path)
        return PipelineContext(clean_df=frame, raw_df=frame)

    def test_happy_and_noisy_fixtures_detect_expected_templates(self) -> None:
        expectations = {
            "financial/happy_path.csv": "financial_timeseries",
            "financial/noisy_valid.csv": "financial_timeseries",
            "ecommerce/happy_path.csv": "ecommerce_orders",
            "ecommerce/noisy_valid.csv": "ecommerce_orders",
            "healthcare/happy_path.csv": "healthcare_medical",
            "healthcare/noisy_valid.csv": "healthcare_medical",
            "hr/happy_path.csv": "hr_workforce",
            "hr/noisy_valid.csv": "hr_workforce",
        }

        for relative_path, expected_kind in expectations.items():
            with self.subTest(relative_path=relative_path):
                detected = detect_business_context(self._context(relative_path))
                self.assertIsNotNone(detected)
                self.assertEqual(detected["kind"], expected_kind)

    def test_ambiguous_and_partial_fixtures_fall_back_to_generic(self) -> None:
        ambiguous_paths = [
            "financial/ambiguous_schema.csv",
            "financial/partial_invalid.csv",
            "ecommerce/ambiguous_schema.csv",
            "ecommerce/partial_invalid.csv",
            "healthcare/ambiguous_schema.csv",
            "healthcare/partial_invalid.csv",
            "hr/ambiguous_schema.csv",
            "hr/partial_invalid.csv",
            "generic/header_only.csv",
            "generic/duplicate_headers.csv",
            "generic/mixed_types.csv",
            "generic/long_text.csv",
            "generic/sparse_columns.csv",
        ]

        for relative_path in ambiguous_paths:
            with self.subTest(relative_path=relative_path):
                detected = detect_business_context(self._context(relative_path))
                self.assertIsNone(detected)

    def test_template_specific_insight_catalogs_contain_expected_findings(self) -> None:
        financial = build_financial_insight_candidates(analyze_financial_context(self._context("financial/happy_path.csv")), "")
        ecommerce = build_ecommerce_insight_candidates(analyze_ecommerce_context(self._context("ecommerce/happy_path.csv")), "")
        healthcare = build_healthcare_insight_candidates(analyze_healthcare_context(self._context("healthcare/happy_path.csv")), "")
        hr = build_hr_insight_candidates(analyze_hr_context(self._context("hr/happy_path.csv")), "")

        self.assertIn("long_term_growth", {item["id"] for item in financial["insights"]})
        self.assertIn("clothing_return_bomb", {item["id"] for item in ecommerce["insights"]})
        self.assertIn("telehealth_quietly_winning", {item["id"] for item in healthcare["insights"]})
        self.assertIn("vp_gender_pay_gap", {item["id"] for item in hr["insights"]})

    def test_prompt_reweighting_is_stable_across_specialized_templates(self) -> None:
        financial_analysis = analyze_financial_context(self._context("financial/happy_path.csv"))
        financial_baseline = build_financial_insight_candidates(financial_analysis, "")
        financial_prompted = build_financial_insight_candidates(financial_analysis, "focus on volatility and gaps")
        self.assertLess(
            [item["id"] for item in financial_prompted["insights"]].index("overnight_gap_bias"),
            [item["id"] for item in financial_baseline["insights"]].index("overnight_gap_bias"),
        )

        healthcare_analysis = analyze_healthcare_context(self._context("healthcare/happy_path.csv"))
        healthcare_baseline = build_healthcare_insight_candidates(healthcare_analysis, "")
        healthcare_prompted = build_healthcare_insight_candidates(healthcare_analysis, "focus on telehealth and adherence")
        self.assertLess(
            [item["id"] for item in healthcare_prompted["insights"]].index("follow_up_placebo"),
            [item["id"] for item in healthcare_baseline["insights"]].index("follow_up_placebo"),
        )

        hr_analysis = analyze_hr_context(self._context("hr/happy_path.csv"))
        hr_baseline = build_hr_insight_candidates(hr_analysis, "")
        hr_prompted = build_hr_insight_candidates(hr_analysis, "focus on pay equity and remote attrition")
        self.assertLess(
            [item["id"] for item in hr_prompted["insights"]].index("remote_retention_risk"),
            [item["id"] for item in hr_baseline["insights"]].index("remote_retention_risk"),
        )

    def test_generated_sparse_large_fixture_still_detects_expected_template(self) -> None:
        rows = []
        for idx in range(1, 1201):
            rows.append(
                {
                    "order_date": f"2024-02-{(idx % 28) + 1:02d}",
                    "order_value": 250 + (idx % 120),
                    "category": "Electronics" if idx % 3 else "Clothing",
                    "channel": "Direct" if idx % 2 else "Referral",
                    "payment_method": "BNPL" if idx % 5 else "Credit Card",
                    "device": "Desktop" if idx % 4 else "Mobile",
                    "discount_pct": idx % 20,
                    "returned": idx % 2,
                    "customer_id": f"c{idx % 90}",
                    "notes": None if idx % 7 else "occasional sparse text",
                }
            )

        context = PipelineContext(clean_df=pd.DataFrame(rows))
        detected = detect_business_context(context)
        self.assertIsNotNone(detected)
        self.assertEqual(detected["kind"], "ecommerce_orders")


if __name__ == "__main__":
    unittest.main()
