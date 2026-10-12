from __future__ import annotations

import io
import sys
import unittest
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
TESTS_ROOT = PROJECT_ROOT / "tests"
for path in (SRC_ROOT, TESTS_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from pipeline.business.matching import detect_columns, match_aliases, normalize_name
from pipeline.business.router import detect_business_context, explain_detection
from pipeline.common.contracts import PipelineContext
from template_generators import generate_ecommerce_fixture, generate_financial_fixture

try:
    from fastapi.testclient import TestClient

    from pipeline.api.app import app

    HAS_FASTAPI = True
except ModuleNotFoundError:
    TestClient = None
    app = None
    HAS_FASTAPI = False


class NameMatchingTests(unittest.TestCase):
    def test_names_match_regardless_of_case_spacing_and_punctuation(self) -> None:
        for spelling in ("Order Date", "order_date", "OrderDate", " ORDER-DATE ", "order.date"):
            with self.subTest(spelling=spelling):
                self.assertEqual(normalize_name(spelling), "orderdate")
        self.assertEqual(normalize_name("Close/Last"), normalize_name("close last"))
        self.assertNotEqual(normalize_name("order_date"), normalize_name("order_id"))

    def test_first_listed_alias_wins_and_missing_fields_are_named(self) -> None:
        aliases = {"revenue": ["order_value", "sales", "amount"], "channel": ["channel", "source"]}
        match = match_aliases(["Amount", "Sales", "Region"], aliases)
        self.assertEqual(match.matched, {"revenue": "Sales"})
        self.assertEqual(match.missing, ["channel"])

    def test_detect_columns_needs_every_required_field(self) -> None:
        frame = pd.DataFrame({"Order Date": ["2024-01-01"], "Sales": [10], "Customer ID": ["c1"]})
        required = {"date": ["order_date"], "revenue": ["sales"]}
        detected = detect_columns(frame, required, {"customer": ["customer_id"], "segment": ["segment"]})
        self.assertEqual(detected, {"required": {"date": "Order Date", "revenue": "Sales"}, "optional": {"customer": "Customer ID"}})
        self.assertIsNone(detect_columns(frame, {**required, "channel": ["channel"]}))


class DetectionTests(unittest.TestCase):
    def test_templates_detect_reformatted_headers(self) -> None:
        # The same data a tool might export with title-cased, spaced, or camel-cased headers.
        cases = [
            (generate_ecommerce_fixture(rows=60), "ecommerce_orders", lambda name: name.replace("_", " ").title()),
            (generate_ecommerce_fixture(rows=60), "ecommerce_orders", lambda name: name.title().replace("_", "")),
            (generate_financial_fixture(rows=60), "financial_timeseries", lambda name: f" {name.upper()} "),
        ]
        for frame, expected_kind, restyle in cases:
            restyled = frame.rename(columns=restyle)
            with self.subTest(columns=list(restyled.columns)[:3]):
                detected = detect_business_context(PipelineContext(clean_df=restyled, raw_df=restyled))
                self.assertIsNotNone(detected)
                self.assertEqual(detected["kind"], expected_kind)
                self.assertEqual(detected["match"]["found"], detected["match"]["expected"] - 2 if expected_kind == "ecommerce_orders" else 6)

    def test_explain_detection_ranks_the_closest_template_first(self) -> None:
        candidates = explain_detection(["Date", "Open", "High", "Low", "Close", "Notes"])
        self.assertEqual(candidates[0]["kind"], "financial_timeseries")
        self.assertEqual((candidates[0]["matched"], candidates[0]["required"]), (5, 6))
        self.assertEqual(candidates[0]["missing"], ["volume"])
        self.assertEqual(len(candidates), 7)

        # Multi-shape templates report whichever accepted shape is closest.
        insurance = explain_detection(["age", "sex", "bmi", "children", "smoker", "region"])
        healthcare = next(item for item in insurance if item["kind"] == "healthcare_medical")
        self.assertEqual(healthcare["profile"], "insurance_risk")
        self.assertEqual(healthcare["missing"], ["charges"])


@unittest.skipUnless(HAS_FASTAPI, "fastapi is not installed in the current Python environment")
class DetectionApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)
        self.csv = (
            "Date,Open,High,Low,Close\n"
            + "\n".join(f"2024-01-{day:02d},{100 + day},{102 + day},{99 + day},{101 + day}" for day in range(1, 29))
        ).encode("utf-8")

    def _post(self, **data: str):
        return self.client.post("/api/analyze", files={"file": ("prices.csv", io.BytesIO(self.csv), "text/csv")}, data=data or None)

    def test_analyze_reports_how_close_each_template_came(self) -> None:
        response = self._post()
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["business_context"]["kind"], "generic")
        closest = payload["detection"]["candidates"][0]
        self.assertEqual((closest["kind"], closest["missing"]), ("financial_timeseries", ["volume"]))

    def test_failed_override_says_what_is_missing(self) -> None:
        response = self._post(template_override="financial_timeseries")
        self.assertEqual(response.status_code, 400)
        self.assertIn("is missing: volume", response.json()["detail"])


if __name__ == "__main__":
    unittest.main()
