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
from pipeline.business.common import parse_flag, prune_empty_blocks, rate_by_dimensions
from pipeline.business.financial_dashboard import SECTION_CONFIG
from pipeline.business.router import build_dashboard, build_insight_candidates, detect_business_context, explain_detection
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
                # Every required column is found under its restyled name; the generators omit two optional ones.
                self.assertEqual(detected["match"]["expected"] - detected["match"]["found"], 2)

    def test_financial_template_accepts_series_without_volume(self) -> None:
        frame = generate_financial_fixture(rows=120).drop(columns=["Volume"])
        detected = detect_business_context(PipelineContext(clean_df=frame, raw_df=frame))
        self.assertIsNotNone(detected)
        self.assertEqual(detected["kind"], "financial_timeseries")
        self.assertNotIn("volume", detected["sections"])

        review = build_insight_candidates("financial_timeseries", detected["analysis"], "")
        self.assertGreaterEqual(len(review["insights"]), 3)
        self.assertFalse([item["id"] for item in review["insights"] if item["category"] == "volume"])

        dashboard = build_dashboard(
            "financial_timeseries",
            detected["analysis"],
            approved_insight_ids=[item["id"] for item in review["insights"]],
            settings={"included_sections": list(SECTION_CONFIG), "show_notes": True},
        )
        section_ids = [section["id"] for section in dashboard["blueprint"]["layout_sections"]]
        self.assertNotIn("volume", section_ids)
        self.assertIn("gaps", section_ids)
        self.assertNotIn("highest-volume sessions", dashboard["html"])
        self.assertIn("Gap Up", dashboard["html"])

    def test_financial_template_accepts_ticker_prefixed_columns(self) -> None:
        frame = generate_financial_fixture(rows=120)
        prefixed = frame.rename(columns={name: f"AAPL.{name}" for name in frame.columns if name != "Date"})
        detected = detect_business_context(PipelineContext(clean_df=prefixed, raw_df=prefixed))
        self.assertIsNotNone(detected)
        self.assertEqual(detected["kind"], "financial_timeseries")
        self.assertIn("volume", detected["sections"])

        # A shared prefix is required: columns that merely end in "low" or "close" are not prices.
        unrelated = pd.DataFrame(
            {"date": pd.date_range("2024-01-01", periods=40).astype(str), "cashflow": range(40), "disclose": range(40), "reopen": range(40), "skyhigh": range(40)}
        )
        self.assertIsNone(detect_business_context(PipelineContext(clean_df=unrelated, raw_df=unrelated)))

    def test_explain_detection_ranks_the_closest_template_first(self) -> None:
        candidates = explain_detection(["Date", "Open", "High", "Close", "Notes"])
        self.assertEqual(candidates[0]["kind"], "financial_timeseries")
        self.assertEqual((candidates[0]["matched"], candidates[0]["required"]), (4, 5))
        self.assertEqual(candidates[0]["missing"], ["low"])
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
            "Date,Open,High,Close\n"
            + "\n".join(f"2024-01-{day:02d},{100 + day},{102 + day},{101 + day}" for day in range(1, 29))
        ).encode("utf-8")

    def _post(self, **data: str):
        return self.client.post("/api/analyze", files={"file": ("prices.csv", io.BytesIO(self.csv), "text/csv")}, data=data or None)

    def test_analyze_reports_how_close_each_template_came(self) -> None:
        response = self._post()
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["business_context"]["kind"], "generic")
        closest = payload["detection"]["candidates"][0]
        self.assertEqual((closest["kind"], closest["missing"]), ("financial_timeseries", ["low"]))

    def test_failed_override_says_what_is_missing(self) -> None:
        response = self._post(template_override="financial_timeseries")
        self.assertEqual(response.status_code, 400)
        self.assertIn("is missing: low", response.json()["detail"])


def workforce_frame(rows: int = 200) -> pd.DataFrame:
    """One row per employee with a leave flag, a department and tenure: no pay, ratings or engagement."""
    departments = ["Stores", "Head Office", "Warehouse", "Online"]
    records = []
    for index in range(rows):
        department = departments[index % len(departments)]
        tenure = (index * 7) % 15
        left = department == "Warehouse" and index % 3 != 0 or tenure < 2 and index % 2 == 0
        records.append(
            {
                "EmployeeID": f"E{index:04d}",
                "department_name": department,
                "length_of_service": tenure,
                "age": 22 + (index * 5) % 40,
                "STATUS": "TERMINATED" if left else "ACTIVE",
                "termreason_desc": "Resignation" if left else "Not Applicable",
            }
        )
    return pd.DataFrame(records)


class SharedHelperTests(unittest.TestCase):
    def test_parse_flag_reads_binary_columns_and_rejects_the_rest(self) -> None:
        self.assertEqual(parse_flag(pd.Series(["Yes", "no", "YES", None])).tolist()[:3], [1.0, 0.0, 1.0])
        self.assertEqual(parse_flag(pd.Series([0, 1, 1, 0])).tolist(), [0.0, 1.0, 1.0, 0.0])
        statuses = pd.Series(["ACTIVE", "TERMINATED", "Active"])
        self.assertEqual(parse_flag(statuses, positives={"terminated"}, negatives={"active"}).tolist(), [0.0, 1.0, 0.0])
        # Not flags: unknown vocabulary, more than two values, or only one class present.
        self.assertIsNone(parse_flag(pd.Series(["delivered", "shipped", "cancelled"] * 5)))
        self.assertIsNone(parse_flag(pd.Series([3, 7, 9, 1])))
        self.assertIsNone(parse_flag(pd.Series([0, 0, 0, 0])))

    def test_rate_by_dimensions_skips_small_groups_and_restated_outcomes(self) -> None:
        frame = workforce_frame()
        flag = parse_flag(frame["STATUS"], positives={"terminated"}, negatives={"active"})
        tables = rate_by_dimensions(frame, flag, ["department_name", "termreason_desc", "EmployeeID"])
        self.assertEqual([table["column"] for table in tables], ["department_name"])
        self.assertEqual(tables[0]["labels"][0], "Warehouse")
        self.assertGreater(tables[0]["spread"], 30)

    def test_prune_empty_blocks_drops_empty_charts_and_emptied_sections(self) -> None:
        def chart(labels: list[str], values: list[float]) -> dict:
            return {"id": "c", "kind": "chart", "chart": {"labels": labels, "series": [{"values": values}]}}

        sections = [
            {"id": "kept", "blocks": [chart(["a"], [1.0]), chart([], []), chart(["a", "b"], [0.0, 0.0])]},
            {"id": "emptied", "blocks": [chart([], []), {"id": "s", "kind": "stat_list", "items": []}]},
            {"id": "notes", "blocks": [{"id": "n", "kind": "note_list", "insights": []}]},
        ]
        pruned = prune_empty_blocks(sections)
        self.assertEqual([section["id"] for section in pruned], ["kept", "notes"])
        self.assertEqual(len(pruned[0]["blocks"]), 1)


class HrTemplateTests(unittest.TestCase):
    def _detect(self, frame: pd.DataFrame):
        return detect_business_context(PipelineContext(clean_df=frame, raw_df=frame))

    def test_leave_flag_with_department_and_tenure_is_enough(self) -> None:
        detected = self._detect(workforce_frame())
        self.assertIsNotNone(detected)
        self.assertEqual(detected["kind"], "hr_workforce")
        # No pay, ratings or training in the file, so those sections are not offered.
        self.assertEqual(list(detected["sections"]), ["overview", "retention", "workforce_model", "notes"])

        analysis = detected["analysis"]
        self.assertIsNone(analysis["summary"]["avg_salary"])
        charted = {table["column"] for table in analysis["signals"]["attrition_by_dimension"]}
        self.assertNotIn("termreason_desc", charted, "a termination reason restates the outcome")
        self.assertIn("Tenure band", charted)

        insights = {item["id"] for item in build_insight_candidates("hr_workforce", analysis, "")["insights"]}
        self.assertTrue({"attrition_hotspot", "overall_attrition"} <= insights)
        self.assertFalse({"high_performers_leave_too", "vp_gender_pay_gap", "training_is_controllable_lever"} & insights)

        dashboard = build_dashboard("hr_workforce", analysis, settings={"included_sections": list(detected["sections"])})
        self.assertEqual([section["id"] for section in dashboard["blueprint"]["layout_sections"]], list(detected["sections"]))

    def test_ratings_written_as_words_are_understood(self) -> None:
        frame = workforce_frame().assign(
            Attrition=lambda data: data["STATUS"].map({"TERMINATED": "Yes", "ACTIVE": "No"}),
            JobSatisfaction=lambda data: [["Low", "Medium", "High", "Very_High"][index % 4] for index in range(len(data))],
            PerformanceRating=lambda data: [["Excellent", "Outstanding"][index % 2] for index in range(len(data))],
        ).drop(columns=["STATUS", "termreason_desc"])
        analysis = self._detect(frame)["analysis"]
        self.assertEqual(analysis["signals"]["attrition_by_performance"]["labels"], ["High performer", "Everyone else"])
        self.assertAlmostEqual(analysis["summary"]["avg_engagement"], 6.25)

    def test_a_status_column_alone_does_not_make_a_workforce_file(self) -> None:
        orders = pd.DataFrame(
            {
                "order_id": range(120),
                "department": ["Toys", "Garden", "Home"] * 40,
                "status": ["delivered", "shipped", "cancelled", "returned"] * 30,
                "amount": [20 + index % 50 for index in range(120)],
            }
        )
        self.assertIsNone(self._detect(orders))

        # A real leave flag, but nothing that marks the rows as employees.
        subscribers = pd.DataFrame(
            {
                "customer_id": range(120),
                "plan": ["Basic", "Plus", "Pro"] * 40,
                "status": ["active", "terminated"] * 60,
                "monthly_fee": [10 + index % 30 for index in range(120)],
            }
        )
        self.assertIsNone(self._detect(subscribers))


if __name__ == "__main__":
    unittest.main()
