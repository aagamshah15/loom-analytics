from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from pipeline.business.outcome_profile import context_support
from pipeline.business.router import build_dashboard, build_insight_candidates, detect_business_context
from pipeline.common.contracts import PipelineContext

# Shapes taken from widely used public datasets that the original templates did not recognize.
# Values are synthetic; column names and value formats follow the real files.


def detect(frame: pd.DataFrame):
    return detect_business_context(PipelineContext(clean_df=frame, raw_df=frame))


def build(detected: dict) -> tuple[dict, dict]:
    review = build_insight_candidates(detected["kind"], detected["analysis"], "")
    dashboard = build_dashboard(
        detected["kind"],
        detected["analysis"],
        approved_insight_ids=[item["id"] for item in review["insights"] if item["recommended"]],
        settings={"included_sections": detected["default_sections"] or list(detected["sections"]), "show_notes": True},
    )
    return review, dashboard


def hospital_encounters(rows: int = 400) -> pd.DataFrame:
    specialties = ["Cardiology", "InternalMedicine", "Surgery", "Emergency"]
    records = []
    for index in range(rows):
        specialty = specialties[index % 4]
        inpatient_visits = index % 5
        readmitted = specialty == "Cardiology" and index % 3 != 0 or inpatient_visits >= 3 and index % 2 == 0
        records.append(
            {
                "encounter_id": 10_000 + index,
                "patient_nbr": 500 + index % 180,
                "age": ["[40-50)", "[50-60)", "[60-70)", "[70-80)"][(index // 4) % 4],
                "time_in_hospital": 1 + index % 9,
                "num_medications": 5 + index % 17,
                "number_inpatient": inpatient_visits,
                "medical_specialty": specialty,
                "diag_1": str(250 + index % 30),
                "readmitted": ("<30" if index % 2 else ">30") if readmitted else "NO",
            }
        )
    return pd.DataFrame(records)


def ab_test(rows: int = 600) -> pd.DataFrame:
    records = []
    for index in range(rows):
        group = "ad" if index % 5 else "psa"
        converted = group == "ad" and index % 9 == 0 or index % 53 == 0
        records.append(
            {
                "user id": 900_000 + index,
                "test group": group,
                "converted": converted,
                "total ads": 5 + (index * 7) % 90 + (40 if converted else 0),
                "most ads day": ["Monday", "Tuesday", "Friday", "Saturday"][index % 4],
            }
        )
    return pd.DataFrame(records)


def web_sessions(rows: int = 500) -> pd.DataFrame:
    records = []
    for index in range(rows):
        returning = index % 3 == 0
        purchased = returning and index % 4 == 0 or index % 37 == 0
        records.append(
            {
                "ProductRelated": 3 + index % 40,
                "BounceRates": 0.0 if purchased else (index % 10) / 100,
                "ExitRates": 0.01 if purchased else 0.02 + (index % 8) / 100,
                "PageValues": 25.0 + index % 30 if purchased else float(index % 3),
                "Month": ["Feb", "Mar", "May", "Nov"][index % 4],
                "VisitorType": "Returning_Visitor" if returning else "New_Visitor",
                "Weekend": index % 7 in (5, 6),
                "Revenue": purchased,
            }
        )
    return pd.DataFrame(records)


def superstore_orders(rows: int = 360) -> pd.DataFrame:
    categories = ["Furniture", "Office Supplies", "Technology"]
    records = []
    for index in range(rows):
        discount = [0.0, 0.0, 0.2, 0.4][index % 4]
        sales = 80.0 + (index * 37) % 900 + (index // 30) * 12
        records.append(
            {
                "Order ID": f"CA-{2014 + index // 120}-{100000 + index // 2}",
                "Order Date": f"{1 + (index // 10) % 12}/{1 + index % 27}/{2014 + index // 120}",
                "Ship Mode": ["Standard Class", "Second Class", "First Class"][index % 3],
                "Customer ID": f"CU-{index % 60:03d}",
                "Segment": ["Consumer", "Corporate", "Home Office"][index % 3],
                "Region": ["West", "East", "Central", "South"][index % 4],
                "Category": categories[0] if index % 5 == 0 else categories[index % 3],
                "Product Name": f"Item {index % 45}",
                "Sales": sales,
                "Quantity": 1 + index % 6,
                "Discount": discount,
                "Profit": sales * (0.28 - discount),
            }
        )
    return pd.DataFrame(records)


def invoice_lines(rows: int = 400) -> pd.DataFrame:
    records = []
    for index in range(rows):
        records.append(
            {
                "InvoiceNo": f"{'C' if index % 25 == 0 else ''}{536000 + index // 4}",
                "StockCode": f"{22000 + index % 70}",
                "Description": f"PRODUCT {index % 70}",
                "Quantity": -2 if index % 25 == 0 else 1 + index % 12,
                "InvoiceDate": f"{1 + (index // 34) % 12}/{1 + index % 27}/2011 {8 + index % 9}:26",
                "UnitPrice": 1.25 + (index % 9) * 0.85,
                "CustomerID": 12000 + index % 85,
                "Country": "United Kingdom" if index % 6 else ["France", "Germany", "EIRE"][index % 3],
            }
        )
    return pd.DataFrame(records)


class OutcomeProfileTests(unittest.TestCase):
    def test_hospital_encounters_match_healthcare_on_the_readmission_flag(self) -> None:
        detected = detect(hospital_encounters())
        self.assertEqual((detected["kind"], detected["analysis"]["profile"]), ("healthcare_medical", "encounters"))
        self.assertEqual(detected["analysis"]["outcome"]["rate_label"], "Readmission rate")
        self.assertEqual(list(detected["sections"]), ["overview", "drivers", "measures", "notes"])

        review, dashboard = build(detected)
        hotspot = next(item for item in review["insights"] if item["id"] == "outcome_hotspot")
        self.assertIn("medical_specialty is Cardiology", hotspot["title"])
        self.assertIn("measure_gap", {item["id"] for item in review["insights"]})
        self.assertEqual([section["id"] for section in dashboard["blueprint"]["layout_sections"]], list(detected["sections"]))
        self.assertIn("Readmission rate by medical_specialty", dashboard["html"])

    def test_ab_test_matches_marketing_and_reports_the_lift(self) -> None:
        detected = detect(ab_test())
        self.assertEqual((detected["kind"], detected["analysis"]["profile"]), ("marketing_campaign", "conversion"))
        review, dashboard = build(detected)
        charted = [table["column"] for table in detected["analysis"]["signals"]["rate_by_dimension"]]
        self.assertIn("test group", charted)
        self.assertGreaterEqual(len(review["insights"]), 3)
        self.assertTrue(dashboard["html"])

    def test_session_export_matches_web_analytics_on_the_purchase_flag(self) -> None:
        detected = detect(web_sessions())
        self.assertEqual((detected["kind"], detected["analysis"]["profile"]), ("web_app_analytics", "sessions"))
        review, dashboard = build(detected)
        self.assertEqual(review["insights"][0]["id"], "outcome_hotspot")
        self.assertGreaterEqual(len(dashboard["blueprint"]["layout_sections"]), 3)

    def test_an_outcome_column_needs_its_domain_around_it(self) -> None:
        # The same flags, with nothing else that marks the rows as patients, campaigns or sessions.
        plain = pd.DataFrame(
            {
                "batch": [f"B{index % 12}" for index in range(200)],
                "plant": ["North", "South"] * 100,
                "weight": [10 + index % 7 for index in range(200)],
                "readmitted": ["NO", "<30"] * 100,
                "converted": [True, False, False, False] * 50,
                "Revenue": [True, False] * 100,
            }
        )
        self.assertIsNone(detect(plain))

    def test_context_support_counts_whole_tokens_and_long_substrings(self) -> None:
        columns = ["address", "total ads", "PatientId", "reward", "Ward_Type", "n_lab_procedures"]
        self.assertEqual(context_support(columns, ["ad", "ads"]), 1)
        self.assertEqual(context_support(columns, ["patient", "ward", "lab"]), 3)
        self.assertEqual(context_support(columns, ["patient"], exclude="PatientId"), 0)


class EcommerceShapeTests(unittest.TestCase):
    def test_order_export_without_channel_payment_or_device_columns(self) -> None:
        detected = detect(superstore_orders())
        self.assertEqual(detected["kind"], "ecommerce_orders")
        self.assertTrue(detected["analysis"]["dataset"]["partial"])
        self.assertNotIn("returns", detected["sections"])

        review, dashboard = build(detected)
        insights = {item["id"] for item in review["insights"]}
        self.assertTrue({"discount_margin_cost", "revenue_concentration", "sales_overview"} <= insights)
        self.assertFalse({"payment_myth", "channel_winner", "clothing_return_bomb"} & insights)
        self.assertEqual(detected["analysis"]["summary"]["order_count"], 180)
        self.assertIn("Revenue by category", dashboard["html"])

    def test_invoice_lines_derive_the_amount_and_returns(self) -> None:
        detected = detect(invoice_lines())
        self.assertEqual(detected["kind"], "ecommerce_orders")
        summary = detected["analysis"]["summary"]
        self.assertEqual(summary["order_count"], 116)  # 100 invoices, 16 of them with a cancellation line
        self.assertAlmostEqual(summary["return_rate"], 4.0)
        frame = invoice_lines()
        self.assertAlmostEqual(summary["total_revenue"], float((frame["Quantity"] * frame["UnitPrice"]).sum()))
        self.assertEqual(summary["top_revenue_group"]["label"], "United Kingdom")

    def test_a_date_and_an_amount_alone_are_not_sales(self) -> None:
        ledger = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=120).astype(str),
                "amount": [100 + index % 40 for index in range(120)],
                "account": ["Rent", "Utilities", "Payroll"] * 40,
                "memo": [f"entry {index}" for index in range(120)],
            }
        )
        self.assertIsNone(detect(ledger))


if __name__ == "__main__":
    unittest.main()
