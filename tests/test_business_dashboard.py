from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

try:
    from fastapi.encoders import jsonable_encoder
except ModuleNotFoundError:
    def jsonable_encoder(value):  # type: ignore[no-redef]
        if isinstance(value, dict):
            return {key: jsonable_encoder(item) for key, item in value.items()}
        if isinstance(value, list):
            return [jsonable_encoder(item) for item in value]
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        return value

from pipeline.business.financial_dashboard import (
    analyze_financial_context,
    build_business_dashboard,
    build_financial_insight_candidates,
    extract_focus_tags,
)
from pipeline.business.ecommerce_dashboard import (
    analyze_ecommerce_context,
    build_business_dashboard as build_ecommerce_dashboard,
    build_ecommerce_insight_candidates,
)
from pipeline.business.router import build_dashboard, detect_business_context
from pipeline.common.contracts import PipelineContext


class BusinessDashboardTests(unittest.TestCase):
    def test_build_business_dashboard_for_stock_data(self) -> None:
        df = pd.DataFrame(
            {
                "Date": pd.to_datetime(
                    [
                        "2020-01-02",
                        "2020-06-15",
                        "2020-12-31",
                        "2021-01-04",
                        "2021-06-15",
                        "2021-12-31",
                        "2022-01-03",
                        "2022-06-15",
                        "2022-12-30",
                        "2023-01-03",
                        "2023-06-15",
                        "2023-12-29",
                    ]
                ),
                "Open": [100, 103, 110, 112, 118, 121, 117, 98, 94, 96, 112, 130],
                "High": [103, 107, 113, 115, 122, 125, 119, 102, 97, 101, 116, 135],
                "Low": [98, 101, 108, 109, 116, 119, 95, 94, 90, 94, 108, 127],
                "Close": [102, 106, 112, 114, 120, 123, 98, 96, 93, 100, 115, 132],
                "Volume": [
                    12000000,
                    14000000,
                    16000000,
                    15000000,
                    17500000,
                    18000000,
                    32000000,
                    28000000,
                    26000000,
                    19000000,
                    21000000,
                    23000000,
                ],
            }
        )
        context = PipelineContext(clean_df=df)
        analysis = analyze_financial_context(context)
        self.assertIsNotNone(analysis)

        insight_bundle = build_financial_insight_candidates(analysis, "focus on volatility and gaps")
        self.assertIn("volatility", insight_bundle["focus_tags"])
        self.assertIn("gaps", insight_bundle["focus_tags"])
        self.assertGreater(len(insight_bundle["insights"]), 0)

        approved_ids = [item["id"] for item in insight_bundle["insights"][:3]]
        dashboard = build_business_dashboard(
            analysis=analysis,
            approved_insight_ids=approved_ids,
            user_prompt="focus on volatility and gaps",
            settings={
                "title": "AXP Stress Dashboard",
                "subtitle": "Approved insight blueprint",
                "included_sections": ["overview", "volatility", "gaps", "data_notes"],
                "metric_count": 3,
                "show_notes": True,
            },
        )

        self.assertIsNotNone(dashboard)
        self.assertEqual(dashboard["kind"], "financial_timeseries")
        self.assertIn("AXP Stress Dashboard", dashboard["html"])
        self.assertIn("gapChart", dashboard["html"])
        self.assertEqual(len(dashboard["payload"]["metric_cards"]), 3)
        self.assertIn("blueprint", dashboard)
        self.assertGreater(len(dashboard["blueprint"]["layout_sections"]), 0)
        self.assertEqual(dashboard["blueprint"]["layout_sections"][0]["id"], "overview")

        encoded_dashboard = build_business_dashboard(
            analysis=jsonable_encoder(analysis),
            approved_insight_ids=approved_ids,
            user_prompt="focus on volatility and gaps",
            settings={
                "title": "AXP Stress Dashboard",
                "subtitle": "Approved insight blueprint",
                "included_sections": ["overview", "volatility", "gaps", "data_notes"],
                "metric_count": 3,
                "show_notes": True,
            },
        )
        self.assertIsNotNone(encoded_dashboard)
        self.assertIn("blueprint", encoded_dashboard)

    def test_extract_focus_tags_is_deterministic(self) -> None:
        tags = extract_focus_tags("Look at dividend behavior, volume spikes, and long-term growth.")
        self.assertEqual(tags, ["growth", "volume", "dividends"])

    def test_build_ecommerce_dashboard_for_order_data(self) -> None:
        df = pd.DataFrame(
            {
                "order_date": pd.to_datetime(
                    [
                        "2023-01-06",
                        "2023-01-07",
                        "2023-01-08",
                        "2023-02-03",
                        "2023-02-10",
                        "2023-03-17",
                        "2023-04-21",
                        "2023-05-05",
                        "2024-01-05",
                        "2024-02-09",
                        "2024-03-15",
                        "2024-04-19",
                        "2024-05-24",
                        "2024-06-28",
                        "2024-07-12",
                        "2024-08-16",
                        "2024-09-20",
                        "2024-10-25",
                        "2024-11-29",
                        "2024-12-27",
                    ]
                ),
                "order_value": [370, 335, 332, 368, 375, 360, 355, 340, 372, 378, 365, 358, 349, 320, 330, 300, 295, 344, 390, 410],
                "category": [
                    "Electronics", "Clothing", "Clothing", "Home & Kitchen", "Electronics",
                    "Clothing", "Beauty", "Books", "Direct", "Clothing",
                    "Electronics", "Sports", "Home & Kitchen", "Clothing", "Clothing",
                    "Books", "Beauty", "Clothing", "Electronics", "Home & Kitchen",
                ],
                "channel": [
                    "Direct", "Organic", "Organic", "Referral", "Direct",
                    "Paid Search", "Referral", "Organic", "Direct", "Referral",
                    "Direct", "Email", "Social", "Organic", "Paid Search",
                    "Organic", "Referral", "Direct", "Social", "Email",
                ],
                "payment_method": [
                    "BNPL", "PayPal", "PayPal", "Credit Card", "BNPL",
                    "Credit Card", "Apple Pay", "Debit Card", "BNPL", "PayPal",
                    "Debit Card", "Apple Pay", "Credit Card", "PayPal", "Credit Card",
                    "Debit Card", "Apple Pay", "BNPL", "Credit Card", "BNPL",
                ],
                "device": [
                    "Desktop", "Mobile", "Mobile", "Desktop", "Desktop",
                    "Mobile", "Tablet", "Desktop", "Desktop", "Mobile",
                    "Desktop", "Tablet", "Mobile", "Mobile", "Mobile",
                    "Desktop", "Tablet", "Desktop", "Mobile", "Desktop",
                ],
                "discount_pct": [0, 10, 25, 0, 0, 10, 0, 0, 0, 10, 0, 10, 0, 25, 25, 0, 10, 0, 0, 0],
                "returned": [0, 1, 1, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0],
                "customer_id": [
                    "c1", "c2", "c2", "c3", "c1", "c4", "c5", "c6", "c1", "c2",
                    "c7", "c8", "c9", "c4", "c4", "c10", "c5", "c1", "c11", "c12",
                ],
            }
        )
        context = PipelineContext(clean_df=df)

        detected = detect_business_context(context)
        self.assertIsNotNone(detected)
        self.assertEqual(detected["kind"], "ecommerce_orders")

        analysis = analyze_ecommerce_context(context)
        self.assertIsNotNone(analysis)
        insight_bundle = build_ecommerce_insight_candidates(analysis, "focus on discounts and returns")
        self.assertIn("discounts", insight_bundle["focus_tags"])
        self.assertIn("returns", insight_bundle["focus_tags"])
        self.assertGreater(len(insight_bundle["insights"]), 0)

        approved_ids = [item["id"] for item in insight_bundle["insights"][:4]]
        dashboard = build_ecommerce_dashboard(
            analysis=analysis,
            approved_insight_ids=approved_ids,
            user_prompt="focus on discounts and returns",
            settings={
                "title": "Commerce Margin Dashboard",
                "subtitle": "Approved e-commerce insights",
                "included_sections": ["overview", "revenue", "returns", "discounts", "notes"],
                "metric_count": 4,
                "show_notes": True,
            },
        )

        self.assertIsNotNone(dashboard)
        self.assertEqual(dashboard["kind"], "ecommerce_orders")
        self.assertIn("Commerce Margin Dashboard", dashboard["html"])
        self.assertIn("discountChart", dashboard["html"])
        self.assertIn("blueprint", dashboard)
        self.assertGreater(len(dashboard["blueprint"]["layout_sections"]), 0)

        routed_dashboard = build_dashboard(
            kind="ecommerce_orders",
            analysis=analysis,
            approved_insight_ids=approved_ids,
            settings={"included_sections": ["overview", "returns"]},
        )
        self.assertIsNotNone(routed_dashboard)
        self.assertEqual(routed_dashboard["kind"], "ecommerce_orders")
        self.assertIn("blueprint", routed_dashboard)


if __name__ == "__main__":
    unittest.main()
