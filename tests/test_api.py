from __future__ import annotations

import io
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

try:
    from fastapi.testclient import TestClient

    from pipeline.api.app import app
    from pipeline.business.router import template_catalog

    HAS_FASTAPI = True
except ModuleNotFoundError:
    TestClient = None
    app = None
    template_catalog = None
    HAS_FASTAPI = False


@unittest.skipUnless(HAS_FASTAPI, "fastapi is not installed in the current Python environment")
class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    def test_templates_endpoint_returns_catalog(self) -> None:
        response = self.client.get("/api/templates")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.json()["templates"]), len(template_catalog()))

    def test_analyze_and_build_dashboard_for_ecommerce_csv(self) -> None:
        csv_data = """order_date,order_value,category,channel,payment_method,device,discount_pct,returned,customer_id
2024-01-05,370,Electronics,Direct,BNPL,Desktop,0,0,c1
2024-01-12,335,Clothing,Organic,PayPal,Mobile,10,1,c2
2024-01-19,332,Clothing,Organic,PayPal,Mobile,25,1,c2
2024-02-02,368,Home & Kitchen,Referral,Credit Card,Desktop,0,0,c3
2024-02-09,375,Electronics,Direct,BNPL,Desktop,0,0,c1
2024-02-16,360,Clothing,Paid Search,Credit Card,Mobile,10,1,c4
2024-03-01,355,Beauty,Referral,Apple Pay,Tablet,0,0,c5
2024-03-08,340,Books,Organic,Debit Card,Desktop,0,0,c6
2024-03-15,372,Electronics,Direct,BNPL,Desktop,0,0,c1
2024-03-22,378,Clothing,Referral,PayPal,Mobile,10,1,c2
2024-03-29,365,Electronics,Direct,Debit Card,Desktop,0,0,c7
2024-04-05,358,Sports,Email,Apple Pay,Tablet,10,0,c8
2024-04-12,349,Home & Kitchen,Social,Credit Card,Mobile,0,0,c9
2024-04-19,320,Clothing,Organic,PayPal,Mobile,25,1,c4
2024-04-26,330,Clothing,Paid Search,Credit Card,Mobile,25,1,c4
2024-05-03,300,Books,Organic,Debit Card,Desktop,0,0,c10
2024-05-10,295,Beauty,Referral,Apple Pay,Tablet,10,0,c5
2024-05-17,344,Clothing,Direct,BNPL,Desktop,0,0,c1
2024-05-24,390,Electronics,Social,Credit Card,Mobile,0,0,c11
2024-05-31,410,Home & Kitchen,Email,BNPL,Desktop,0,0,c12
"""
        analyze_response = self.client.post(
            "/api/analyze",
            files={"file": ("orders.csv", io.BytesIO(csv_data.encode("utf-8")), "text/csv")},
        )
        self.assertEqual(analyze_response.status_code, 200)
        analyze_payload = analyze_response.json()
        self.assertEqual(analyze_payload["business_context"]["kind"], "ecommerce_orders")
        approved_ids = [item["id"] for item in analyze_payload["review"]["insights"][:3]]

        build_response = self.client.post(
            "/api/build-dashboard",
            json={
                "kind": analyze_payload["business_context"]["kind"],
                "analysis": analyze_payload["business_context"]["analysis"],
                "approved_insight_ids": approved_ids,
                "settings": {
                    "title": "Commerce Preview",
                    "subtitle": "Approved narrative",
                    "included_sections": ["overview", "revenue", "returns", "channels", "discounts", "notes"],
                    "metric_count": 3,
                    "show_notes": True,
                },
            },
        )
        self.assertEqual(build_response.status_code, 200)
        payload = build_response.json()
        self.assertIn("blueprint", payload)
        self.assertEqual(payload["blueprint"]["title"], "Commerce Preview")
        self.assertGreater(len(payload["blueprint"]["layout_sections"]), 0)
        self.assertIn("html", payload)


if __name__ == "__main__":
    unittest.main()
