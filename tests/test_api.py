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
    stress_fixtures = PROJECT_ROOT / "tests" / "fixtures" / "stress"

    def setUp(self) -> None:
        self.client = TestClient(app)

    def _post_fixture(self, relative_path: str, *, template_override: str | None = None):
        fixture_path = self.stress_fixtures / relative_path
        with fixture_path.open("rb") as handle:
            files = {"file": (fixture_path.name, io.BytesIO(handle.read()), "text/csv")}
            data = {"template_override": template_override} if template_override else None
            return self.client.post("/api/analyze", files=files, data=data)

    def test_templates_endpoint_returns_catalog(self) -> None:
        response = self.client.get("/api/templates")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.json()["templates"]), len(template_catalog()))

    def test_analyze_rejects_non_csv_upload(self) -> None:
        response = self.client.post(
            "/api/analyze",
            files={"file": ("notes.txt", io.BytesIO(b"hello"), "text/plain")},
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("CSV file", response.json()["detail"])

    def test_analyze_accepts_utf8_bom_fixture(self) -> None:
        csv_data = (
            "\ufefforder_date,order_value,category,channel,payment_method,device,discount_pct,returned,customer_id\n"
            "2024-01-05,370,Electronics,Direct,BNPL,Desktop,0,0,c1\n"
            "2024-01-12,335,Clothing,Organic,PayPal,Mobile,10,1,c2\n"
            "2024-01-19,332,Clothing,Organic,PayPal,Mobile,25,1,c2\n"
            "2024-02-02,368,Home & Kitchen,Referral,Credit Card,Desktop,0,0,c3\n"
            "2024-02-09,375,Electronics,Direct,BNPL,Desktop,0,0,c1\n"
            "2024-02-16,360,Clothing,Paid Search,Credit Card,Mobile,10,1,c4\n"
            "2024-03-01,355,Beauty,Referral,Apple Pay,Tablet,0,0,c5\n"
            "2024-03-08,340,Books,Organic,Debit Card,Desktop,0,0,c6\n"
            "2024-03-15,372,Electronics,Direct,BNPL,Desktop,0,0,c1\n"
            "2024-03-22,378,Clothing,Referral,PayPal,Mobile,10,1,c2\n"
            "2024-03-29,365,Electronics,Direct,Debit Card,Desktop,0,0,c7\n"
            "2024-04-05,358,Sports,Email,Apple Pay,Tablet,10,0,c8\n"
            "2024-04-12,349,Home & Kitchen,Social,Credit Card,Mobile,0,0,c9\n"
            "2024-04-19,320,Clothing,Organic,PayPal,Mobile,25,1,c4\n"
            "2024-04-26,330,Clothing,Paid Search,Credit Card,Mobile,25,1,c4\n"
            "2024-05-03,300,Books,Organic,Debit Card,Desktop,0,0,c10\n"
            "2024-05-10,295,Beauty,Referral,Apple Pay,Tablet,10,0,c5\n"
            "2024-05-17,344,Clothing,Direct,BNPL,Desktop,0,0,c1\n"
            "2024-05-24,390,Electronics,Social,Credit Card,Mobile,0,0,c11\n"
            "2024-05-31,410,Home & Kitchen,Email,BNPL,Desktop,0,0,c12\n"
        )
        response = self.client.post(
            "/api/analyze",
            files={"file": ("orders.csv", io.BytesIO(csv_data.encode("utf-8")), "text/csv")},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["business_context"]["kind"], "ecommerce_orders")

    def test_analyze_and_build_dashboard_for_ecommerce_csv(self) -> None:
        analyze_response = self._post_fixture("ecommerce/happy_path.csv")
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

    def test_template_override_success_and_failure_paths(self) -> None:
        success = self._post_fixture("ecommerce/happy_path.csv", template_override="ecommerce_orders")
        self.assertEqual(success.status_code, 200)
        self.assertEqual(success.json()["business_context"]["kind"], "ecommerce_orders")

        failure = self._post_fixture("ecommerce/happy_path.csv", template_override="financial_timeseries")
        self.assertEqual(failure.status_code, 400)
        self.assertIn("did not match", failure.json()["detail"])

    def test_review_endpoint_reweights_relevant_insights(self) -> None:
        analyze_response = self._post_fixture("ecommerce/happy_path.csv")
        payload = analyze_response.json()

        baseline = self.client.post(
            "/api/review",
            json={
                "kind": payload["business_context"]["kind"],
                "analysis": payload["business_context"]["analysis"],
                "user_prompt": "",
            },
        )
        self.assertEqual(baseline.status_code, 200)

        prompted = self.client.post(
            "/api/review",
            json={
                "kind": payload["business_context"]["kind"],
                "analysis": payload["business_context"]["analysis"],
                "user_prompt": "focus on discounts and returns",
            },
        )
        self.assertEqual(prompted.status_code, 200)
        self.assertIn("discounts", prompted.json()["focus_tags"])
        self.assertIn("returns", prompted.json()["focus_tags"])

        baseline_ids = [item["id"] for item in baseline.json()["insights"]]
        prompted_ids = [item["id"] for item in prompted.json()["insights"]]
        self.assertLess(prompted_ids.index("payment_myth"), baseline_ids.index("payment_myth"))

    def test_build_dashboard_handles_zero_approved_and_invalid_sections(self) -> None:
        analyze_response = self._post_fixture("ecommerce/happy_path.csv")
        payload = analyze_response.json()

        build_response = self.client.post(
            "/api/build-dashboard",
            json={
                "kind": payload["business_context"]["kind"],
                "analysis": payload["business_context"]["analysis"],
                "approved_insight_ids": [],
                "settings": {
                    "title": "Zero Approval Check",
                    "subtitle": "Stress harness",
                    "included_sections": ["overview", "bogus_section", "discounts"],
                    "metric_count": 2,
                    "show_notes": False,
                },
            },
        )
        self.assertEqual(build_response.status_code, 200)
        dashboard = build_response.json()
        self.assertEqual(dashboard["blueprint"]["title"], "Zero Approval Check")
        self.assertEqual([section["id"] for section in dashboard["blueprint"]["layout_sections"]], ["overview", "discounts"])
        self.assertGreater(len(dashboard["blueprint"]["approved_insights"]), 0)

    def test_build_dashboard_rejects_mismatched_analysis_payload(self) -> None:
        analyze_response = self._post_fixture("ecommerce/happy_path.csv")
        payload = analyze_response.json()

        build_response = self.client.post(
            "/api/build-dashboard",
            json={
                "kind": "financial_timeseries",
                "analysis": payload["business_context"]["analysis"],
                "approved_insight_ids": ["discount_paradox"],
                "settings": {"included_sections": ["overview"]},
            },
        )
        self.assertEqual(build_response.status_code, 400)
        self.assertIn("Invalid analysis payload", build_response.json()["detail"])

    def test_large_synthetic_csv_smoke(self) -> None:
        rows = [
            "order_date,order_value,category,channel,payment_method,device,discount_pct,returned,customer_id"
        ]
        for idx in range(1, 1501):
            rows.append(
                f"2024-01-{(idx % 28) + 1:02d},{300 + (idx % 90)},Electronics,Direct,BNPL,Desktop,{idx % 15},{idx % 2},c{idx % 75}"
            )

        response = self.client.post(
            "/api/analyze",
            files={"file": ("large-orders.csv", io.BytesIO("\n".join(rows).encode("utf-8")), "text/csv")},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["business_context"]["kind"], "ecommerce_orders")


if __name__ == "__main__":
    unittest.main()
