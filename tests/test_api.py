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

    def test_analyze_and_build_dashboard_for_marketing_csv(self) -> None:
        csv_data = "\n".join(
            [
                "campaign_id,channel,spend,revenue,experiment_group,device,bounce_rate,impressions,age_group",
                "c1,TV/Radio,10000000,1200000,Control,Desktop,0.44,8000000,18-24",
                "c2,TV/Radio,9000000,900000,Variant A,Desktop,0.45,7500000,25-34",
                "c3,TV/Radio,8000000,1000000,Variant B,Desktop,0.43,7000000,35-44",
                "c4,TV/Radio,7000000,800000,Control,Connected TV,0.45,6500000,45-54",
                "c5,Display,11000000,1500000,Variant A,Desktop,0.48,9000000,18-24",
                "c6,Display,10000000,1200000,Variant B,Mobile,0.51,8500000,25-34",
                "c7,Display,10000000,1300000,Control,Mobile,0.50,8000000,35-44",
                "c8,Display,9000000,1100000,Variant A,Desktop,0.47,7500000,45-54",
                "c9,Email,1200000,960000,Control,Desktop,0.31,900000,18-24",
                "c10,Email,1100000,850000,Control,Desktop,0.30,850000,25-34",
                "c11,Email,1300000,1020000,Control,Connected TV,0.44,800000,35-44",
                "c12,Email,1400000,1090000,Control,Desktop,0.29,780000,45-54",
                "c13,Paid Search,8000000,1080000,Variant A,Desktop,0.42,12000000,18-24",
                "c14,Paid Search,7000000,910000,Variant A,Mobile,0.50,11000000,25-34",
                "c15,Paid Search,8000000,1000000,Variant B,Desktop,0.41,10500000,35-44",
                "c16,Paid Search,7000000,870000,Variant B,Mobile,0.53,10000000,45-54",
                "c17,Connected TV,6000000,3000000,Control,Connected TV,0.45,4500000,18-24",
                "c18,Connected TV,5000000,2500000,Control,Connected TV,0.44,4000000,25-34",
                "c19,Connected TV,6000000,3100000,Variant A,Connected TV,0.46,3800000,35-44",
                "c20,Connected TV,5000000,2400000,Variant B,Connected TV,0.45,3700000,45-54",
                "c21,Mobile App,4500000,1150000,Variant B,Mobile,0.55,14000000,18-24",
                "c22,Mobile App,4500000,1120000,Variant A,Mobile,0.53,13500000,25-34",
                "c23,Social,4000000,1000000,Variant B,Mobile,0.52,5000000,35-44",
                "c24,Social,4000000,980000,Variant A,Desktop,0.40,4800000,45-54",
            ]
        )

        analyze_response = self.client.post(
            "/api/analyze",
            files={"file": ("marketing.csv", io.BytesIO(csv_data.encode("utf-8")), "text/csv")},
        )
        self.assertEqual(analyze_response.status_code, 200)
        analyze_payload = analyze_response.json()
        self.assertEqual(analyze_payload["business_context"]["kind"], "marketing_campaign")

        build_response = self.client.post(
            "/api/build-dashboard",
            json={
                "kind": analyze_payload["business_context"]["kind"],
                "analysis": analyze_payload["business_context"]["analysis"],
                "approved_insight_ids": [item["id"] for item in analyze_payload["review"]["insights"][:3]],
                "settings": {
                    "title": "Marketing Preview",
                    "subtitle": "Approved campaign narrative",
                    "included_sections": ["overview", "channels", "testing", "audience", "funnel", "notes"],
                    "metric_count": 3,
                    "show_notes": True,
                },
            },
        )
        self.assertEqual(build_response.status_code, 200)
        payload = build_response.json()
        self.assertIn("blueprint", payload)
        self.assertEqual(payload["kind"], "marketing_campaign")

    def test_analyze_and_build_dashboard_for_survey_csv(self) -> None:
        csv_data = "\n".join(
            [
                "role,tenure_months,nps,ces,would_recommend,renewal_intent,reporting_score,reliability_score,complaint_theme",
                "Executive,72,9,1.8,1,4.8,4.5,4.8,Missing features",
                "Executive,65,8,2.0,1,4.5,4.2,4.6,Missing features",
                "Executive,80,9,1.9,1,4.9,4.3,4.7,Reporting",
                "Executive,58,7,2.5,1,4.1,4.0,4.5,Reporting",
                "Executive,62,8,2.2,1,4.3,4.1,4.4,Pricing",
                "End User,1,2,4.8,0,1.4,2.1,4.0,Missing features",
                "End User,2,3,4.7,0,1.8,2.3,4.1,Missing features",
                "End User,2,1,4.9,0,1.2,1.9,4.2,Reporting",
                "End User,3,4,4.5,1,2.0,2.5,4.0,Reporting",
                "End User,4,5,4.0,1,2.2,2.7,4.1,Support",
                "End User,7,6,3.9,1,2.7,3.0,4.2,Missing features",
                "End User,8,5,3.8,1,2.9,3.1,4.1,Usability",
                "End User,10,6,3.7,1,3.0,3.2,4.3,Reporting",
                "End User,14,7,3.1,1,3.5,3.4,4.4,Support",
                "End User,18,8,2.9,1,3.8,3.5,4.5,Pricing",
                "Executive,84,9,1.7,1,4.9,4.4,4.9,Reporting",
                "Executive,90,8,1.8,1,4.6,4.3,4.8,Missing features",
                "End User,3,4,4.2,0,1.9,2.4,4.0,Missing features",
                "End User,5,5,3.6,1,2.4,2.8,4.2,Reporting",
                "End User,6,6,3.4,1,2.7,3.0,4.3,Usability",
            ]
        )

        analyze_response = self.client.post(
            "/api/analyze",
            files={"file": ("survey.csv", io.BytesIO(csv_data.encode("utf-8")), "text/csv")},
        )
        self.assertEqual(analyze_response.status_code, 200)
        analyze_payload = analyze_response.json()
        self.assertEqual(analyze_payload["business_context"]["kind"], "survey_sentiment")

        build_response = self.client.post(
            "/api/build-dashboard",
            json={
                "kind": analyze_payload["business_context"]["kind"],
                "analysis": analyze_payload["business_context"]["analysis"],
                "approved_insight_ids": [item["id"] for item in analyze_payload["review"]["insights"][:3]],
                "settings": {
                    "title": "Survey Preview",
                    "subtitle": "Approved survey narrative",
                    "included_sections": ["overview", "stakeholders", "onboarding", "effort", "product", "renewal", "notes"],
                    "metric_count": 3,
                    "show_notes": True,
                },
            },
        )
        self.assertEqual(build_response.status_code, 200)
        payload = build_response.json()
        self.assertIn("blueprint", payload)
        self.assertEqual(payload["kind"], "survey_sentiment")

    def test_analyze_and_build_dashboard_for_web_analytics_csv(self) -> None:
        csv_data = "\n".join(
            [
                "device,channel,page,sessions,conversions,bounce_rate,load_time,campaign,scroll_depth,avg_time_on_page,visitor_type,exit_count",
                "Mobile,Social,Home,520,18,61,3.6,Social Prospecting,32,120,New,160",
                "Mobile,Social,Blog,410,9,54,3.4,Social Prospecting,37,305,New,95",
                "Desktop,Paid Search,Pricing,300,22,34,2.0,Brand Search,58,150,Returning,54",
                "Desktop,Paid Search,Home,360,21,39,1.9,Brand Search,52,98,New,120",
                "Tablet,Email,Dashboard,180,24,28,2.4,Onboarding Email,69,340,Returning,20",
                "Tablet,Email,Home,170,20,31,2.2,Onboarding Email,64,110,New,48",
                "Mobile,Organic,Blog,240,11,40,3.0,Lifecycle Nurture,48,290,Returning,60",
                "Desktop,Organic,Features,210,16,36,2.1,Lifecycle Nurture,55,180,Returning,39",
                "Mobile,Social,Home,500,17,58,3.5,Social Prospecting,34,102,New,155",
                "Desktop,Email,Blog,150,26,33,2.0,Onboarding Email,72,145,Returning,31",
                "Mobile,Direct,Home,230,13,42,3.1,Homepage CTA,46,85,New,70",
                "Desktop,Direct,Pricing,260,18,38,2.0,Homepage CTA,57,132,Returning,63",
                "Mobile,Referral,Home,190,9,44,3.0,Partner Launch,43,118,New,58",
                "Desktop,Referral,Dashboard,130,15,32,2.3,Partner Launch,68,310,Returning,17",
                "Mobile,Social,Blog,470,16,60,3.7,Social Prospecting,33,300,New,148",
                "Desktop,Paid Search,Home,320,20,35,2.0,Brand Search,56,115,Returning,51",
                "Mobile,Organic,Pricing,200,13,37,3.1,Lifecycle Nurture,49,165,Returning,40",
                "Desktop,Email,Features,160,22,34,2.1,Onboarding Email,70,175,Returning,24",
                "Mobile,Direct,Home,220,12,43,3.2,Homepage CTA,45,92,New,65",
                "Desktop,Paid Search,Blog,280,19,36,2.0,Brand Search,53,285,Returning,73",
            ]
        )

        analyze_response = self.client.post(
            "/api/analyze",
            files={"file": ("web-analytics.csv", io.BytesIO(csv_data.encode("utf-8")), "text/csv")},
        )
        self.assertEqual(analyze_response.status_code, 200)
        analyze_payload = analyze_response.json()
        self.assertEqual(analyze_payload["business_context"]["kind"], "web_app_analytics")

        build_response = self.client.post(
            "/api/build-dashboard",
            json={
                "kind": analyze_payload["business_context"]["kind"],
                "analysis": analyze_payload["business_context"]["analysis"],
                "approved_insight_ids": [item["id"] for item in analyze_payload["review"]["insights"][:3]],
                "settings": {
                    "title": "Web Analytics Preview",
                    "subtitle": "Approved web narrative",
                    "included_sections": ["overview", "devices", "channels", "campaigns", "content", "retention", "notes"],
                    "metric_count": 3,
                    "show_notes": True,
                },
            },
        )
        self.assertEqual(build_response.status_code, 200)
        payload = build_response.json()
        self.assertIn("blueprint", payload)
        self.assertEqual(payload["kind"], "web_app_analytics")


if __name__ == "__main__":
    unittest.main()
