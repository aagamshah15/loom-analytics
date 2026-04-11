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
from pipeline.business.healthcare_dashboard import (
    analyze_healthcare_context,
    build_business_dashboard as build_healthcare_dashboard,
    build_healthcare_insight_candidates,
)
from pipeline.business.marketing_dashboard import (
    analyze_marketing_context,
    build_business_dashboard as build_marketing_dashboard,
    build_marketing_insight_candidates,
)
from pipeline.business.survey_dashboard import (
    analyze_survey_context,
    build_business_dashboard as build_survey_dashboard,
    build_survey_insight_candidates,
)
from pipeline.business.web_analytics_dashboard import (
    analyze_web_analytics_context,
    build_business_dashboard as build_web_analytics_dashboard,
    build_web_analytics_insight_candidates,
)
from pipeline.business.hr_dashboard import (
    analyze_hr_context,
    build_business_dashboard as build_hr_business_dashboard,
    build_hr_insight_candidates,
)
from pipeline.business.router import build_dashboard, detect_business_context
from pipeline.common.contracts import PipelineContext


class BusinessDashboardTests(unittest.TestCase):
    def test_hr_detector_supports_ibm_attrition_style_schema(self) -> None:
        rows = []
        for index in range(24):
            rows.append(
                {
                    "EmployeeNumber": index + 1,
                    "Age": 24 + (index % 18),
                    "Attrition": "Yes" if index % 5 == 0 else "No",
                    "Department": ["Sales", "Research & Development", "Human Resources"][index % 3],
                    "Gender": "Female" if index % 2 == 0 else "Male",
                    "JobLevel": 1 + (index % 4),
                    "MonthlyIncome": 3200 + (index % 9) * 900,
                    "PerformanceRating": 3 if index % 4 else 4,
                    "JobSatisfaction": 1 + (index % 4),
                    "TrainingTimesLastYear": index % 6,
                    "YearsAtCompany": index % 10,
                }
            )

        context = PipelineContext(clean_df=pd.DataFrame(rows))
        detected = detect_business_context(context)

        self.assertIsNotNone(detected)
        self.assertEqual(detected["kind"], "hr_workforce")

        analysis = analyze_hr_context(context)
        self.assertIsNotNone(analysis)
        self.assertGreater(len(build_hr_insight_candidates(analysis, "")["insights"]), 0)

    def test_hr_detector_supports_simple_attrition_risk_schema(self) -> None:
        rows = []
        departments = ["sales", "support", "technical", "management", "hr"]
        salaries = ["low", "medium", "high"]
        for index in range(30):
            rows.append(
                {
                    "satisfaction_level": 0.38 + (index % 5) * 0.11,
                    "last_evaluation": 0.52 + (index % 4) * 0.1,
                    "number_project": 2 + (index % 4),
                    "average_montly_hours": 145 + (index % 5) * 22,
                    "time_spend_company": 2 + (index % 6),
                    "Work_accident": 1 if index % 9 == 0 else 0,
                    "left": 1 if index % 4 == 0 else 0,
                    "promotion_last_5years": 1 if index % 11 == 0 else 0,
                    "Department": departments[index % len(departments)],
                    "salary": salaries[index % len(salaries)],
                }
            )

        context = PipelineContext(clean_df=pd.DataFrame(rows))
        detected = detect_business_context(context)

        self.assertIsNotNone(detected)
        self.assertEqual(detected["kind"], "hr_workforce")

        analysis = analyze_hr_context(context)
        self.assertIsNotNone(analysis)
        insight_ids = {item["id"] for item in build_hr_insight_candidates(analysis, "")["insights"]}
        self.assertTrue({"high_performers_leave_too", "engagement_does_not_save_attrition"} & insight_ids)

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

    def test_prompt_terms_can_raise_non_keyword_insights(self) -> None:
        df = pd.DataFrame(
            {
                "order_date": pd.to_datetime(
                    [
                        "2024-01-05",
                        "2024-01-12",
                        "2024-01-19",
                        "2024-02-02",
                        "2024-02-09",
                        "2024-02-16",
                        "2024-03-01",
                        "2024-03-08",
                        "2024-03-15",
                        "2024-03-22",
                        "2024-04-05",
                        "2024-04-12",
                        "2024-04-19",
                        "2024-04-26",
                        "2024-05-03",
                        "2024-05-10",
                        "2024-05-17",
                        "2024-05-24",
                        "2024-05-31",
                        "2024-06-07",
                    ]
                ),
                "order_value": [370, 335, 332, 368, 375, 360, 355, 340, 372, 378, 365, 358, 349, 320, 330, 300, 295, 344, 390, 410],
                "category": [
                    "Electronics", "Clothing", "Clothing", "Home", "Electronics",
                    "Clothing", "Beauty", "Books", "Electronics", "Clothing",
                    "Electronics", "Sports", "Home", "Clothing", "Clothing",
                    "Books", "Beauty", "Clothing", "Electronics", "Home",
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
                    "c1", "c2", "c2", "c3", "c1",
                    "c4", "c5", "c6", "c1", "c2",
                    "c7", "c8", "c9", "c4", "c4",
                    "c10", "c5", "c1", "c11", "c12",
                ],
            }
        )
        analysis = analyze_ecommerce_context(PipelineContext(clean_df=df))
        self.assertIsNotNone(analysis)

        baseline = build_ecommerce_insight_candidates(analysis, "")
        prompted = build_ecommerce_insight_candidates(analysis, "focus the story on buyer quality")

        baseline_scores = {item["id"]: item["score"] for item in baseline["insights"]}
        prompted_scores = {item["id"]: item["score"] for item in prompted["insights"]}

        self.assertGreater(prompted_scores["channel_winner"], baseline_scores["channel_winner"])

    def test_build_healthcare_dashboard_for_patient_data(self) -> None:
        df = pd.DataFrame(
            {
                "patient_id": [f"p{i}" for i in range(1, 21)],
                "medication_adherence": [
                    "High", "High", "High", "High", "High",
                    "Low", "Low", "Low", "Low", "Low",
                    "Medium", "Medium", "Medium", "Medium", "Medium",
                    "Low", "High", "Telehealth", "Low", "High",
                ],
                "readmitted": [0, 0, 0, 1, 0, 1, 1, 0, 1, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0],
                "visit_type": [
                    "In-person", "In-person", "Telehealth", "Telehealth", "Telehealth",
                    "In-person", "In-person", "Telehealth", "In-person", "In-person",
                    "Telehealth", "Telehealth", "In-person", "In-person", "Telehealth",
                    "In-person", "Telehealth", "Telehealth", "In-person", "Telehealth",
                ],
                "satisfaction_score": [4.1, 4.0, 4.8, 4.7, 4.6, 3.9, 3.8, 4.6, 3.7, 3.8, 4.3, 4.4, 4.0, 4.1, 4.5, 3.6, 4.7, 4.8, 3.5, 4.6],
                "follow_up_scheduled": [1, 1, 1, 0, 0, 1, 0, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 0, 1],
                "insurance_type": [
                    "Medicare", "Medicare", "Commercial", "Commercial", "Commercial",
                    "Self-pay", "Self-pay", "Medicaid", "Self-pay", "Self-pay",
                    "Commercial", "Medicare", "Medicaid", "Commercial", "Medicare",
                    "Self-pay", "Commercial", "Commercial", "Self-pay", "Medicare",
                ],
                "cost": [3945, 4050, 3200, 3180, 3120, 3801, 3850, 2900, 3790, 3840, 3300, 3980, 3010, 3250, 3890, 3815, 3150, 3220, 3825, 4010],
                "race": [
                    "White", "White", "Black", "Hispanic", "White",
                    "Black", "Hispanic", "White", "Black", "Hispanic",
                    "White", "White", "Black", "Hispanic", "White",
                    "Black", "White", "White", "Hispanic", "White",
                ],
            }
        )

        context = PipelineContext(clean_df=df)
        detected = detect_business_context(context)
        self.assertIsNotNone(detected)
        self.assertEqual(detected["kind"], "healthcare_medical")

        analysis = analyze_healthcare_context(context)
        self.assertIsNotNone(analysis)
        insights = build_healthcare_insight_candidates(analysis, "focus on telehealth and adherence")
        self.assertIn("telehealth", insights["focus_tags"])
        self.assertIn("adherence", insights["focus_tags"])
        self.assertGreater(len(insights["insights"]), 0)

        approved_ids = [item["id"] for item in insights["insights"][:4]]
        dashboard = build_healthcare_dashboard(
            analysis=analysis,
            approved_insight_ids=approved_ids,
            user_prompt="focus on telehealth and adherence",
            settings={
                "title": "Healthcare Outcomes Dashboard",
                "subtitle": "Approved care insights",
                "included_sections": ["overview", "adherence", "care_delivery", "equity", "costs", "notes"],
                "metric_count": 4,
                "show_notes": True,
            },
        )

        self.assertIsNotNone(dashboard)
        self.assertEqual(dashboard["kind"], "healthcare_medical")
        self.assertIn("Healthcare Outcomes Dashboard", dashboard["html"])
        self.assertIn("blueprint", dashboard)
        self.assertGreater(len(dashboard["blueprint"]["layout_sections"]), 0)

        routed_dashboard = build_dashboard(
            kind="healthcare_medical",
            analysis=analysis,
            approved_insight_ids=approved_ids,
            settings={"included_sections": ["overview", "adherence", "care_delivery"]},
        )
        self.assertIsNotNone(routed_dashboard)
        self.assertEqual(routed_dashboard["kind"], "healthcare_medical")

    def test_build_healthcare_dashboard_for_admissions_data(self) -> None:
        admission_dates = pd.date_range("2024-01-01", periods=24, freq="7D")
        discharge_dates = admission_dates + pd.to_timedelta(
            [12, 14, 16, 15, 13, 17, 15, 16, 14, 15, 13, 17, 12, 14, 16, 15, 13, 17, 15, 16, 14, 15, 13, 17],
            unit="D",
        )
        df = pd.DataFrame(
            {
                "Name": [f"Patient {i}" for i in range(1, 25)],
                "Age": [45, 62, 38, 51, 57, 49, 66, 42, 53, 47, 60, 58, 44, 63, 39, 52, 56, 48, 67, 41, 54, 46, 59, 57],
                "Gender": ["Female", "Male"] * 12,
                "Medical Condition": [
                    "Obesity", "Diabetes", "Hypertension", "Obesity", "Arthritis", "Diabetes",
                    "Hypertension", "Obesity", "Arthritis", "Obesity", "Diabetes", "Hypertension",
                    "Obesity", "Diabetes", "Hypertension", "Obesity", "Arthritis", "Diabetes",
                    "Hypertension", "Obesity", "Arthritis", "Obesity", "Diabetes", "Hypertension",
                ],
                "Date of Admission": admission_dates,
                "Discharge Date": discharge_dates,
                "Insurance Provider": [
                    "Medicare", "Aetna", "Blue Cross", "Cigna", "UnitedHealthcare", "Medicare",
                    "Aetna", "Blue Cross", "Cigna", "UnitedHealthcare", "Medicare", "Aetna",
                    "Blue Cross", "Cigna", "UnitedHealthcare", "Medicare", "Aetna", "Blue Cross",
                    "Cigna", "UnitedHealthcare", "Medicare", "Aetna", "Blue Cross", "Cigna",
                ],
                "Billing Amount": [
                    27800, 25400, 25100, 28150, 24800, 25550,
                    25200, 27950, 24920, 28200, 25600, 25350,
                    27720, 25520, 25180, 28040, 24860, 25640,
                    25240, 27980, 24980, 28110, 25580, 25310,
                ],
                "Admission Type": [
                    "Emergency", "Elective", "Urgent", "Emergency", "Elective", "Urgent",
                    "Emergency", "Elective", "Urgent", "Emergency", "Elective", "Urgent",
                    "Emergency", "Elective", "Urgent", "Emergency", "Elective", "Urgent",
                    "Emergency", "Elective", "Urgent", "Emergency", "Elective", "Urgent",
                ],
                "Medication": ["Metformin", "Lisinopril", "Atorvastatin", "Semaglutide"] * 6,
                "Test Results": [
                    "Inconclusive", "Normal", "Abnormal", "Inconclusive", "Normal", "Abnormal",
                    "Inconclusive", "Normal", "Abnormal", "Inconclusive", "Normal", "Abnormal",
                    "Inconclusive", "Normal", "Abnormal", "Inconclusive", "Normal", "Abnormal",
                    "Inconclusive", "Normal", "Abnormal", "Inconclusive", "Normal", "Abnormal",
                ],
            }
        )

        context = PipelineContext(clean_df=df)
        detected = detect_business_context(context)
        self.assertIsNotNone(detected)
        self.assertEqual(detected["kind"], "healthcare_medical")

        analysis = analyze_healthcare_context(context)
        self.assertIsNotNone(analysis)
        self.assertEqual(analysis["profile"], "admissions")

        insights = build_healthcare_insight_candidates(analysis, "focus on diagnosis ambiguity and billing")
        insight_ids = {item["id"] for item in insights["insights"]}
        self.assertIn("diagnostic_ambiguity_signal", insight_ids)
        self.assertIn("payer_pricing_flat", insight_ids)

        dashboard = build_healthcare_dashboard(
            analysis=analysis,
            approved_insight_ids=list(insight_ids)[:4],
            user_prompt="focus on diagnosis ambiguity and billing",
            settings={
                "title": "Hospital Operations Dashboard",
                "subtitle": "Approved admissions insights",
                "included_sections": ["overview", "utilization", "diagnoses", "billing", "notes"],
                "metric_count": 4,
                "show_notes": True,
            },
        )

        self.assertIsNotNone(dashboard)
        self.assertEqual(dashboard["kind"], "healthcare_medical")
        self.assertIn("blueprint", dashboard)
        self.assertGreater(len(dashboard["blueprint"]["layout_sections"]), 0)
        self.assertEqual(dashboard["blueprint"]["layout_sections"][1]["id"], "utilization")

    def test_build_healthcare_dashboard_for_insurance_risk_data(self) -> None:
        df = pd.DataFrame(
            {
                "age": [19, 28, 33, 45, 52, 31, 41, 58, 23, 36, 49, 61, 27, 39, 54, 30, 43, 57, 25, 34, 47, 63, 29, 40],
                "sex": [
                    "female", "male", "female", "male", "female", "male", "female", "male",
                    "female", "male", "female", "male", "female", "male", "female", "male",
                    "female", "male", "female", "male", "female", "male", "female", "male",
                ],
                "bmi": [27.9, 33.0, 30.5, 22.4, 35.1, 28.7, 31.8, 29.4, 26.2, 34.5, 32.1, 36.8, 24.9, 30.8, 33.7, 27.1, 31.2, 35.4, 25.3, 29.9, 34.1, 37.0, 26.8, 30.1],
                "children": [0, 1, 2, 0, 3, 1, 2, 4, 0, 1, 2, 3, 0, 2, 3, 1, 2, 4, 0, 1, 2, 3, 1, 2],
                "smoker": [
                    "yes", "no", "yes", "no", "yes", "no", "yes", "no",
                    "yes", "no", "yes", "no", "yes", "no", "yes", "no",
                    "yes", "no", "yes", "no", "yes", "no", "yes", "no",
                ],
                "region": [
                    "southwest", "southeast", "northwest", "northeast", "southeast", "southwest",
                    "northwest", "northeast", "southwest", "southeast", "northwest", "northeast",
                    "southwest", "southeast", "northwest", "northeast", "southwest", "southeast",
                    "northwest", "northeast", "southwest", "southeast", "northwest", "northeast",
                ],
                "charges": [
                    16884, 1725, 4449, 21984, 38606, 3757, 8241, 11381,
                    23045, 6406, 28923, 15170, 2855, 9779, 46151, 4149,
                    12574, 14478, 1731, 7953, 30259, 27000, 48970, 8347,
                ],
            }
        )

        context = PipelineContext(clean_df=df)
        detected = detect_business_context(context)
        self.assertIsNotNone(detected)
        self.assertEqual(detected["kind"], "healthcare_medical")

        analysis = analyze_healthcare_context(context)
        self.assertIsNotNone(analysis)
        self.assertEqual(analysis["profile"], "insurance_risk")

        insights = build_healthcare_insight_candidates(analysis, "focus on smoking and pricing")
        insight_ids = {item["id"] for item in insights["insights"]}
        self.assertIn("smoking_cost_wall", insight_ids)
        self.assertIn("region_is_not_the_story", insight_ids)

        dashboard = build_healthcare_dashboard(
            analysis=analysis,
            approved_insight_ids=list(insight_ids)[:4],
            user_prompt="focus on smoking and pricing",
            settings={
                "title": "Healthcare Risk Dashboard",
                "subtitle": "Approved insurance risk insights",
                "included_sections": ["overview", "risk_factors", "demographics", "pricing", "notes"],
                "metric_count": 4,
                "show_notes": True,
            },
        )

        self.assertIsNotNone(dashboard)
        self.assertEqual(dashboard["kind"], "healthcare_medical")
        self.assertIn("blueprint", dashboard)
        self.assertGreater(len(dashboard["blueprint"]["layout_sections"]), 0)
        self.assertEqual(dashboard["blueprint"]["layout_sections"][1]["id"], "risk_factors")

    def test_build_marketing_dashboard_for_campaign_data(self) -> None:
        df = pd.DataFrame(
            {
                "campaign_id": [f"c{i}" for i in range(1, 25)],
                "channel": [
                    "TV/Radio", "TV/Radio", "TV/Radio", "TV/Radio",
                    "Display", "Display", "Display", "Display",
                    "Email", "Email", "Email", "Email",
                    "Paid Search", "Paid Search", "Paid Search", "Paid Search",
                    "Connected TV", "Connected TV", "Connected TV", "Connected TV",
                    "Mobile App", "Mobile App", "Social", "Social",
                ],
                "spend": [
                    10_000_000, 9_000_000, 8_000_000, 7_000_000,
                    11_000_000, 10_000_000, 10_000_000, 9_000_000,
                    1_200_000, 1_100_000, 1_300_000, 1_400_000,
                    8_000_000, 7_000_000, 8_000_000, 7_000_000,
                    6_000_000, 5_000_000, 6_000_000, 5_000_000,
                    4_500_000, 4_500_000, 4_000_000, 4_000_000,
                ],
                "revenue": [
                    1_200_000, 900_000, 1_000_000, 800_000,
                    1_500_000, 1_200_000, 1_300_000, 1_100_000,
                    960_000, 850_000, 1_020_000, 1_090_000,
                    1_080_000, 910_000, 1_000_000, 870_000,
                    3_000_000, 2_500_000, 3_100_000, 2_400_000,
                    1_150_000, 1_120_000, 1_000_000, 980_000,
                ],
                "experiment_group": [
                    "Control", "Variant A", "Variant B", "Control",
                    "Variant A", "Variant B", "Control", "Variant A",
                    "Control", "Control", "Control", "Control",
                    "Variant A", "Variant A", "Variant B", "Variant B",
                    "Control", "Control", "Variant A", "Variant B",
                    "Variant B", "Variant A", "Variant B", "Variant A",
                ],
                "device": [
                    "Desktop", "Desktop", "Desktop", "Connected TV",
                    "Desktop", "Mobile", "Mobile", "Desktop",
                    "Desktop", "Desktop", "Connected TV", "Desktop",
                    "Desktop", "Mobile", "Desktop", "Mobile",
                    "Connected TV", "Connected TV", "Connected TV", "Connected TV",
                    "Mobile", "Mobile", "Mobile", "Desktop",
                ],
                "bounce_rate": [
                    0.44, 0.45, 0.43, 0.45,
                    0.48, 0.51, 0.50, 0.47,
                    0.31, 0.30, 0.44, 0.29,
                    0.42, 0.50, 0.41, 0.53,
                    0.45, 0.44, 0.46, 0.45,
                    0.55, 0.53, 0.52, 0.40,
                ],
                "impressions": [
                    8_000_000, 7_500_000, 7_000_000, 6_500_000,
                    9_000_000, 8_500_000, 8_000_000, 7_500_000,
                    900_000, 850_000, 800_000, 780_000,
                    12_000_000, 11_000_000, 10_500_000, 10_000_000,
                    4_500_000, 4_000_000, 3_800_000, 3_700_000,
                    14_000_000, 13_500_000, 5_000_000, 4_800_000,
                ],
                "age_group": [
                    "18-24", "25-34", "35-44", "45-54",
                    "18-24", "25-34", "35-44", "45-54",
                    "18-24", "25-34", "35-44", "45-54",
                    "18-24", "25-34", "35-44", "45-54",
                    "18-24", "25-34", "35-44", "45-54",
                    "18-24", "25-34", "35-44", "45-54",
                ],
            }
        )

        context = PipelineContext(clean_df=df)
        detected = detect_business_context(context)
        self.assertIsNotNone(detected)
        self.assertEqual(detected["kind"], "marketing_campaign")

        analysis = analyze_marketing_context(context)
        self.assertIsNotNone(analysis)
        insights = build_marketing_insight_candidates(analysis, "focus on channels and mobile")
        self.assertIn("channels", insights["focus_tags"])
        self.assertIn("device", insights["focus_tags"])
        self.assertGreater(len(insights["insights"]), 0)

        insight_ids = {item["id"] for item in insights["insights"]}
        self.assertIn("channel_budget_black_holes", insight_ids)
        self.assertIn("mobile_conversion_leak", insight_ids)

        approved_ids = [item["id"] for item in insights["insights"][:4]]
        dashboard = build_marketing_dashboard(
            analysis=analysis,
            approved_insight_ids=approved_ids,
            user_prompt="focus on channels and mobile",
            settings={
                "title": "Marketing Efficiency Dashboard",
                "subtitle": "Approved campaign insights",
                "included_sections": ["overview", "channels", "testing", "audience", "funnel", "notes"],
                "metric_count": 4,
                "show_notes": True,
            },
        )

        self.assertIsNotNone(dashboard)
        self.assertEqual(dashboard["kind"], "marketing_campaign")
        self.assertIn("Marketing Efficiency Dashboard", dashboard["html"])
        self.assertIn("blueprint", dashboard)
        self.assertGreater(len(dashboard["blueprint"]["layout_sections"]), 0)

        routed_dashboard = build_dashboard(
            kind="marketing_campaign",
            analysis=analysis,
            approved_insight_ids=approved_ids,
            settings={"included_sections": ["overview", "channels", "funnel"]},
        )
        self.assertIsNotNone(routed_dashboard)
        self.assertEqual(routed_dashboard["kind"], "marketing_campaign")

    def test_marketing_crm_profile_detects(self) -> None:
        df = pd.DataFrame(
            {
                "Income": [20000, 22000, 25000, 28000, 30000, 32000, 35000, 38000, 42000, 45000, 50000, 55000, 60000, 65000, 70000, 75000, 80000, 85000, 90000, 95000],
                "Kidhome": [1, 1, 0, 1, 2, 0, 1, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0],
                "Teenhome": [1, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0],
                "Recency": [70, 66, 61, 58, 55, 49, 45, 39, 35, 31, 28, 24, 21, 17, 14, 10, 8, 6, 4, 2],
                "MntTotal": [40, 55, 65, 80, 95, 120, 150, 210, 260, 330, 420, 510, 620, 760, 890, 1020, 1180, 1350, 1600, 1850],
                "NumDealsPurchases": [2, 1, 3, 2, 4, 2, 5, 3, 1, 2, 4, 5, 1, 2, 5, 6, 1, 5, 6, 7],
                "NumWebPurchases": [1, 1, 2, 1, 2, 2, 3, 3, 4, 3, 4, 5, 4, 5, 6, 6, 5, 6, 7, 7],
                "NumCatalogPurchases": [0, 0, 1, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 4, 4, 5, 4, 5, 6, 6],
                "NumStorePurchases": [1, 1, 1, 2, 2, 2, 3, 2, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 7],
                "NumWebVisitsMonth": [8, 8, 7, 7, 6, 6, 6, 5, 5, 5, 4, 4, 4, 3, 3, 3, 2, 2, 2, 1],
                "AcceptedCmpOverall": [0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4],
                "Response": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 1, 1, 1, 1],
            }
        )
        context = PipelineContext(clean_df=df)
        analysis = analyze_marketing_context(context)
        self.assertIsNotNone(analysis)
        self.assertEqual(analysis["profile"], "crm")

    def test_marketing_lead_generation_profile_detects(self) -> None:
        df = pd.DataFrame(
            {
                "mql_id": [f"m{i}" for i in range(1, 25)],
                "first_contact_date": pd.date_range("2018-01-01", periods=24, freq="7D"),
                "landing_page_id": ["lp1", "lp1", "lp2", "lp2", "lp1", "lp3", "lp4", "lp1", "lp2", "lp3", "lp4", "lp5", "lp1", "lp2", "lp3", "lp4", "lp5", "lp6", "lp1", "lp2", "lp3", "lp4", "lp5", "lp6"],
                "origin": ["organic_search", "paid_search", "social", "unknown", "organic_search", "social", "paid_search", "organic_search", "unknown", "social", "email", "referral", "organic_search", "paid_search", "social", "unknown", "email", "referral", "organic_search", "paid_search", "social", "unknown", "email", "referral"],
            }
        )
        context = PipelineContext(clean_df=df)
        analysis = analyze_marketing_context(context)
        self.assertIsNotNone(analysis)
        self.assertEqual(analysis["profile"], "lead_generation")

    def test_marketing_closed_deals_profile_detects(self) -> None:
        df = pd.DataFrame(
            {
                "mql_id": [f"m{i}" for i in range(1, 25)],
                "won_date": pd.date_range("2018-01-01", periods=24, freq="10D"),
                "business_segment": ["home_decor", "pet", "car_accessories", "home_decor", "pet", "construction_tools_house_garden"] * 4,
                "lead_type": ["industry", "online_small", "online_big", "industry", "offline", "other"] * 4,
                "lead_behaviour_profile": ["shark", "cat", "eagle", "shark", "wolf", "cat"] * 4,
                "business_type": ["manufacturer", "reseller", "manufacturer", "manufacturer", "reseller", "other"] * 4,
                "declared_monthly_revenue": [500000, 12000, 80000, 450000, 3000, 210000] * 4,
            }
        )
        context = PipelineContext(clean_df=df)
        analysis = analyze_marketing_context(context)
        self.assertIsNotNone(analysis)
        self.assertEqual(analysis["profile"], "closed_deals")

    def test_build_marketing_dashboard_for_crm_profile(self) -> None:
        df = pd.DataFrame(
            {
                "Income": [20000, 22000, 25000, 28000, 30000, 32000, 35000, 38000, 42000, 45000, 50000, 55000, 60000, 65000, 70000, 75000, 80000, 85000, 90000, 95000],
                "Kidhome": [1, 1, 0, 1, 2, 0, 1, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0],
                "Teenhome": [1, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0],
                "Recency": [70, 66, 61, 58, 55, 49, 45, 39, 35, 31, 28, 24, 21, 17, 14, 10, 8, 6, 4, 2],
                "MntTotal": [40, 55, 65, 80, 95, 120, 150, 210, 260, 330, 420, 510, 620, 760, 890, 1020, 1180, 1350, 1600, 1850],
                "NumDealsPurchases": [2, 1, 3, 2, 4, 2, 5, 3, 1, 2, 4, 5, 1, 2, 5, 6, 1, 5, 6, 7],
                "NumWebPurchases": [1, 1, 2, 1, 2, 2, 3, 3, 4, 3, 4, 5, 4, 5, 6, 6, 5, 6, 7, 7],
                "NumCatalogPurchases": [0, 0, 1, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 4, 4, 5, 4, 5, 6, 6],
                "NumStorePurchases": [1, 1, 1, 2, 2, 2, 3, 2, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 7],
                "NumWebVisitsMonth": [8, 8, 7, 7, 6, 6, 6, 5, 5, 5, 4, 4, 4, 3, 3, 3, 2, 2, 2, 1],
                "AcceptedCmpOverall": [0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4],
                "Response": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 1, 1, 1, 1],
            }
        )
        analysis = analyze_marketing_context(PipelineContext(clean_df=df))
        self.assertIsNotNone(analysis)
        self.assertEqual(analysis["profile"], "crm")

        dashboard = build_marketing_dashboard(
            analysis=analysis,
            approved_insight_ids=None,
            settings={"included_sections": ["overview", "offers", "customers", "segments", "retention", "notes"]},
        )
        self.assertIsNotNone(dashboard)
        self.assertEqual(dashboard["kind"], "marketing_campaign")
        self.assertEqual(dashboard["blueprint"]["dataset"]["customer_count"], 20)
        self.assertIn("offers", [section["id"] for section in dashboard["blueprint"]["layout_sections"]])

    def test_build_marketing_dashboard_for_lead_generation_profile(self) -> None:
        df = pd.DataFrame(
            {
                "mql_id": [f"m{i}" for i in range(1, 25)],
                "first_contact_date": pd.date_range("2018-01-01", periods=24, freq="7D"),
                "landing_page_id": ["lp1", "lp1", "lp2", "lp2", "lp1", "lp3", "lp4", "lp1", "lp2", "lp3", "lp4", "lp5", "lp1", "lp2", "lp3", "lp4", "lp5", "lp6", "lp1", "lp2", "lp3", "lp4", "lp5", "lp6"],
                "origin": ["organic_search", "paid_search", "social", "unknown", "organic_search", "social", "paid_search", "organic_search", "unknown", "social", "email", "referral", "organic_search", "paid_search", "social", "unknown", "email", "referral", "organic_search", "paid_search", "social", "unknown", "email", "referral"],
            }
        )
        analysis = analyze_marketing_context(PipelineContext(clean_df=df))
        self.assertIsNotNone(analysis)
        self.assertEqual(analysis["profile"], "lead_generation")

        dashboard = build_marketing_dashboard(
            analysis=analysis,
            approved_insight_ids=None,
            settings={"included_sections": ["overview", "acquisition", "landing_pages", "sources", "notes"]},
        )
        self.assertIsNotNone(dashboard)
        self.assertEqual(dashboard["blueprint"]["dataset"]["lead_count"], 24)
        self.assertIn("landing_pages", [section["id"] for section in dashboard["blueprint"]["layout_sections"]])

    def test_build_marketing_dashboard_for_closed_deals_profile(self) -> None:
        df = pd.DataFrame(
            {
                "mql_id": [f"m{i}" for i in range(1, 25)],
                "won_date": pd.date_range("2018-01-01", periods=24, freq="10D"),
                "business_segment": ["home_decor", "pet", "car_accessories", "home_decor", "pet", "construction_tools_house_garden"] * 4,
                "lead_type": ["industry", "online_small", "online_big", "industry", "offline", "other"] * 4,
                "lead_behaviour_profile": ["shark", "cat", "eagle", "shark", "wolf", "cat"] * 4,
                "business_type": ["manufacturer", "reseller", "manufacturer", "manufacturer", "reseller", "other"] * 4,
                "declared_monthly_revenue": [500000, 12000, 80000, 450000, 3000, 210000] * 4,
            }
        )
        analysis = analyze_marketing_context(PipelineContext(clean_df=df))
        self.assertIsNotNone(analysis)
        self.assertEqual(analysis["profile"], "closed_deals")

        dashboard = build_marketing_dashboard(
            analysis=analysis,
            approved_insight_ids=None,
            settings={"included_sections": ["overview", "segments", "revenue", "sales_motion", "notes"]},
        )
        self.assertIsNotNone(dashboard)
        self.assertEqual(dashboard["blueprint"]["dataset"]["deal_count"], 24)
        self.assertIn("sales_motion", [section["id"] for section in dashboard["blueprint"]["layout_sections"]])

    def test_build_survey_dashboard_for_sentiment_data(self) -> None:
        df = pd.DataFrame(
            {
                "role": [
                    "Executive", "Executive", "Executive", "Executive", "Executive",
                    "End User", "End User", "End User", "End User", "End User",
                    "End User", "End User", "End User", "End User", "End User",
                    "Executive", "Executive", "End User", "End User", "End User",
                ],
                "tenure_months": [72, 65, 80, 58, 62, 1, 2, 2, 3, 4, 7, 8, 10, 14, 18, 84, 90, 3, 5, 6],
                "nps": [9, 8, 9, 7, 8, 2, 3, 1, 4, 5, 6, 5, 6, 7, 8, 9, 8, 4, 5, 6],
                "ces": [1.8, 2.0, 1.9, 2.5, 2.2, 4.8, 4.7, 4.9, 4.5, 4.0, 3.9, 3.8, 3.7, 3.1, 2.9, 1.7, 1.8, 4.2, 3.6, 3.4],
                "would_recommend": [1, 1, 1, 1, 1, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1],
                "renewal_intent": [4.8, 4.5, 4.9, 4.1, 4.3, 1.4, 1.8, 1.2, 2.0, 2.2, 2.7, 2.9, 3.0, 3.5, 3.8, 4.9, 4.6, 1.9, 2.4, 2.7],
                "reporting_score": [4.5, 4.2, 4.3, 4.0, 4.1, 2.1, 2.3, 1.9, 2.5, 2.7, 3.0, 3.1, 3.2, 3.4, 3.5, 4.4, 4.3, 2.4, 2.8, 3.0],
                "reliability_score": [4.8, 4.6, 4.7, 4.5, 4.4, 4.0, 4.1, 4.2, 4.0, 4.1, 4.2, 4.1, 4.3, 4.4, 4.5, 4.9, 4.8, 4.0, 4.2, 4.3],
                "complaint_theme": [
                    "Missing features", "Missing features", "Reporting", "Reporting", "Pricing",
                    "Missing features", "Missing features", "Reporting", "Reporting", "Support",
                    "Missing features", "Usability", "Reporting", "Support", "Pricing",
                    "Reporting", "Missing features", "Missing features", "Reporting", "Usability",
                ],
            }
        )

        context = PipelineContext(clean_df=df)
        detected = detect_business_context(context)
        self.assertIsNotNone(detected)
        self.assertEqual(detected["kind"], "survey_sentiment")

        analysis = analyze_survey_context(context)
        self.assertIsNotNone(analysis)
        insight_bundle = build_survey_insight_candidates(analysis, "focus on onboarding and renewal risk")
        self.assertIn("onboarding", insight_bundle["focus_tags"])
        self.assertIn("renewal", insight_bundle["focus_tags"])
        self.assertGreater(len(insight_bundle["insights"]), 0)

        approved_ids = [item["id"] for item in insight_bundle["insights"][:4]]
        dashboard = build_survey_dashboard(
            analysis=analysis,
            approved_insight_ids=approved_ids,
            user_prompt="focus on onboarding and renewal risk",
            settings={
                "title": "Survey Risk Dashboard",
                "subtitle": "Approved survey narrative",
                "included_sections": ["overview", "stakeholders", "onboarding", "effort", "product", "renewal", "notes"],
                "metric_count": 4,
                "show_notes": True,
            },
        )

        self.assertIsNotNone(dashboard)
        self.assertEqual(dashboard["kind"], "survey_sentiment")
        self.assertIn("Survey Risk Dashboard", dashboard["html"])
        self.assertIn("blueprint", dashboard)
        self.assertGreater(len(dashboard["blueprint"]["layout_sections"]), 0)

        routed_dashboard = build_dashboard(
            kind="survey_sentiment",
            analysis=analysis,
            approved_insight_ids=approved_ids,
            settings={"included_sections": ["overview", "renewal", "notes"]},
        )
        self.assertIsNotNone(routed_dashboard)
        self.assertEqual(routed_dashboard["kind"], "survey_sentiment")

    def test_build_survey_dashboard_for_text_sentiment_data(self) -> None:
        df = pd.DataFrame(
            {
                "2401": ["2402", "2403", "2404", "2405"] * 6,
                "Borderlands": ["Borderlands", "Facebook", "Twitter", "Twitter"] * 6,
                "Positive": ["Negative", "Neutral", "Positive", "Negative"] * 6,
                "im getting on borderlands and i will murder you all ,": [
                    "servers are broken and support is useless",
                    "patch notes are out and the update looks fine",
                    "love the gameplay loop here",
                    "matchmaking still feels awful tonight",
                ]
                * 6,
            }
        )

        context = PipelineContext(clean_df=df)
        detected = detect_business_context(context)
        self.assertIsNotNone(detected)
        self.assertEqual(detected["kind"], "survey_sentiment")
        self.assertEqual(detected["analysis"]["profile"], "text_sentiment")

        analysis = analyze_survey_context(context)
        self.assertIsNotNone(analysis)
        insight_bundle = build_survey_insight_candidates(analysis, "focus on platforms and negative tone")
        self.assertIn("sources", insight_bundle["focus_tags"])
        self.assertIn("language", insight_bundle["focus_tags"])

        dashboard = build_survey_dashboard(
            analysis=analysis,
            approved_insight_ids=[item["id"] for item in insight_bundle["insights"][:3]],
            settings={"included_sections": ["overview", "sources", "language", "momentum", "notes"]},
        )
        self.assertIsNotNone(dashboard)
        self.assertIn("Text Sentiment Narrative", dashboard["html"])

    def test_build_survey_dashboard_for_customer_satisfaction_data(self) -> None:
        df = pd.DataFrame(
            {
                "productRate": [4, 4, 5, 3, 4, 5] * 4,
                "priceRate": [3, 2, 3, 2, 3, 2] * 4,
                "promoRate": [5, 4, 5, 4, 5, 4] * 4,
                "ambianceRate": [5, 4, 4, 5, 4, 5] * 4,
                "wifiRate": [3, 4, 4, 3, 4, 3] * 4,
                "serviceRate": [4, 5, 5, 4, 5, 4] * 4,
                "chooseRate": [3, 2, 4, 3, 4, 3] * 4,
                "promoMethodApp": [1, 1, 1, 0, 1, 0] * 4,
                "promoMethodEmail": [0, 1, 1, 0, 1, 0] * 4,
                "promoMethodSoc": [1, 0, 1, 1, 0, 1] * 4,
                "visitNo": [3, 2, 4, 1, 3, 2] * 4,
                "loyal": [1, 0, 1, 0, 1, 0] * 4,
            }
        )

        context = PipelineContext(clean_df=df)
        detected = detect_business_context(context)
        self.assertIsNotNone(detected)
        self.assertEqual(detected["kind"], "survey_sentiment")
        self.assertEqual(detected["analysis"]["profile"], "satisfaction")

        analysis = analyze_survey_context(context)
        self.assertIsNotNone(analysis)
        dashboard = build_survey_dashboard(
            analysis=analysis,
            approved_insight_ids=None,
            settings={"included_sections": ["overview", "experience", "channels", "loyalty", "notes"]},
        )
        self.assertIsNotNone(dashboard)
        self.assertIn("Satisfaction Narrative", dashboard["html"])

    def test_build_survey_dashboard_for_wellbeing_data(self) -> None:
        df = pd.DataFrame(
            {
                "Perceived_Hearing_Meaning": ["Staying connected", "Enjoying music", "Staying alert", "Staying connected"] * 6,
                "Hearing_FOMO": ["Sometimes", "Rarely", "Often", "Sometimes"] * 6,
                "Hearing_Test_Barrier": ["Cost", "Never felt the need", "Cost", "Fear"] * 6,
                "Missed_Important_Sounds": ["Yes, in family conversations", "No", "Yes, in public spaces", "Yes, in family conversations"] * 6,
                "Left_Out_Due_To_Hearing": ["Yes, often", "Only in noisy places", "No", "Yes, often"] * 6,
                "Belief_Early_Hearing_Care": [5, 4, 5, 4] * 6,
                "Interest_in_Hearing_App": ["Yes", "Yes", "Maybe", "Yes"] * 6,
                "Paid_App_Test_Interest": ["Maybe, if it offers good value", "No", "Yes", "Maybe, if it offers good value"] * 6,
                "Age_group": ["18 - 24", "25 - 34", "18 - 24", "35 - 44"] * 6,
                "Ear_Discomfort_After_Use": ["No", "Yes", "Yes", "No"] * 6,
            }
        )

        context = PipelineContext(clean_df=df)
        detected = detect_business_context(context)
        self.assertIsNotNone(detected)
        self.assertEqual(detected["kind"], "survey_sentiment")
        self.assertEqual(detected["analysis"]["profile"], "wellbeing")

        analysis = analyze_survey_context(context)
        self.assertIsNotNone(analysis)
        dashboard = build_survey_dashboard(
            analysis=analysis,
            approved_insight_ids=None,
            settings={"included_sections": ["overview", "barriers", "wellbeing", "demographics", "notes"]},
        )
        self.assertIsNotNone(dashboard)
        self.assertIn("Wellbeing Survey Narrative", dashboard["html"])

    def test_build_web_analytics_dashboard_for_funnel_data(self) -> None:
        df = pd.DataFrame(
            {
                "device": [
                    "Mobile", "Mobile", "Desktop", "Desktop", "Tablet", "Tablet",
                    "Mobile", "Desktop", "Mobile", "Desktop", "Mobile", "Desktop",
                    "Mobile", "Desktop", "Mobile", "Desktop", "Mobile", "Desktop",
                    "Mobile", "Desktop",
                ],
                "channel": [
                    "Social", "Social", "Paid Search", "Paid Search", "Email", "Email",
                    "Organic", "Organic", "Social", "Email", "Direct", "Direct",
                    "Referral", "Referral", "Social", "Paid Search", "Organic", "Email",
                    "Direct", "Paid Search",
                ],
                "page": [
                    "Home", "Blog", "Pricing", "Home", "Dashboard", "Home",
                    "Blog", "Features", "Home", "Blog", "Home", "Pricing",
                    "Home", "Dashboard", "Blog", "Home", "Pricing", "Features",
                    "Home", "Blog",
                ],
                "sessions": [520, 410, 300, 360, 180, 170, 240, 210, 500, 150, 230, 260, 190, 130, 470, 320, 200, 160, 220, 280],
                "conversions": [18, 9, 22, 21, 24, 20, 11, 16, 17, 26, 13, 18, 9, 15, 16, 20, 13, 22, 12, 19],
                "bounce_rate": [61, 54, 34, 39, 28, 31, 40, 36, 58, 33, 42, 38, 44, 32, 60, 35, 37, 34, 43, 36],
                "load_time": [3.6, 3.4, 2.0, 1.9, 2.4, 2.2, 3.0, 2.1, 3.5, 2.0, 3.1, 2.0, 3.0, 2.3, 3.7, 2.0, 3.1, 2.1, 3.2, 2.0],
                "campaign": [
                    "Social Prospecting", "Social Prospecting", "Brand Search", "Brand Search", "Onboarding Email", "Onboarding Email",
                    "Lifecycle Nurture", "Lifecycle Nurture", "Social Prospecting", "Onboarding Email", "Homepage CTA", "Homepage CTA",
                    "Partner Launch", "Partner Launch", "Social Prospecting", "Brand Search", "Lifecycle Nurture", "Onboarding Email",
                    "Homepage CTA", "Brand Search",
                ],
                "scroll_depth": [32, 37, 58, 52, 69, 64, 48, 55, 34, 72, 46, 57, 43, 68, 33, 56, 49, 70, 45, 53],
                "avg_time_on_page": [120, 305, 150, 98, 340, 110, 290, 180, 102, 145, 85, 132, 118, 310, 300, 115, 165, 175, 92, 285],
                "visitor_type": [
                    "New", "New", "Returning", "New", "Returning", "New",
                    "Returning", "Returning", "New", "Returning", "New", "Returning",
                    "New", "Returning", "New", "Returning", "Returning", "Returning",
                    "New", "Returning",
                ],
                "exit_count": [160, 95, 54, 120, 20, 48, 60, 39, 155, 31, 70, 63, 58, 17, 148, 51, 40, 24, 65, 73],
            }
        )

        context = PipelineContext(clean_df=df)
        detected = detect_business_context(context)
        self.assertIsNotNone(detected)
        self.assertEqual(detected["kind"], "web_app_analytics")

        analysis = analyze_web_analytics_context(context)
        self.assertIsNotNone(analysis)
        insight_bundle = build_web_analytics_insight_candidates(analysis, "focus on mobile and channels")
        self.assertIn("mobile", insight_bundle["focus_tags"])
        self.assertIn("channels", insight_bundle["focus_tags"])
        self.assertGreater(len(insight_bundle["insights"]), 0)

        approved_ids = [item["id"] for item in insight_bundle["insights"][:4]]
        dashboard = build_web_analytics_dashboard(
            analysis=analysis,
            approved_insight_ids=approved_ids,
            user_prompt="focus on mobile and channels",
            settings={
                "title": "Web Funnel Dashboard",
                "subtitle": "Approved web analytics narrative",
                "included_sections": ["overview", "devices", "channels", "campaigns", "content", "retention", "notes"],
                "metric_count": 4,
                "show_notes": True,
            },
        )

        self.assertIsNotNone(dashboard)
        self.assertEqual(dashboard["kind"], "web_app_analytics")
        self.assertIn("Web Funnel Dashboard", dashboard["html"])
        self.assertIn("blueprint", dashboard)
        self.assertGreater(len(dashboard["blueprint"]["layout_sections"]), 0)

        routed_dashboard = build_dashboard(
            kind="web_app_analytics",
            analysis=analysis,
            approved_insight_ids=approved_ids,
            settings={"included_sections": ["overview", "devices", "channels", "content"]},
        )
        self.assertIsNotNone(routed_dashboard)
        self.assertEqual(routed_dashboard["kind"], "web_app_analytics")

    def test_build_hr_dashboard_for_workforce_data(self) -> None:
        df = pd.DataFrame(
            {
                "employee_id": [f"e{i}" for i in range(1, 25)],
                "department": [
                    "Engineering", "Engineering", "Sales", "Sales", "Customer Support", "Customer Support",
                    "Customer Support", "Customer Support", "HR", "HR", "Finance", "Finance",
                    "Engineering", "Sales", "Customer Support", "Product", "Product", "Customer Support",
                    "Marketing", "Marketing", "Operations", "Operations", "Customer Support", "Leadership",
                ],
                "performance_score": [4.4, 4.2, 3.8, 3.6, 4.1, 3.2, 4.3, 3.5, 4.0, 3.7, 3.8, 4.1, 4.5, 3.4, 3.3, 4.2, 4.1, 3.1, 3.9, 3.8, 3.7, 3.6, 3.2, 4.3],
                "attrition": [1, 0, 0, 1, 1, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 0, 0, 1, 0, 1, 0, 0, 1, 0],
                "engagement_score": [8.9, 8.5, 6.7, 6.1, 7.0, 5.8, 8.7, 4.9, 8.2, 6.4, 7.1, 8.0, 9.1, 5.7, 4.8, 8.6, 8.4, 5.0, 6.3, 5.9, 6.1, 6.0, 4.7, 8.8],
                "gender": [
                    "Female", "Male", "Female", "Male", "Female", "Male",
                    "Female", "Male", "Female", "Male", "Female", "Male",
                    "Female", "Male", "Female", "Female", "Male", "Male",
                    "Female", "Male", "Female", "Male", "Female", "Male",
                ],
                "level": [
                    "VP", "VP", "Director", "Director", "Manager", "Manager",
                    "Manager", "Manager", "Director", "Director", "Director", "Director",
                    "VP", "Director", "Manager", "Director", "Director", "Manager",
                    "Manager", "Manager", "Manager", "Manager", "Manager", "VP",
                ],
                "salary": [
                    262000, 299000, 182000, 188000, 97000, 99000, 101000, 98000, 176000, 185000, 179000, 186000,
                    268000, 181000, 96000, 184000, 187000, 100000, 108000, 109000, 103000, 104000, 97000, 301000,
                ],
                "training_hours": [52, 48, 18, 12, 10, 8, 50, 14, 46, 22, 12, 55, 60, 9, 11, 47, 49, 13, 16, 14, 19, 12, 10, 58],
                "work_mode": [
                    "Remote", "Onsite", "Hybrid", "Onsite", "Remote", "Onsite",
                    "Remote", "Remote", "Onsite", "Hybrid", "Onsite", "Remote",
                    "Remote", "Onsite", "Remote", "Hybrid", "Onsite", "Remote",
                    "Hybrid", "Onsite", "Onsite", "Hybrid", "Remote", "Onsite",
                ],
            }
        )

        context = PipelineContext(clean_df=df)
        detected = detect_business_context(context)
        self.assertIsNotNone(detected)
        self.assertEqual(detected["kind"], "hr_workforce")

        analysis = analyze_hr_context(context)
        self.assertIsNotNone(analysis)
        insights = build_hr_insight_candidates(analysis, "focus on pay equity and remote attrition")
        self.assertIn("pay_equity", insights["focus_tags"])
        self.assertIn("remote", insights["focus_tags"])
        self.assertGreater(len(insights["insights"]), 0)

        approved_ids = [item["id"] for item in insights["insights"][:4]]
        dashboard = build_hr_business_dashboard(
            analysis=analysis,
            approved_insight_ids=approved_ids,
            user_prompt="focus on pay equity and remote attrition",
            settings={
                "title": "Workforce Risk Dashboard",
                "subtitle": "Approved HR insights",
                "included_sections": ["overview", "retention", "compensation", "development", "workforce_model", "notes"],
                "metric_count": 4,
                "show_notes": True,
            },
        )

        self.assertIsNotNone(dashboard)
        self.assertEqual(dashboard["kind"], "hr_workforce")
        self.assertIn("Workforce Risk Dashboard", dashboard["html"])
        self.assertIn("blueprint", dashboard)
        self.assertGreater(len(dashboard["blueprint"]["layout_sections"]), 0)

        routed_dashboard = build_dashboard(
            kind="hr_workforce",
            analysis=analysis,
            approved_insight_ids=approved_ids,
            settings={"included_sections": ["overview", "retention", "compensation"]},
        )
        self.assertIsNotNone(routed_dashboard)
        self.assertEqual(routed_dashboard["kind"], "hr_workforce")


if __name__ == "__main__":
    unittest.main()
