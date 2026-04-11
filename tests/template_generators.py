from __future__ import annotations

import random
from typing import Callable

import pandas as pd


def generate_financial_fixture(seed: int = 7, rows: int = 48) -> pd.DataFrame:
    random.seed(seed)
    price = 100.0
    records = []
    for index in range(rows):
        drift = 1.8 if index % 12 else -3.5
        open_price = price + random.uniform(-2.5, 2.5)
        close_price = max(25.0, open_price + drift + random.uniform(-1.4, 1.4))
        high = max(open_price, close_price) + random.uniform(0.2, 2.8)
        low = min(open_price, close_price) - random.uniform(0.2, 2.6)
        volume = 1_500_000 + (index % 6) * 220_000 + random.randint(0, 160_000)
        records.append(
            {
                "Date": f"2021-{(index % 12) + 1:02d}-{(index % 27) + 1:02d}",
                "Open": round(open_price, 2),
                "High": round(high, 2),
                "Low": round(low, 2),
                "Close": round(close_price, 2),
                "Volume": volume,
            }
        )
        price = close_price
    return pd.DataFrame(records)


def generate_ecommerce_fixture(seed: int = 11, rows: int = 48) -> pd.DataFrame:
    random.seed(seed)
    channels = ["Direct", "Organic", "Referral", "Paid Search", "Email", "Social"]
    categories = ["Electronics", "Clothing", "Beauty", "Books", "Home & Kitchen"]
    payments = ["BNPL", "PayPal", "Credit Card", "Debit Card", "Apple Pay"]
    devices = ["Desktop", "Mobile", "Tablet"]
    records = []
    for index in range(rows):
        channel = channels[index % len(channels)]
        category = categories[index % len(categories)]
        device = devices[index % len(devices)]
        order_value = 240 + (index % 9) * 22 + random.randint(-12, 18)
        discount = [0, 10, 25][index % 3]
        returned = 1 if category == "Clothing" and discount > 0 and index % 2 else 0
        records.append(
            {
                "order_date": f"2024-{(index % 12) + 1:02d}-{(index % 27) + 1:02d}",
                "order_value": order_value,
                "category": category,
                "channel": channel,
                "payment_method": payments[index % len(payments)],
                "device": device,
                "discount_pct": discount,
                "returned": returned,
                "customer_id": f"c{index % 19}",
            }
        )
    return pd.DataFrame(records)


def generate_healthcare_outcomes_fixture(seed: int = 13, rows: int = 36) -> pd.DataFrame:
    random.seed(seed)
    adherence_levels = ["High", "Low", "Medium"]
    visit_types = ["Telehealth", "In-person"]
    insurance_types = ["Commercial", "Medicare", "Medicaid", "Self-pay"]
    races = ["White", "Black", "Hispanic", "Asian"]
    records = []
    for index in range(rows):
        adherence = adherence_levels[index % len(adherence_levels)]
        telehealth = visit_types[index % len(visit_types)]
        readmitted = 1 if adherence == "Low" and index % 2 == 0 else 0
        if telehealth == "Telehealth" and adherence == "High":
            readmitted = 0
        records.append(
            {
                "patient_id": f"p{index + 1}",
                "medication_adherence": adherence,
                "readmitted": readmitted,
                "visit_type": telehealth,
                "satisfaction_score": round(3.4 + (0.8 if telehealth == "Telehealth" else 0.2) + (0.5 if adherence == "High" else -0.3), 2),
                "follow_up_scheduled": 1 if index % 2 else 0,
                "insurance_type": insurance_types[index % len(insurance_types)],
                "cost": 2800 + (index % 6) * 220 + (750 if adherence == "Low" else 0),
                "race": races[index % len(races)],
            }
        )
    return pd.DataFrame(records)


def generate_healthcare_admissions_fixture(seed: int = 17, rows: int = 30) -> pd.DataFrame:
    random.seed(seed)
    conditions = ["Obesity", "Diabetes", "Hypertension", "Arthritis"]
    admission_types = ["Emergency", "Elective", "Urgent"]
    insurers = ["Medicare", "Aetna", "Blue Cross", "Cigna"]
    records = []
    for index in range(rows):
        admission_date = pd.Timestamp("2024-01-01") + pd.Timedelta(days=index * 6)
        length_of_stay = 10 + (index % 5) * 2
        records.append(
            {
                "Name": f"Patient {index + 1}",
                "Age": 34 + (index % 28),
                "Gender": "Female" if index % 2 == 0 else "Male",
                "Medical Condition": conditions[index % len(conditions)],
                "Date of Admission": admission_date.strftime("%Y-%m-%d"),
                "Discharge Date": (admission_date + pd.Timedelta(days=length_of_stay)).strftime("%Y-%m-%d"),
                "Insurance Provider": insurers[index % len(insurers)],
                "Billing Amount": 22_000 + (index % 7) * 1_150,
                "Admission Type": admission_types[index % len(admission_types)],
                "Medication": ["Metformin", "Lisinopril", "Atorvastatin", "Semaglutide"][index % 4],
                "Test Results": ["Normal", "Abnormal", "Inconclusive"][index % 3],
            }
        )
    return pd.DataFrame(records)


def generate_healthcare_insurance_fixture(seed: int = 19, rows: int = 36) -> pd.DataFrame:
    random.seed(seed)
    records = []
    for index in range(rows):
        smoker = "yes" if index % 3 == 0 else "no"
        bmi = 24.5 + (index % 8) * 1.4
        charges = 2800 + bmi * 240 + (14_000 if smoker == "yes" else 0) + (index % 5) * 300
        records.append(
            {
                "age": 22 + (index % 39),
                "sex": "female" if index % 2 == 0 else "male",
                "bmi": round(bmi, 1),
                "children": index % 4,
                "smoker": smoker,
                "region": ["southwest", "southeast", "northwest", "northeast"][index % 4],
                "charges": round(charges, 2),
            }
        )
    return pd.DataFrame(records)


def generate_hr_fixture(seed: int = 23, rows: int = 36) -> pd.DataFrame:
    random.seed(seed)
    departments = ["Engineering", "Sales", "Customer Support", "Product", "Marketing", "Finance"]
    records = []
    for index in range(rows):
        department = departments[index % len(departments)]
        work_mode = ["Remote", "Onsite", "Hybrid"][index % 3]
        attrition = 1 if department == "Customer Support" and index % 2 == 0 else 0
        records.append(
            {
                "employee_id": f"e{index + 1}",
                "department": department,
                "performance_score": round(3.1 + (index % 5) * 0.3, 1),
                "attrition": attrition,
                "engagement_score": round(5.0 + (index % 6) * 0.7 - (0.6 if work_mode == "Remote" else 0.0), 1),
                "gender": "Female" if index % 2 == 0 else "Male",
                "level": ["Manager", "Director", "VP"][index % 3],
                "salary": 92_000 + (index % 7) * 11_000 + (28_000 if index % 3 == 2 else 0),
                "training_hours": 10 + (index % 6) * 8,
                "work_mode": work_mode,
            }
        )
    return pd.DataFrame(records)


def generate_marketing_attribution_fixture(seed: int = 29, rows: int = 24) -> pd.DataFrame:
    random.seed(seed)
    channels = ["TV/Radio", "Display", "Email", "Paid Search", "Connected TV", "Social"]
    devices = ["Desktop", "Mobile", "Connected TV"]
    groups = ["Control", "Variant A", "Variant B"]
    records = []
    for index in range(rows):
        channel = channels[index % len(channels)]
        spend = 1_200_000 + (index % 5) * 250_000
        multiplier = {
            "TV/Radio": 0.12,
            "Display": 0.14,
            "Email": 0.78,
            "Paid Search": 0.31,
            "Connected TV": 0.5,
            "Social": 0.26,
        }[channel]
        records.append(
            {
                "campaign_id": f"cmp{index + 1}",
                "channel": channel,
                "spend": spend,
                "revenue": round(spend * multiplier, 2),
                "experiment_group": groups[index % len(groups)],
                "device": devices[index % len(devices)],
                "bounce_rate": round(0.33 + (index % 4) * 0.06 + (0.12 if channel == "Social" else 0.0), 2),
                "impressions": 400_000 + (index % 6) * 55_000,
                "age_group": ["18-24", "25-34", "35-44", "45-54"][index % 4],
            }
        )
    return pd.DataFrame(records)


def generate_marketing_crm_fixture(seed: int = 31, rows: int = 36) -> pd.DataFrame:
    random.seed(seed)
    records = []
    for index in range(rows):
        prior_wins = 0 if index < 16 else (2 if index < 28 else 4)
        response = 1 if prior_wins >= 2 and index % 2 == 0 else 0
        mnt_total = 60 + (index % 8) * 150
        records.append(
            {
                "Income": 22_000 + (index % 12) * 5_400,
                "Kidhome": index % 3,
                "Teenhome": (index + 1) % 2,
                "Recency": 82 - index * 2,
                "MntTotal": mnt_total,
                "NumDealsPurchases": index % 7,
                "NumWebPurchases": 1 + (index % 7),
                "NumCatalogPurchases": index % 6,
                "NumStorePurchases": 1 + (index % 6),
                "NumWebVisitsMonth": 1 + (index % 9),
                "AcceptedCmpOverall": prior_wins,
                "Response": response,
            }
        )
    return pd.DataFrame(records)


def generate_marketing_lead_fixture(seed: int = 37, rows: int = 30) -> pd.DataFrame:
    random.seed(seed)
    origins = ["organic_search", "paid_search", "social", "unknown", "email", "referral"]
    landing_pages = ["lp-a", "lp-b", "lp-c", "lp-d", "lp-e"]
    records = []
    for index in range(rows):
        records.append(
            {
                "mql_id": f"m{index + 1}",
                "first_contact_date": (pd.Timestamp("2024-01-01") + pd.Timedelta(days=index * 5)).strftime("%Y-%m-%d"),
                "landing_page_id": landing_pages[index % len(landing_pages)],
                "origin": origins[index % len(origins)],
            }
        )
    return pd.DataFrame(records)


def generate_marketing_closed_deals_fixture(seed: int = 41, rows: int = 30) -> pd.DataFrame:
    random.seed(seed)
    records = []
    for index in range(rows):
        lead_type = ["industry", "online_small", "online_big", "offline", "other"][index % 5]
        behaviour = ["shark", "cat", "eagle", "wolf", "shark"][index % 5]
        business_type = ["manufacturer", "reseller", "manufacturer", "services", "manufacturer"][index % 5]
        revenue = {
            "industry": 420_000,
            "online_small": 18_000,
            "online_big": 80_000,
            "offline": 24_000,
            "other": 55_000,
        }[lead_type] + (index % 4) * 4_500
        records.append(
            {
                "mql_id": f"deal{index + 1}",
                "won_date": (pd.Timestamp("2024-01-10") + pd.Timedelta(days=index * 8)).strftime("%Y-%m-%d"),
                "business_segment": ["home_decor", "pet", "car_accessories", "construction", "food"][index % 5],
                "lead_type": lead_type,
                "lead_behaviour_profile": behaviour,
                "business_type": business_type,
                "declared_monthly_revenue": revenue,
            }
        )
    return pd.DataFrame(records)


def generate_survey_fixture(seed: int = 43, rows: int = 28) -> pd.DataFrame:
    random.seed(seed)
    records = []
    for index in range(rows):
        role = "Executive" if index % 6 == 0 else "End User"
        tenure = [1, 2, 3, 5, 7, 12, 18, 36, 72][index % 9]
        if role == "Executive":
            nps = 8 if index % 3 else 9
            ces = 2.0 + (index % 3) * 0.2
            renewal = 4.4 + (index % 3) * 0.2
        else:
            nps = [2, 3, 4, 5, 6, 7][index % 6]
            ces = 4.8 - (index % 5) * 0.3
            renewal = 1.5 + (index % 5) * 0.4
        records.append(
            {
                "role": role,
                "tenure_months": tenure,
                "nps": nps,
                "ces": round(ces, 2),
                "would_recommend": 1 if nps >= 7 else 0,
                "renewal_intent": round(renewal, 2),
                "reporting_score": round(3.1 if role == "End User" else 4.3, 2),
                "reliability_score": round(4.5 if role == "End User" else 4.8, 2),
                "complaint_theme": "Missing features" if index % 3 == 0 else ("Reporting" if index % 3 == 1 else "Usability"),
            }
        )
    return pd.DataFrame(records)


def generate_web_analytics_fixture(seed: int = 47, rows: int = 30) -> pd.DataFrame:
    random.seed(seed)
    devices = ["Mobile", "Desktop", "Tablet"]
    channels = ["Social", "Paid Search", "Email", "Organic", "Direct", "Referral"]
    pages = ["Home", "Blog", "Pricing", "Features", "Dashboard"]
    visitor_types = ["New", "Returning"]
    records = []
    for index in range(rows):
        device = devices[index % len(devices)]
        channel = channels[index % len(channels)]
        page = pages[index % len(pages)]
        sessions = 140 + (index % 7) * 35
        mobile_penalty = 0.037 if device == "Mobile" else 0.074 if device == "Desktop" else 0.056
        if channel == "Email":
            conversion_rate = 0.176
        elif channel == "Social":
            conversion_rate = 0.036
        else:
            conversion_rate = mobile_penalty
        conversions = round(sessions * conversion_rate)
        records.append(
            {
                "device": device,
                "channel": channel,
                "page": page,
                "sessions": sessions,
                "conversions": conversions,
                "bounce_rate": round(33 + (index % 5) * 4 + (14 if channel == "Social" else 0), 1),
                "load_time": round(3.46 if device == "Mobile" else 1.98 if device == "Desktop" else 2.45, 2),
                "campaign": "Onboarding Email" if channel == "Email" else ("Social Prospecting" if channel == "Social" else "Site Core"),
                "scroll_depth": round(34.3 if channel == "Social" else 58.5 + (index % 3) * 4.5, 1),
                "avg_time_on_page": 297 if page == "Blog" else 160 + (index % 4) * 24,
                "visitor_type": visitor_types[index % len(visitor_types)],
                "exit_count": 34 + (28 if page == "Home" else 10) + (index % 6) * 4,
            }
        )
    return pd.DataFrame(records)


def generate_large_fixture(kind: str, rows: int = 1200, seed: int = 97) -> pd.DataFrame:
    generators: dict[str, Callable[[int, int], pd.DataFrame]] = {
        "financial_timeseries": generate_financial_fixture,
        "ecommerce_orders": generate_ecommerce_fixture,
        "healthcare_medical": generate_healthcare_outcomes_fixture,
        "hr_workforce": generate_hr_fixture,
        "marketing_campaign": generate_marketing_crm_fixture,
        "survey_sentiment": generate_survey_fixture,
        "web_app_analytics": generate_web_analytics_fixture,
    }
    generator = generators[kind]
    return generator(seed=seed, rows=rows)
