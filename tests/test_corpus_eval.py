from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
for path in (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

try:
    import corpus_eval

    HAS_FASTAPI = True
except ModuleNotFoundError:
    corpus_eval = None
    HAS_FASTAPI = False


def scored(expected: list[str], detected: str | None, *, insights: int = 5, sections: int = 4, outcome: str | None = None) -> dict:
    result = {
        "id": f"{'+'.join(expected) or 'unlabeled'}->{detected}",
        "expected": expected,
        "detected": detected,
        "profile": None,
        "insights": insights if detected else 0,
        "sections": sections if detected else 0,
        "has_dashboard": bool(detected),
        "nearest": corpus_eval.explain_miss(["Order Date", "Sales", "Category", "Discount"]),
        "issue": "",
        "sha256": "same",
    }
    result["outcome"] = outcome or corpus_eval.classify(result)
    return result


@unittest.skipUnless(HAS_FASTAPI, "fastapi is not installed in the current Python environment")
class CorpusEvalTests(unittest.TestCase):
    def test_classify_covers_every_labeled_outcome(self) -> None:
        self.assertEqual(scored(["hr_workforce"], "hr_workforce")["outcome"], "ok")
        self.assertEqual(scored(["hr_workforce"], "hr_workforce", insights=1)["outcome"], "thin")
        self.assertEqual(scored(["hr_workforce"], None)["outcome"], "missed")
        self.assertEqual(scored(["hr_workforce"], "survey_sentiment")["outcome"], "wrong_template")
        self.assertEqual(scored(["generic"], None)["outcome"], "generic")
        self.assertEqual(scored(["generic"], "ecommerce_orders")["outcome"], "wrong_template")

    def test_classify_accepts_any_listed_label_and_unlabeled_files(self) -> None:
        self.assertEqual(scored(["financial_timeseries", "generic"], None)["outcome"], "generic")
        self.assertEqual(scored(["financial_timeseries", "generic"], "financial_timeseries")["outcome"], "ok")
        self.assertEqual(scored([], None)["outcome"], "generic")
        self.assertEqual(scored([], "web_app_analytics", sections=2)["outcome"], "thin")

    def test_explain_miss_names_the_missing_required_fields(self) -> None:
        nearest = corpus_eval.explain_miss(["Order Date", "Sales", "Category", "Discount"])["ecommerce_orders"]
        self.assertEqual(nearest["matched"], 3)
        self.assertEqual(nearest["missing"], ["date", "channel", "payment", "device", "returned"])

    def test_scorecard_rates_and_baseline_diff(self) -> None:
        results = [
            scored(["hr_workforce"], "hr_workforce"),
            scored(["hr_workforce"], None),
            scored(["ecommerce_orders"], None),
            scored(["generic"], None),
            scored(["generic"], "ecommerce_orders"),
            scored(["survey_sentiment"], None, outcome="unreadable"),
        ]
        scorecard = corpus_eval.build_scorecard(results, not_fetched=["k-web-ab-data"])

        rates = scorecard["rates"]
        self.assertEqual(rates["specialized_recall"], {"count": 1, "of": 4, "pct": 25.0})
        self.assertEqual(rates["generic_kept_generic"], {"count": 1, "of": 2, "pct": 50.0})
        self.assertEqual(rates["wrong_template"]["count"], 1)
        self.assertEqual(rates["dashboard_output"], {"count": 2, "of": 5, "pct": 40.0})
        self.assertEqual(scorecard["per_kind"]["ecommerce_orders"]["false_positives"], 1)
        self.assertEqual(scorecard["missing_fields"]["ecommerce_orders"]["channel"], 1)
        self.assertIn("k-web-ab-data", corpus_eval.render_markdown(scorecard))

        improved = [dict(item) for item in results]
        improved[1] = scored(["hr_workforce"], "hr_workforce")
        improved[1]["id"] = results[1]["id"]
        improved[0] = dict(results[0], outcome="crash")
        regressions = corpus_eval.print_baseline_diff(scorecard, corpus_eval.build_scorecard(improved, []))
        self.assertEqual(regressions, [f"{results[0]['id']}: ok -> crash"])


if __name__ == "__main__":
    unittest.main()
