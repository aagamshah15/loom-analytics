from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import logging
import subprocess
import sys
import time
import traceback
import warnings
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
# Added explicitly so the script also runs under `python -I`, which is how corpus files should be read.
for import_root in (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

from fastapi.testclient import TestClient

from corpus_fetch import DEFAULT_MANIFEST, corpus_dir, entry_path, load_manifest
from pipeline.api.app import app
from pipeline.business import (
    ecommerce_dashboard,
    financial_dashboard,
    healthcare_dashboard,
    hr_dashboard,
    marketing_dashboard,
    survey_dashboard,
    web_analytics_dashboard,
)


GENERIC = "generic"
SPECIALIZED_KINDS = [
    "financial_timeseries",
    "ecommerce_orders",
    "healthcare_medical",
    "marketing_campaign",
    "hr_workforce",
    "survey_sentiment",
    "web_app_analytics",
]

# Outcomes, best to worst. "generic" means Loom correctly declined to pick a template.
GOOD_OUTCOMES = {"ok", "generic"}
ACCEPTABLE_OUTCOMES = GOOD_OUTCOMES | {"thin"}
THIN_BELOW = 3

# Required-column tables per template profile, used only to explain a miss. Survey's
# text, satisfaction and wellbeing profiles match by pattern and have no table.
REQUIRED_FIELDS: dict[str, dict[str, dict[str, list[str]]]] = {
    "financial_timeseries": {"ohlcv": financial_dashboard.REQUIRED_COLUMN_ALIASES},
    "ecommerce_orders": {"orders": ecommerce_dashboard.REQUIRED_COLUMN_ALIASES},
    "hr_workforce": {"workforce": hr_dashboard.REQUIRED_COLUMN_ALIASES},
    "healthcare_medical": {
        "outcomes": healthcare_dashboard.OUTCOMES_REQUIRED_COLUMN_ALIASES,
        "admissions": healthcare_dashboard.ADMISSIONS_REQUIRED_COLUMN_ALIASES,
        "insurance_risk": healthcare_dashboard.INSURANCE_REQUIRED_COLUMN_ALIASES,
        "claims_fraud": healthcare_dashboard.CLAIMS_REQUIRED_COLUMN_ALIASES,
    },
    "marketing_campaign": {
        "attribution": marketing_dashboard.ATTRIBUTION_REQUIRED_COLUMN_ALIASES,
        "crm": marketing_dashboard.CRM_REQUIRED_COLUMN_ALIASES,
        "lead_generation": marketing_dashboard.LEAD_REQUIRED_COLUMN_ALIASES,
        "closed_deals": marketing_dashboard.DEAL_REQUIRED_COLUMN_ALIASES,
    },
    "survey_sentiment": {"b2b_nps": survey_dashboard.B2B_REQUIRED_COLUMN_ALIASES},
    "web_app_analytics": {"traffic": web_analytics_dashboard.REQUIRED_COLUMN_ALIASES},
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Score Loom's detection and dashboard output against a CSV corpus.")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--only", nargs="*", default=[], help="Limit to these corpus ids.")
    parser.add_argument(
        "--extra",
        nargs="*",
        default=[],
        type=Path,
        help="Extra folders or files of unlabeled CSVs. They are scored for output and crashes only.",
    )
    parser.add_argument("--fixtures", action="store_true", help="Also score the repo's labeled template fixtures.")
    parser.add_argument("--out", type=Path, default=PROJECT_ROOT / "output" / "corpus")
    parser.add_argument("--baseline", type=Path, help="A previous scorecard.json to diff against.")
    parser.add_argument("--fail-on-regression", action="store_true")
    args = parser.parse_args()

    logging.disable(logging.CRITICAL)
    warnings.simplefilter("ignore")

    targets, not_fetched = _collect_targets(args)
    if not targets:
        print("No corpus files found. Run scripts/corpus_fetch.py --yes, or pass --extra / --fixtures.")
        return 1

    client = TestClient(app)
    results = []
    for index, target in enumerate(targets, start=1):
        result = evaluate_file(client, target)
        results.append(result)
        print(f"[{index:>3}/{len(targets)}] {result['outcome']:14} {result['id']}")

    scorecard = build_scorecard(results, not_fetched)
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "scorecard.json").write_text(json.dumps(scorecard, indent=2), encoding="utf-8")
    (args.out / "scorecard.md").write_text(render_markdown(scorecard), encoding="utf-8")
    print()
    print(render_summary(scorecard))
    print(f"\nScorecard written to {args.out}/scorecard.md and scorecard.json")

    if args.baseline:
        regressions = print_baseline_diff(json.loads(args.baseline.read_text()), scorecard)
        if regressions and args.fail_on_regression:
            return 1
    return 0


def _collect_targets(args: argparse.Namespace) -> tuple[list[dict[str, Any]], list[str]]:
    targets: list[dict[str, Any]] = []
    not_fetched: list[str] = []

    root = corpus_dir()
    for entry in load_manifest(args.manifest):
        if args.only and entry["id"] not in args.only:
            continue
        path = entry_path(root, entry)
        if not path.exists():
            not_fetched.append(entry["id"])
            continue
        expected = entry["expected"]
        targets.append(
            {
                "id": entry["id"],
                "path": path,
                "group": "corpus",
                "expected": [expected] if isinstance(expected, str) else list(expected),
                "notes": entry.get("notes", ""),
            }
        )

    if args.fixtures and not args.only:
        fixtures_root = PROJECT_ROOT / "tests" / "fixtures"
        manifest = json.loads((fixtures_root / "template_manifest.json").read_text())
        for template in manifest["templates"]:
            for fixture in template["fixtures"]:
                targets.append(
                    {
                        "id": f"fixture:{fixture['path']}",
                        "path": fixtures_root / fixture["path"],
                        "group": "fixtures",
                        "expected": [fixture["expected_kind"]],
                        "notes": fixture["schema_family"],
                    }
                )

    for extra in [] if args.only else args.extra:
        extra = extra.expanduser()
        files = [extra] if extra.is_file() else sorted(extra.rglob("*.csv"))
        for path in files:
            relative = path.name if extra.is_file() else str(path.relative_to(extra))
            targets.append(
                {"id": f"{extra.name}:{relative}", "path": path, "group": extra.name, "expected": [], "notes": ""}
            )

    return targets, not_fetched


def evaluate_file(client: TestClient, target: dict[str, Any]) -> dict[str, Any]:
    """Run one CSV through the same two API calls the app makes and record what came back."""
    path: Path = target["path"]
    content = path.read_bytes()
    result: dict[str, Any] = {
        "id": target["id"],
        "group": target["group"],
        "file": path.name,
        "expected": target["expected"],
        "notes": target["notes"],
        "size_bytes": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
        "detected": None,
        "profile": None,
        "insights": 0,
        "sections": 0,
        "has_dashboard": False,
        "rows": None,
        "columns": [],
        "template_errors": [],
        "issue": "",
        "analyze_ms": None,
        "build_ms": None,
        "nearest": {},
    }

    started = time.perf_counter()
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            response = client.post("/api/analyze", files={"file": ("upload.csv", io.BytesIO(content), "text/csv")})
    except Exception:
        result["outcome"] = "crash"
        result["issue"] = "analyze: " + _last_traceback_lines()
        return result
    result["analyze_ms"] = round((time.perf_counter() - started) * 1000)

    if response.status_code == 422:
        result["outcome"] = "unreadable"
        result["issue"] = response.json().get("detail", "")
        return result
    if response.status_code != 200:
        result["outcome"] = "crash"
        result["issue"] = f"analyze returned {response.status_code}: {response.text[:200]}"
        return result

    payload = response.json()
    report = payload["report"]
    result["rows"] = report.get("row_count")
    result["columns"] = list(report.get("schema", {}).keys())
    result["template_errors"] = report.get("metadata", {}).get("template_errors", [])
    result["nearest"] = explain_miss(result["columns"])

    business_context = payload.get("business_context")
    if business_context is not None and business_context["kind"] != GENERIC:
        insights = payload["review"]["insights"]
        result["detected"] = business_context["kind"]
        result["profile"] = business_context["analysis"].get("profile")
        result["insights"] = len(insights)
        approved = [item["id"] for item in insights if item.get("recommended")] or [item["id"] for item in insights[:4]]
        # Same defaults the React app sends: recommended insights approved, overview plus every insight's section.
        included_sections = list(dict.fromkeys(["overview", *[item["section"] for item in insights]]))

        started = time.perf_counter()
        try:
            build = client.post(
                "/api/build-dashboard",
                json={
                    "kind": business_context["kind"],
                    "analysis": business_context["analysis"],
                    "approved_insight_ids": approved,
                    "settings": {"included_sections": included_sections, "metric_count": 4, "show_notes": True},
                },
            )
        except Exception:
            result["outcome"] = "crash"
            result["issue"] = "build-dashboard: " + _last_traceback_lines()
            return result
        result["build_ms"] = round((time.perf_counter() - started) * 1000)
        if build.status_code != 200:
            result["outcome"] = "crash"
            result["issue"] = f"build-dashboard returned {build.status_code}: {build.text[:200]}"
            return result
        dashboard = build.json()
        result["sections"] = len(dashboard["blueprint"]["layout_sections"])
        result["has_dashboard"] = bool(dashboard.get("html")) and result["sections"] > 0

    result["outcome"] = classify(result)
    if result["template_errors"] and not result["issue"]:
        result["issue"] = "; ".join(f"{item['kind']} {item['stage']}: {item['error']}" for item in result["template_errors"])
    return result


def classify(result: dict[str, Any]) -> str:
    detected = result["detected"] or GENERIC
    thin = result["insights"] < THIN_BELOW or result["sections"] < THIN_BELOW
    expected = result["expected"]

    if not expected:  # unlabeled: only output quality can be judged
        if detected == GENERIC:
            return "generic"
        return "thin" if thin else "ok"
    if detected in expected:
        if detected == GENERIC:
            return "generic"
        return "thin" if thin else "ok"
    return "missed" if detected == GENERIC else "wrong_template"


def explain_miss(columns: list[str]) -> dict[str, dict[str, Any]]:
    """For each template, the profile closest to matching and the required fields it lacks.

    A stand-in until detection itself can report this.
    """
    present = {str(column).lower().strip() for column in columns}
    nearest: dict[str, dict[str, Any]] = {}
    for kind, profiles in REQUIRED_FIELDS.items():
        best: Optional[dict[str, Any]] = None
        for profile, required in profiles.items():
            missing = [field for field, aliases in required.items() if not any(alias in present for alias in aliases)]
            matched = len(required) - len(missing)
            candidate = {"profile": profile, "matched": matched, "required": len(required), "missing": missing}
            if best is None or matched / len(required) > best["matched"] / best["required"]:
                best = candidate
        nearest[kind] = best or {}
    return nearest


def build_scorecard(results: list[dict[str, Any]], not_fetched: list[str]) -> dict[str, Any]:
    labeled = [item for item in results if item["expected"]]
    readable = [item for item in results if item["outcome"] != "unreadable"]

    per_kind: dict[str, dict[str, int]] = {}
    for kind in SPECIALIZED_KINDS:
        strict = [item for item in labeled if item["expected"] == [kind]]
        per_kind[kind] = {
            "expected_only_this": len(strict),
            "detected_correctly": sum(1 for item in strict if item["detected"] == kind),
            "thin": sum(1 for item in strict if item["detected"] == kind and item["outcome"] == "thin"),
            "accepts_this": sum(1 for item in labeled if kind in item["expected"]),
            "false_positives": sum(1 for item in labeled if item["detected"] == kind and kind not in item["expected"]),
        }

    missing_fields: dict[str, Counter] = defaultdict(Counter)
    for item in labeled:
        if item["outcome"] != "missed":
            continue
        for kind in item["expected"]:
            if kind != GENERIC and item["nearest"].get(kind):
                missing_fields[kind].update(item["nearest"][kind]["missing"])

    confusion: dict[str, Counter] = defaultdict(Counter)
    for item in labeled:
        confusion[" | ".join(item["expected"])][item["detected"] or GENERIC] += 1

    strict_specialized = [item for item in labeled if len(item["expected"]) == 1 and item["expected"][0] != GENERIC]
    should_be_generic = [item for item in labeled if item["expected"] == [GENERIC]]
    detected_specialized = [item for item in results if item["detected"]]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "commit": _git_commit(),
        "totals": {
            "files": len(results),
            "labeled": len(labeled),
            "not_fetched": len(not_fetched),
            "outcomes": dict(Counter(item["outcome"] for item in results)),
        },
        "rates": {
            "crashes": sum(1 for item in results if item["outcome"] == "crash"),
            "unreadable": sum(1 for item in results if item["outcome"] == "unreadable"),
            "dashboard_output": _rate(sum(1 for item in readable if item["has_dashboard"]), len(readable)),
            "specialized_recall": _rate(
                sum(1 for item in strict_specialized if item["detected"] == item["expected"][0]), len(strict_specialized)
            ),
            "acceptable_label": _rate(
                sum(1 for item in labeled if item["outcome"] in ACCEPTABLE_OUTCOMES), len(labeled)
            ),
            "wrong_template": _rate(sum(1 for item in labeled if item["outcome"] == "wrong_template"), len(labeled)),
            "generic_kept_generic": _rate(
                sum(1 for item in should_be_generic if not item["detected"]), len(should_be_generic)
            ),
            "thin_dashboards": _rate(
                sum(1 for item in detected_specialized if item["outcome"] == "thin"), len(detected_specialized)
            ),
        },
        "per_kind": per_kind,
        "confusion": {expected: dict(counts) for expected, counts in confusion.items()},
        "missing_fields": {kind: dict(counts.most_common()) for kind, counts in missing_fields.items()},
        "not_fetched": not_fetched,
        "files": results,
    }


def render_summary(scorecard: dict[str, Any]) -> str:
    rates = scorecard["rates"]
    totals = scorecard["totals"]
    lines = [
        f"Files scored: {totals['files']} ({totals['labeled']} labeled, {totals['not_fetched']} in the manifest not fetched)",
        f"Outcomes: {', '.join(f'{name}={count}' for name, count in sorted(totals['outcomes'].items()))}",
        f"Crashes: {rates['crashes']}   Unreadable: {rates['unreadable']}",
        f"Produced a dashboard: {_format_rate(rates['dashboard_output'])} of readable files",
        f"Specialized recall: {_format_rate(rates['specialized_recall'])} of files with one expected template",
        f"Acceptable result: {_format_rate(rates['acceptable_label'])} of labeled files",
        f"Wrong template: {_format_rate(rates['wrong_template'])} of labeled files",
        f"Generic files kept generic: {_format_rate(rates['generic_kept_generic'])}",
        f"Thin dashboards: {_format_rate(rates['thin_dashboards'])} of detected templates",
    ]
    return "\n".join(lines)


def render_markdown(scorecard: dict[str, Any]) -> str:
    lines = [
        "# Loom corpus scorecard",
        "",
        f"Generated {scorecard['generated_at']} at commit `{scorecard['commit']}`.",
        "",
        "```",
        render_summary(scorecard),
        "```",
        "",
        "## By template",
        "",
        "| Template | Expected (only this) | Detected | Thin | Also acceptable for | False positives |",
        "|---|---|---|---|---|---|",
    ]
    for kind, stats in scorecard["per_kind"].items():
        lines.append(
            f"| {kind} | {stats['expected_only_this']} | {stats['detected_correctly']} | {stats['thin']} | "
            f"{stats['accepts_this'] - stats['expected_only_this']} | {stats['false_positives']} |"
        )

    lines += ["", "## Expected vs detected", "", "| Expected | Detected | Files |", "|---|---|---|"]
    for expected, counts in sorted(scorecard["confusion"].items()):
        for detected, count in sorted(counts.items(), key=lambda pair: -pair[1]):
            lines.append(f"| {expected} | {detected} | {count} |")

    lines += ["", "## Required fields most often missing on a miss", ""]
    for kind, counts in scorecard["missing_fields"].items():
        lines.append(f"- **{kind}**: " + ", ".join(f"{field} ({count})" for field, count in counts.items()))
    if not scorecard["missing_fields"]:
        lines.append("None.")

    problems = [item for item in scorecard["files"] if item["outcome"] not in GOOD_OUTCOMES]
    lines += ["", f"## Files needing attention ({len(problems)})", "", "| Outcome | File | Expected | Detected | Insights | Sections | Detail |", "|---|---|---|---|---|---|---|"]
    for item in sorted(problems, key=lambda entry: (entry["outcome"], entry["id"])):
        lines.append(
            f"| {item['outcome']} | {item['id']} | {', '.join(item['expected']) or '-'} | "
            f"{item['detected'] or GENERIC}{'/' + item['profile'] if item['profile'] else ''} | "
            f"{item['insights']} | {item['sections']} | {_detail(item)} |"
        )

    if scorecard["not_fetched"]:
        lines += ["", f"## Not fetched ({len(scorecard['not_fetched'])})", "", ", ".join(scorecard["not_fetched"])]
    return "\n".join(lines) + "\n"


def print_baseline_diff(baseline: dict[str, Any], current: dict[str, Any]) -> list[str]:
    previous = {item["id"]: item for item in baseline["files"]}
    regressions: list[str] = []
    improvements: list[str] = []
    changed_files: list[str] = []
    for item in current["files"]:
        before = previous.get(item["id"])
        if before is None:
            continue
        if before["sha256"] != item["sha256"]:
            changed_files.append(item["id"])
        if before["outcome"] == item["outcome"]:
            continue
        line = f"{item['id']}: {before['outcome']} -> {item['outcome']}"
        if _outcome_rank(item["outcome"]) > _outcome_rank(before["outcome"]):
            regressions.append(line)
        else:
            improvements.append(line)

    print(f"\nCompared with baseline at {baseline.get('commit')}: {len(improvements)} improved, {len(regressions)} regressed")
    for line in improvements:
        print(f"  + {line}")
    for line in regressions:
        print(f"  - {line}")
    if changed_files:
        print(f"  note: the file contents changed since the baseline for: {', '.join(changed_files)}")
    return regressions


def _outcome_rank(outcome: str) -> int:
    order = ["ok", "generic", "thin", "missed", "wrong_template", "unreadable", "crash"]
    return order.index(outcome) if outcome in order else len(order)


def _detail(item: dict[str, Any]) -> str:
    if item["issue"]:
        return item["issue"].replace("|", "/")[:160]
    if item["outcome"] == "missed":
        parts = []
        for kind in item["expected"]:
            nearest = item["nearest"].get(kind)
            if kind != GENERIC and nearest:
                parts.append(
                    f"{nearest['profile']} {nearest['matched']}/{nearest['required']}, missing {', '.join(nearest['missing'][:6])}"
                )
        return "; ".join(parts)
    return ""


def _rate(numerator: int, denominator: int) -> dict[str, Any]:
    return {"count": numerator, "of": denominator, "pct": round(numerator / denominator * 100, 1) if denominator else None}


def _format_rate(rate: dict[str, Any]) -> str:
    if rate["pct"] is None:
        return "n/a"
    return f"{rate['count']}/{rate['of']} ({rate['pct']}%)"


def _last_traceback_lines() -> str:
    lines = traceback.format_exc().strip().splitlines()
    location = next((line.strip() for line in reversed(lines) if line.strip().startswith("File ") and "/src/pipeline/" in line), "")
    return f"{lines[-1]} [{location}]" if location else lines[-1]


def _git_commit() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], cwd=PROJECT_ROOT, capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


if __name__ == "__main__":
    sys.exit(main())
