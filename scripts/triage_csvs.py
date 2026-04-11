from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from pipeline.business.router import build_dashboard, build_insight_candidates, detect_business_context
from pipeline.run import run_pipeline


def main() -> int:
    parser = argparse.ArgumentParser(description="Triage CSV files through Loom's template pipeline.")
    parser.add_argument("root", nargs="?", default="/Users/aagam/Kaggle", help="CSV file or directory to scan.")
    parser.add_argument("--json-out", help="Optional path for a JSON report.")
    parser.add_argument("--csv-out", help="Optional path for a CSV report.")
    args = parser.parse_args()

    root = Path(args.root).expanduser()
    if root.is_file():
        files = [root]
    else:
        files = sorted(root.rglob("*.csv"))

    rows = [_triage_file(path) for path in files]
    _print_table(rows)

    if args.json_out:
        Path(args.json_out).write_text(json.dumps(rows, indent=2), encoding="utf-8")
    if args.csv_out:
        _write_csv(Path(args.csv_out), rows)

    failures = [row for row in rows if row["status"] == "fail"]
    return 1 if failures else 0


def _triage_file(path: Path) -> dict[str, Any]:
    row: dict[str, Any] = {
        "path": str(path),
        "file": path.name,
        "size_mb": round(path.stat().st_size / (1024 * 1024), 2),
        "status": "unknown",
        "kind": None,
        "profile": None,
        "pipeline_errors": 0,
        "pipeline_warnings": 0,
        "insight_count": 0,
        "section_count": 0,
        "issue": "",
    }
    try:
        context = run_pipeline(input_path=path, persist_outputs=False, include_visualizations=False)
        row["pipeline_errors"] = len(context.errors)
        row["pipeline_warnings"] = len(context.warnings)
        if context.errors:
            row["status"] = "fail"
            row["issue"] = "; ".join(context.errors)
            return row

        detected = detect_business_context(context)
        if detected is None:
            row["status"] = "generic"
            row["issue"] = "No specialized template detected."
            return row

        kind = detected["kind"]
        analysis = detected["analysis"]
        insights = build_insight_candidates(kind, analysis, "focus on the most actionable risks")
        approved_ids = [item["id"] for item in insights.get("insights", [])[:4]]
        dashboard = build_dashboard(
            kind,
            analysis,
            approved_insight_ids=approved_ids,
            settings={"metric_count": 4, "show_notes": True},
        )
        sections = dashboard.get("blueprint", {}).get("layout_sections", []) if dashboard else []

        row["status"] = "ok"
        row["kind"] = kind
        row["profile"] = analysis.get("profile")
        row["insight_count"] = len(insights.get("insights", []))
        row["section_count"] = len(sections)
        if row["insight_count"] < 3:
            row["status"] = "thin"
            row["issue"] = "Specialized template works, but insight output is sparse."
        if row["section_count"] < 3:
            row["status"] = "thin"
            row["issue"] = "Specialized template works, but dashboard sections are sparse."
        return row
    except Exception as exc:
        row["status"] = "fail"
        row["issue"] = f"{type(exc).__name__}: {exc}"
        return row


def _print_table(rows: list[dict[str, Any]]) -> None:
    print("status | file | kind/profile | insights | sections | issue")
    print("-" * 96)
    for row in rows:
        profile = row["profile"] if row["profile"] is not None else "-"
        kind_profile = f"{row['kind'] or '-'} / {profile}"
        print(
            f"{row['status']:7} | {row['file']} | {kind_profile} | "
            f"{row['insight_count']:>2} | {row['section_count']:>2} | {row['issue']}"
        )


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    raise SystemExit(main())
