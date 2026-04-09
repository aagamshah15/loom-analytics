from __future__ import annotations

import json
import mimetypes
import os
import re
import socket
import sys
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURES_ROOT = PROJECT_ROOT / "tests" / "fixtures" / "stress"

FRONTEND_URL = os.getenv("LOOM_FRONTEND_URL", "https://loom-analytics.netlify.app").rstrip("/")
BACKEND_URL = os.getenv("LOOM_BACKEND_URL", "https://loom-analytics.onrender.com").rstrip("/")

FIXTURE_MAP = {
    "financial_timeseries": FIXTURES_ROOT / "financial" / "happy_path.csv",
    "ecommerce_orders": FIXTURES_ROOT / "ecommerce" / "happy_path.csv",
    "healthcare_medical": FIXTURES_ROOT / "healthcare" / "happy_path.csv",
    "hr_workforce": FIXTURES_ROOT / "hr" / "happy_path.csv",
}

SECTION_MAP = {
    "financial_timeseries": ["overview", "seasonality", "volatility", "gaps", "volume", "data_notes"],
    "ecommerce_orders": ["overview", "revenue", "returns", "channels", "discounts", "notes"],
    "healthcare_medical": ["overview", "adherence", "care_delivery", "equity", "costs", "notes"],
    "hr_workforce": ["overview", "retention", "compensation", "development", "workforce_model", "notes"],
}


def main() -> int:
    print("Loom hosted smoke check")
    print(f"- frontend: {FRONTEND_URL}")
    print(f"- backend: {BACKEND_URL}")

    health = get_json(f"{BACKEND_URL}/api/health")
    assert_equal(health.get("status"), "ok", "Backend health check must return ok")
    print("  ok health endpoint")

    assert_frontend_bundle_targets_backend()
    print("  ok frontend bundle points at Render backend")

    for kind, fixture_path in FIXTURE_MAP.items():
        print(f"  checking {kind} with {fixture_path.name}")
        analyze = post_csv(f"{BACKEND_URL}/api/analyze", fixture_path)
        context = analyze.get("business_context")
        assert_true(context is not None, f"{kind} should return a business context")
        assert_equal(context["kind"], kind, f"{fixture_path.name} should detect {kind}")

        approved_ids = [item["id"] for item in analyze["review"]["insights"][:3]]
        dashboard = post_json(
            f"{BACKEND_URL}/api/build-dashboard",
            {
                "kind": kind,
                "analysis": context["analysis"],
                "approved_insight_ids": approved_ids,
                "settings": {
                    "title": f"{kind} smoke dashboard",
                    "subtitle": "Hosted smoke test",
                    "included_sections": SECTION_MAP[kind],
                    "metric_count": 3,
                    "show_notes": True,
                },
            },
        )
        assert_true(bool(dashboard.get("html")), f"{kind} should return HTML")
        assert_true(bool(dashboard.get("blueprint", {}).get("layout_sections")), f"{kind} should return layout sections")
        print(f"    ok analyze + build for {kind}")

    print("Hosted smoke checks passed.")
    return 0


def assert_frontend_bundle_targets_backend() -> None:
    html = get_text(FRONTEND_URL)
    script_paths = re.findall(r'<script[^>]+src="([^"]+assets/[^"]+\.js)"', html)
    assert_true(bool(script_paths), "Frontend HTML should reference a JS bundle")

    for script_path in script_paths:
        bundle = get_text(urljoin(FRONTEND_URL, script_path))
        if BACKEND_URL in bundle:
            return

    raise AssertionError("Frontend bundle did not contain the configured backend URL")


def get_json(url: str) -> dict[str, Any]:
    request = Request(url, method="GET")
    return json.loads(read_response(request))


def get_text(url: str) -> str:
    request = Request(url, method="GET")
    return read_response(request)


def post_json(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    request = Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    return json.loads(read_response(request))


def post_csv(url: str, fixture_path: Path) -> dict[str, Any]:
    boundary = f"----loom-{uuid.uuid4().hex}"
    mime_type = mimetypes.guess_type(fixture_path.name)[0] or "text/csv"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{fixture_path.name}"\r\n'
        f"Content-Type: {mime_type}\r\n\r\n"
    ).encode("utf-8") + fixture_path.read_bytes() + f"\r\n--{boundary}--\r\n".encode("utf-8")

    request = Request(
        url,
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )
    return json.loads(read_response(request))


def read_response(request: Request) -> str:
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            with urlopen(request, timeout=90) as response:
                return response.read().decode("utf-8")
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise AssertionError(f"{request.full_url} failed with {exc.code}: {body}") from exc
        except (URLError, socket.timeout) as exc:
            last_error = exc
            if attempt == 2:
                break
            time.sleep(5 * (attempt + 1))

    raise AssertionError(f"{request.full_url} failed: {last_error}")


def assert_equal(left: Any, right: Any, message: str) -> None:
    if left != right:
        raise AssertionError(f"{message}. Expected {right!r}, got {left!r}")


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as exc:
        print(f"Smoke check failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
