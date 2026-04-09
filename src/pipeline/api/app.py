from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware

from pipeline.business.router import (
    analyze_for_kind,
    build_dashboard,
    build_insight_candidates,
    default_dashboard_title,
    detect_business_context,
    section_options,
    template_catalog,
)
from pipeline.common.reporting import build_report_dict
from pipeline.run import run_pipeline

DEFAULT_CORS_ORIGINS = ["http://localhost:5173", "http://127.0.0.1:5173"]


def _cors_origins() -> list[str]:
    configured = os.getenv("APP_CORS_ORIGINS", "")
    if not configured.strip():
        return DEFAULT_CORS_ORIGINS
    return [origin.strip() for origin in configured.split(",") if origin.strip()]


app = FastAPI(title="Loom API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins(),
    allow_origin_regex=os.getenv("APP_CORS_ORIGIN_REGEX"),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/templates")
def get_templates() -> dict[str, Any]:
    return {"templates": template_catalog()}


@app.post("/api/analyze")
async def analyze_csv(
    file: UploadFile = File(...),
    validate_only: bool = Form(False),
    template_override: Optional[str] = Form(None),
) -> dict[str, Any]:
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a CSV file.")

    with tempfile.TemporaryDirectory(prefix="loom-api-") as tmpdir:
        input_path = Path(tmpdir) / file.filename
        input_path.write_bytes(await file.read())

        context = run_pipeline(
            input_path=input_path,
            output_dir=None,
            config_path=None,
            validate_only=validate_only,
            persist_outputs=False,
            include_visualizations=False,
        )

    report = build_report_dict(context)
    detected = detect_business_context(context)
    business_context = None

    if template_override:
        business_context = analyze_for_kind(context, template_override)
        if business_context is None and template_override != "generic":
            raise HTTPException(status_code=400, detail=f"Template override '{template_override}' did not match this dataset.")
    else:
        business_context = detected

    if business_context is not None and business_context["kind"] != "generic":
        review = build_insight_candidates(business_context["kind"], business_context["analysis"], "")
    else:
        review = {"insights": [], "focus_tags": []}

    return jsonable_encoder(
        {
            "report": report,
            "detected_template": detected,
            "business_context": business_context,
            "review": review,
            "template_options": template_catalog(),
        }
    )


@app.post("/api/review")
def regenerate_review(payload: dict[str, Any]) -> dict[str, Any]:
    kind = payload.get("kind")
    analysis = payload.get("analysis")
    user_prompt = payload.get("user_prompt", "")
    if not kind or analysis is None:
        raise HTTPException(status_code=400, detail="kind and analysis are required.")

    try:
        review = build_insight_candidates(kind, analysis, user_prompt)
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid analysis payload for template '{kind}'.") from exc
    return jsonable_encoder(review)


@app.post("/api/build-dashboard")
def build_dashboard_endpoint(payload: dict[str, Any]) -> dict[str, Any]:
    kind = payload.get("kind")
    analysis = payload.get("analysis")
    approved_insight_ids = payload.get("approved_insight_ids", [])
    user_prompt = payload.get("user_prompt", "")
    settings = payload.get("settings", {})

    if not kind or analysis is None:
        raise HTTPException(status_code=400, detail="kind and analysis are required.")

    try:
        dashboard = build_dashboard(
            kind=kind,
            analysis=analysis,
            approved_insight_ids=approved_insight_ids,
            user_prompt=user_prompt,
            settings=settings,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid analysis payload for template '{kind}'.") from exc
    if dashboard is None:
        raise HTTPException(status_code=400, detail="Unable to build dashboard for this template.")

    return jsonable_encoder(dashboard)


@app.get("/api/template-meta/{kind}")
def get_template_meta(kind: str) -> dict[str, Any]:
    options = section_options(kind)
    if not options and kind != "generic":
        raise HTTPException(status_code=404, detail="Unknown template kind.")

    return {
        "kind": kind,
        "default_dashboard_title": default_dashboard_title(kind),
        "sections": options,
    }
