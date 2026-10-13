from __future__ import annotations

import logging
import os
import shutil
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
    explain_detection,
    section_options,
    template_catalog,
)
from pipeline.common.contracts import PipelineContext
from pipeline.common.reporting import build_report_dict
from pipeline.run import run_pipeline

logger = logging.getLogger(__name__)

DEFAULT_CORS_ORIGINS = ["http://localhost:5173", "http://127.0.0.1:5173"]

# Analysis needs many times the file's size in memory. A small host should set this well
# below the pipeline's own 50 MB ceiling, so a large upload is refused instead of killing the process.
MAX_UPLOAD_MB = float(os.getenv("APP_MAX_UPLOAD_MB", "50"))


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

    if file.size is not None and file.size > MAX_UPLOAD_MB * 1024 * 1024:
        raise HTTPException(
            status_code=413,
            detail=f"This file is {file.size / (1024 * 1024):.0f} MB. The limit is {MAX_UPLOAD_MB:.0f} MB.",
        )

    with tempfile.TemporaryDirectory(prefix="loom-api-") as tmpdir:
        # The client-supplied filename is untrusted; never use it to build a path.
        input_path = Path(tmpdir) / "upload.csv"
        with input_path.open("wb") as handle:
            shutil.copyfileobj(file.file, handle)  # streamed, so the upload is not also held in memory

        context = run_pipeline(
            input_path=input_path,
            output_dir=None,
            config_path=None,
            validate_only=validate_only,
            persist_outputs=False,
            include_visualizations=False,
        )

    if context.raw_df is None:
        # Nothing was loaded, so there is nothing to analyze. Later-stage failures keep going:
        # templates can still work from whatever the pipeline did produce.
        logger.warning("Upload could not be ingested: %s", context.errors)
        message = next(
            (error["message"] for error in context.errors if error.get("type") != "unhandled_exception"),
            "Loom could not read this file as CSV data.",
        )
        raise HTTPException(status_code=422, detail=message)

    detected = detect_business_context(context)
    candidates = explain_detection(list(context.raw_df.columns))
    business_context = None

    if template_override:
        business_context = analyze_for_kind(context, template_override)
        if business_context is None and template_override != "generic":
            raise HTTPException(status_code=400, detail=_override_failure(template_override, candidates))
    else:
        business_context = detected

    # Every readable CSV gets a dashboard: no specialized match, or a template that cannot
    # describe this dataset, falls back to the generic template instead of a dead end.
    review = _insights_or_none(context, business_context)
    if review is None:
        if business_context is not None and detected is not None and detected["kind"] == business_context["kind"]:
            detected = None
        if business_context is None or business_context["kind"] != "generic":
            business_context = analyze_for_kind(context, "generic")
            review = _insights_or_none(context, business_context)
        if review is None:
            business_context = None
            review = {"insights": [], "focus_tags": []}

    return jsonable_encoder(
        {
            "report": build_report_dict(context),
            "detected_template": detected,
            "business_context": business_context,
            "review": review,
            "detection": {"candidates": candidates},
            "template_options": template_catalog(),
        }
    )


def _override_failure(kind: str, candidates: list[dict[str, Any]]) -> str:
    message = f"Template override '{kind}' did not match this dataset."
    candidate = next((item for item in candidates if item["kind"] == kind), None)
    if candidate is None:
        return message
    if candidate["missing"]:
        return f"{message} The closest shape it accepts is missing: {', '.join(candidate['missing'])}."
    return f"{message} The columns are there, but too few rows have usable values."


def _insights_or_none(context: PipelineContext, business_context: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if business_context is None:
        return None
    try:
        return build_insight_candidates(business_context["kind"], business_context["analysis"], "")
    except Exception as exc:
        logger.exception("Template %s failed while building insights", business_context["kind"])
        context.metadata.setdefault("template_errors", []).append(
            {"kind": business_context["kind"], "stage": "insights", "error": f"{type(exc).__name__}: {exc}"}
        )
        context.add_warning(f"The {business_context['display_name']} template could not be applied to this file.")
        return None


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
