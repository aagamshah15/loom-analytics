from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any, Callable, List, Optional, Union

from pipeline.analysis.analysis import run_analysis
from pipeline.cleaning.cleaning import run_cleaning
from pipeline.common.config import PipelineConfig, load_pipeline_config
from pipeline.common.contracts import PipelineContext, PipelineExecutionError
from pipeline.common.logging_utils import configure_logger
from pipeline.common.reporting import write_reports
from pipeline.ingestion.ingestion import run_ingestion
from pipeline.insights.insights import generate_insights
from pipeline.validation.validation import run_validation
from pipeline.visualization.visualization import generate_visualizations


def run_pipeline(
    *,
    input_path: Union[str, Path],
    output_dir: Optional[Union[str, Path]] = None,
    config_path: Optional[Union[str, Path]] = None,
    validate_only: bool = False,
    persist_outputs: bool = True,
    include_visualizations: bool = True,
) -> PipelineContext:
    if persist_outputs and output_dir is None:
        raise ValueError("output_dir is required when persist_outputs=True")

    context = PipelineContext(
        input_path=Path(input_path),
        output_dir=Path(output_dir) if output_dir is not None else None,
        metadata={"validate_only": validate_only},
    )
    config = load_pipeline_config(config_path)
    context.metadata["config"] = config.as_dict()
    context.metadata["persist_outputs"] = persist_outputs
    context.metadata["generate_visualizations"] = include_visualizations
    logger = configure_logger(context.run_id, context.output_dir if persist_outputs else None)
    context.metadata["started_at_epoch"] = time.time()

    logger.info("Pipeline started", stage="pipeline_start", input_path=str(context.input_path))

    try:
        _run_stage("ingestion", context, logger, run_ingestion, config)
        _run_stage("validation", context, logger, run_validation, config)

        if validate_only:
            context.add_warning("Validation-only mode enabled. Cleaning, analysis, visualization, and insights were skipped.")
        else:
            _run_stage("cleaning", context, logger, run_cleaning, config)
            _run_stage("analysis", context, logger, run_analysis, config)
            if include_visualizations:
                _run_stage("visualization", context, logger, generate_visualizations, config)
            _run_stage("insights", context, logger, generate_insights, config)
    except PipelineExecutionError as exc:
        context.add_error(stage=exc.stage, message=exc.message, details=exc.details)
        logger.warning("Handled pipeline exception", stage=exc.stage, details=exc.details, message_text=exc.message)
    except Exception as exc:  # pragma: no cover - safety net
        context.add_error(stage="pipeline", message=str(exc), error_type="unhandled_exception")
        logger.error("Unhandled exception", stage="pipeline", message_text=str(exc))
    finally:
        context.metadata["finished_at_epoch"] = time.time()
        context.metadata["artifacts"] = write_reports(context) if persist_outputs else {}
        logger.info("Pipeline finished", stage="pipeline_end", errors=len(context.errors), warnings=len(context.warnings))

    return context


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the CSV analytics pipeline.")
    parser.add_argument("--input", required=True, help="Path to the CSV file.")
    parser.add_argument("--output", required=True, help="Directory for generated artifacts.")
    parser.add_argument("--config", help="Optional YAML config path.")
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Run ingestion and validation only, then emit a report.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    context = run_pipeline(
        input_path=args.input,
        output_dir=args.output,
        config_path=args.config,
        validate_only=args.validate_only,
    )
    return 1 if context.errors else 0


def _run_stage(
    stage_name: str,
    context: PipelineContext,
    logger: Any,
    func: Callable[[PipelineContext, PipelineConfig], Any],
    config: PipelineConfig,
) -> None:
    started = time.perf_counter()
    logger.info("Stage started", stage=stage_name)
    func(context, config)
    duration_ms = round((time.perf_counter() - started) * 1000, 2)
    stage_timings = context.metadata.setdefault("stage_timings_ms", {})
    stage_timings[stage_name] = duration_ms
    logger.info("Stage finished", stage=stage_name, duration_ms=duration_ms)


if __name__ == "__main__":
    sys.exit(main())
