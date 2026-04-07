from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.common.config import PipelineConfig
from pipeline.common.contracts import PipelineContext, PipelineExecutionError

try:
    import chardet
except ImportError:  # pragma: no cover - optional fallback
    chardet = None


MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024


def run_ingestion(context: PipelineContext, _: PipelineConfig) -> None:
    if context.input_path is None:
        raise PipelineExecutionError(stage="ingestion", message="No input file path was provided.")

    path = context.input_path
    if not path.exists():
        raise PipelineExecutionError(stage="ingestion", message=f"Input file not found: {path}")
    if path.suffix.lower() != ".csv":
        raise PipelineExecutionError(stage="ingestion", message="Invalid file type. Only .csv files are supported.")

    file_size = path.stat().st_size
    if file_size > MAX_FILE_SIZE_BYTES:
        raise PipelineExecutionError(stage="ingestion", message="File exceeds 50MB limit.")
    if _looks_binary(path):
        raise PipelineExecutionError(stage="ingestion", message="Invalid file type. The file does not appear to be text CSV data.")

    encoding_info = _detect_encoding(path)
    encoding = encoding_info["encoding"]
    if encoding_info["fallback_used"]:
        context.add_warning("Encoding detected as latin-1, converted to UTF-8.")

    delimiter = _sniff_delimiter(path, encoding)
    nonempty_line_count = _count_nonempty_lines(path, encoding)

    try:
        df = pd.read_csv(
            path,
            encoding=encoding,
            sep=delimiter,
            engine="python",
            on_bad_lines="skip",
        )
    except pd.errors.EmptyDataError as exc:
        raise PipelineExecutionError(stage="ingestion", message="File contains no data rows.") from exc
    except UnicodeDecodeError as exc:
        raise PipelineExecutionError(
            stage="ingestion",
            message="The file encoding could not be decoded safely.",
        ) from exc

    if nonempty_line_count <= 1 or df.empty:
        raise PipelineExecutionError(stage="ingestion", message="File contains no data rows.")

    if len(df.columns) == 1:
        context.add_warning("Single-column file: limited insights available.")

    approximate_skipped_rows = max(0, (nonempty_line_count - 1) - len(df))
    if approximate_skipped_rows > 0:
        context.add_warning(f"Approx. {approximate_skipped_rows} malformed or blank rows were skipped during load.")

    context.raw_df = df
    context.metadata["ingestion"] = {
        "file_size_bytes": file_size,
        "encoding": encoding,
        "encoding_confidence": encoding_info["confidence"],
        "delimiter": delimiter,
        "row_count": int(df.shape[0]),
        "column_count": int(df.shape[1]),
        "approx_skipped_rows": approximate_skipped_rows,
    }


def _looks_binary(path: Path) -> bool:
    sample = path.read_bytes()[:1024]
    return b"\x00" in sample


def _detect_encoding(path: Path) -> dict[str, Any]:
    raw = path.read_bytes()[:10000]
    if chardet is None:
        return {"encoding": "utf-8", "confidence": 1.0, "fallback_used": False}

    detected = chardet.detect(raw)
    encoding = detected.get("encoding") or "utf-8"
    confidence = float(detected.get("confidence") or 0.0)

    fallback_used = False
    if confidence < 0.8:
        encoding = "latin-1"
        fallback_used = True

    return {
        "encoding": encoding,
        "confidence": confidence,
        "fallback_used": fallback_used,
    }


def _sniff_delimiter(path: Path, encoding: str) -> str:
    sample = path.read_text(encoding=encoding, errors="replace")[:10000]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except csv.Error:
        return ","


def _count_nonempty_lines(path: Path, encoding: str) -> int:
    text = path.read_text(encoding=encoding, errors="replace")
    return sum(1 for line in text.splitlines() if line.strip())
