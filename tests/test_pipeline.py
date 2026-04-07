from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from pipeline.cleaning.cleaning import run_cleaning
from pipeline.common.config import PipelineConfig
from pipeline.common.contracts import PipelineContext, PipelineExecutionError
from pipeline.ingestion.ingestion import run_ingestion
from pipeline.run import run_pipeline
from pipeline.validation.validation import run_validation


class PipelineTests(unittest.TestCase):
    fixtures_dir = PROJECT_ROOT / "tests" / "fixtures"

    def test_ingestion_rejects_non_csv_extension(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "bad.txt"
            path.write_text("not,a,csv\n1,2,3\n")
            context = PipelineContext(input_path=path, output_dir=Path(tmp_dir))
            with self.assertRaises(PipelineExecutionError):
                run_ingestion(context, PipelineConfig())

    def test_validation_flags_outliers_and_duplicates(self) -> None:
        context = self._context_for_fixture("duplicates.csv")
        run_ingestion(context, PipelineConfig())
        run_validation(context, PipelineConfig())

        self.assertGreater(context.quality_report["duplicates"]["count"], 0)
        self.assertIn("Revenue", context.schema)

    def test_cleaning_is_idempotent(self) -> None:
        config = PipelineConfig()
        context = self._context_for_fixture("missing_values.csv")
        run_ingestion(context, config)
        run_validation(context, config)
        run_cleaning(context, config)
        first_clean = context.clean_df.copy(deep=True)

        rerun_context = PipelineContext(
            input_path=context.input_path,
            output_dir=context.output_dir,
            raw_df=first_clean.copy(deep=True),
            schema=context.schema,
        )
        run_cleaning(rerun_context, config)

        self.assertTrue(first_clean.equals(rerun_context.clean_df))

    def test_run_pipeline_generates_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir) / "artifacts"
            context = run_pipeline(
                input_path=self.fixtures_dir / "normal.csv",
                output_dir=output_dir,
            )

            self.assertFalse(context.errors)
            self.assertTrue((output_dir / "report.json").exists())
            self.assertTrue((output_dir / "summary.md").exists())
            self.assertTrue((output_dir / "summary.html").exists())

            report = json.loads((output_dir / "report.json").read_text())
            self.assertIn("quality_report", report)
            self.assertIn("analysis_results", report)

    def test_validate_only_skips_downstream_stages(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir) / "validation_only"
            context = run_pipeline(
                input_path=self.fixtures_dir / "normal.csv",
                output_dir=output_dir,
                validate_only=True,
            )

            self.assertFalse(context.errors)
            self.assertEqual(context.analysis_results, {})
            self.assertEqual(context.charts, [])
            self.assertIn("Validation-only mode enabled", " ".join(context.warnings))

    def test_run_pipeline_supports_in_memory_mode(self) -> None:
        context = run_pipeline(
            input_path=self.fixtures_dir / "normal.csv",
            output_dir=None,
            persist_outputs=False,
            include_visualizations=False,
        )

        self.assertFalse(context.errors)
        self.assertEqual(context.metadata["artifacts"], {})
        self.assertEqual(context.charts, [])

    def test_empty_fixture_returns_structured_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir) / "empty_case"
            context = run_pipeline(
                input_path=self.fixtures_dir / "empty.csv",
                output_dir=output_dir,
            )

            self.assertTrue(context.errors)
            self.assertEqual(context.errors[0]["stage"], "ingestion")

    def _context_for_fixture(self, name: str) -> PipelineContext:
        return PipelineContext(
            input_path=self.fixtures_dir / name,
            output_dir=PROJECT_ROOT / "output" / "test-artifacts",
        )


if __name__ == "__main__":
    unittest.main()
