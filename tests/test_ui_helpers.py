from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from pipeline.ui.helpers import create_ephemeral_workspace, persist_uploaded_file, sanitize_filename


class UiHelperTests(unittest.TestCase):
    def test_sanitize_filename_replaces_unsafe_characters(self) -> None:
        self.assertEqual(sanitize_filename("sales report (final).csv"), "sales_report_final_.csv")

    def test_persist_uploaded_file_writes_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = persist_uploaded_file(Path(tmp_dir), "input csv.csv", b"a,b\n1,2\n")
            self.assertTrue(target.exists())
            self.assertEqual(target.read_text(), "a,b\n1,2\n")
            self.assertEqual(target.name, "input_csv.csv")

    def test_create_ephemeral_workspace_returns_temp_directory(self) -> None:
        workspace = create_ephemeral_workspace()
        try:
            self.assertTrue(Path(workspace.name).exists())
        finally:
            workspace.cleanup()


if __name__ == "__main__":
    unittest.main()
