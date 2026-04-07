from __future__ import annotations

import re
from pathlib import Path
from tempfile import TemporaryDirectory


def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name.strip())
    return cleaned or "upload.csv"


def persist_uploaded_file(target_dir: Path, filename: str, data: bytes) -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / sanitize_filename(filename)
    path.write_bytes(data)
    return path


def create_ephemeral_workspace() -> TemporaryDirectory:
    return TemporaryDirectory(prefix="loom-ui-")
