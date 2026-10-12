from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = PROJECT_ROOT / "tests" / "corpus" / "manifest.yaml"

# Same ceiling as the pipeline's ingestion stage; anything larger could never be analyzed.
MAX_FILE_BYTES = 50 * 1024 * 1024


def corpus_dir() -> Path:
    """Where corpus files live. Outside the repo so worktrees and clones can share one copy."""
    configured = os.getenv("LOOM_CORPUS_DIR")
    return Path(configured).expanduser() if configured else Path.home() / ".cache" / "loom" / "corpus"


def load_manifest(path: Path = DEFAULT_MANIFEST) -> list[dict[str, Any]]:
    entries = yaml.safe_load(path.read_text())["entries"]
    seen: set[str] = set()
    for entry in entries:
        if entry["id"] in seen:
            raise ValueError(f"Duplicate corpus id: {entry['id']}")
        seen.add(entry["id"])
    return entries


def entry_path(root: Path, entry: dict[str, Any]) -> Path:
    """Each entry gets its own directory, so a downloaded file never lands next to another's."""
    if entry["source"] == "kaggle":
        filename = Path(entry["file"]).name
    else:
        filename = Path(urlparse(entry["ref"]).path).name
    return root / entry["id"] / filename


def main() -> int:
    parser = argparse.ArgumentParser(
        description="List or download the Loom detection corpus. Nothing is downloaded without --yes."
    )
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--source", choices=["url", "kaggle", "all"], default="all")
    parser.add_argument("--only", nargs="*", default=[], help="Limit to these corpus ids.")
    parser.add_argument("--yes", action="store_true", help="Download the files that are missing.")
    args = parser.parse_args()

    root = corpus_dir()
    entries = [
        entry
        for entry in load_manifest(args.manifest)
        if (args.source == "all" or entry["source"] == args.source) and (not args.only or entry["id"] in args.only)
    ]
    missing = [entry for entry in entries if not entry_path(root, entry).exists()]

    _print_plan(root, entries, missing)
    if not args.yes:
        if missing:
            print("\nNothing was downloaded. Re-run with --yes to fetch the missing files.")
        return 0

    failures = 0
    for entry in missing:
        target = entry_path(root, entry)
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            if entry["source"] == "kaggle":
                _fetch_kaggle(entry, target)
            else:
                _fetch_url(entry["ref"], target)
            print(f"  fetched  {entry['id']}  ({target.stat().st_size / 1024:.0f} KB)")
        except FetchError as exc:
            failures += 1
            target.unlink(missing_ok=True)
            print(f"  FAILED   {entry['id']}: {exc}")

    print(f"\n{len(missing) - failures} fetched, {failures} failed. Corpus directory: {root}")
    return 1 if failures else 0


class FetchError(Exception):
    pass


def _print_plan(root: Path, entries: list[dict[str, Any]], missing: list[dict[str, Any]]) -> None:
    missing_ids = {entry["id"] for entry in missing}
    print(f"Corpus directory: {root}")
    print(f"{'status':8} {'source':7} {'size':>9}  id  <-  ref")
    for entry in entries:
        size = entry.get("size_bytes")
        size_text = f"{size / 1024:.0f} KB" if size else "unknown"
        status = "missing" if entry["id"] in missing_ids else "present"
        ref = entry["ref"] if entry["source"] == "url" else f"{entry['ref']} :: {entry['file']}"
        print(f"{status:8} {entry['source']:7} {size_text:>9}  {entry['id']}  <-  {ref}")

    known_bytes = sum(entry.get("size_bytes") or 0 for entry in missing)
    unknown = sum(1 for entry in missing if not entry.get("size_bytes"))
    print(
        f"\n{len(entries)} entries, {len(missing)} missing: "
        f"{known_bytes / 1048576:.1f} MB of known size plus {unknown} of unknown size "
        f"(each capped at {MAX_FILE_BYTES // 1048576} MB)."
    )
    if any(entry["source"] == "kaggle" for entry in missing) and _kaggle_cli() is None:
        print(
            "Kaggle entries need the Kaggle CLI and your own API token:\n"
            "  pip install kaggle, then follow https://www.kaggle.com/docs/api to place the token."
        )


def _fetch_url(url: str, target: Path) -> None:
    request = Request(url, headers={"User-Agent": "loom-corpus-fetch"})
    partial = target.with_name(target.name + ".part")
    try:
        with urlopen(request, timeout=60) as response, partial.open("wb") as handle:
            written = 0
            while True:
                chunk = response.read(1024 * 256)
                if not chunk:
                    break
                written += len(chunk)
                if written > MAX_FILE_BYTES:
                    raise FetchError(f"larger than {MAX_FILE_BYTES // 1048576} MB")
                handle.write(chunk)
    except (HTTPError, URLError, TimeoutError) as exc:
        partial.unlink(missing_ok=True)
        raise FetchError(str(exc)) from exc
    except FetchError:
        partial.unlink(missing_ok=True)
        raise
    partial.replace(target)


def _fetch_kaggle(entry: dict[str, Any], target: Path) -> None:
    cli = _kaggle_cli()
    if cli is None:
        raise FetchError("Kaggle CLI is not installed")

    # The CLI reads the user's own token; this script never opens or prints it.
    result = subprocess.run(
        [cli, "datasets", "download", "-d", entry["ref"], "-f", entry["file"], "-p", str(target.parent), "--force"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip().splitlines()
        raise FetchError(detail[-1] if detail else "kaggle CLI failed")

    if not target.exists():
        archive = _find_archive(target.parent)
        if archive is None:
            raise FetchError("the file was not in the download")
        _extract_member(archive, target)
        archive.unlink()
    if not _within_limit(target):
        raise FetchError(f"larger than {MAX_FILE_BYTES // 1048576} MB")


def _kaggle_cli() -> Optional[str]:
    """Prefer the CLI installed next to this interpreter; an older one on PATH may not read newer tokens."""
    beside_interpreter = Path(sys.executable).parent / "kaggle"
    if beside_interpreter.exists():
        return str(beside_interpreter)
    return shutil.which("kaggle")


def _find_archive(directory: Path) -> Optional[Path]:
    archives = sorted(directory.glob("*.zip"))
    return archives[0] if archives else None


def _extract_member(archive: Path, target: Path) -> None:
    """Pull out only the wanted file, by name, so nothing in the archive chooses where it is written."""
    with zipfile.ZipFile(archive) as bundle:
        member = next((name for name in bundle.namelist() if Path(name).name == target.name), None)
        if member is None:
            raise FetchError("the file was not in the downloaded archive")
        if bundle.getinfo(member).file_size > MAX_FILE_BYTES:
            raise FetchError(f"larger than {MAX_FILE_BYTES // 1048576} MB")
        with bundle.open(member) as source, target.open("wb") as handle:
            shutil.copyfileobj(source, handle)


def _within_limit(path: Path) -> bool:
    return path.stat().st_size <= MAX_FILE_BYTES


if __name__ == "__main__":
    sys.exit(main())
