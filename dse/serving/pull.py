from __future__ import annotations

import argparse
import filecmp
import os
import re
import shutil
import subprocess
import urllib.request
import uuid
from pathlib import Path
from typing import Callable

from dse.serving.store import ExportError, load_version


def _ls(path: str, run: Callable) -> list[tuple[str, str]]:
    result = run(["hdfs", "dfs", "-ls", path], check=True, capture_output=True, text=True)
    entries = []
    for line in result.stdout.splitlines():
        if not line.strip() or line.startswith("Found "):
            continue
        fields = line.split(maxsplit=7)
        if len(fields) != 8:
            raise ValueError(f"invalid HDFS listing: {line}")
        entries.append((fields[0], fields[7].rsplit("/", 1)[-1]))
    return entries


def _remote_path(root: str, name: str) -> str:
    return root.rstrip("/") + "/" + name


def _remove_incoming(path: Path) -> None:
    if path.is_symlink() or not path.is_dir():
        path.unlink(missing_ok=True)
    else:
        shutil.rmtree(path)


def _same_contents(first: Path, second: Path) -> bool:
    first_entries = {p.relative_to(first): p for p in first.rglob("*")}
    second_entries = {p.relative_to(second): p for p in second.rglob("*")}
    if first_entries.keys() != second_entries.keys():
        return False
    for name, left in first_entries.items():
        right = second_entries[name]
        if left.is_symlink() or right.is_symlink():
            return False
        if left.is_dir() and right.is_dir():
            continue
        if not left.is_file() or not right.is_file() or not filecmp.cmp(left, right, shallow=False):
            return False
    return True


def pull_latest(
    export_root: str, serving_root: Path, run: Callable = subprocess.run
) -> int | None:
    candidates = []
    for mode, name in _ls(export_root, run):
        match = re.fullmatch(r"v(0|[1-9][0-9]*)", name)
        if mode.startswith("d") and match:
            candidates.append((int(match.group(1)), name))

    selected = None
    for version, name in sorted(candidates, reverse=True):
        remote = _remote_path(export_root, name)
        if any(mode.startswith("-") and entry == "manifest.json" for mode, entry in _ls(remote, run)):
            selected = (version, name, remote)
            break
    if selected is None:
        return None

    version, name, remote = selected
    destination = serving_root / name
    current = serving_root / "current"
    if (
        current.is_symlink()
        and destination.is_dir()
        and not destination.is_symlink()
        and current.resolve() == destination.resolve()
    ):
        return None

    serving_root.mkdir(parents=True, exist_ok=True)
    incoming = serving_root / f".incoming-{name}"
    _remove_incoming(incoming)
    try:
        run(["hdfs", "dfs", "-get", remote, str(incoming)], check=True, capture_output=True, text=True)
        loaded = load_version(incoming)
        if loaded.version != version:
            raise ExportError(f"manifest version does not match {name}")

        if destination.exists() or destination.is_symlink():
            if destination.is_symlink() or not destination.is_dir():
                raise ExportError(f"invalid existing version directory: {destination}")
            existing = load_version(destination)
            if existing.version != version or not _same_contents(incoming, destination):
                raise ExportError(f"existing version differs from export: {name}")
        else:
            incoming.rename(destination)

        temporary_link = serving_root / f".current-{uuid.uuid4().hex}"
        try:
            temporary_link.symlink_to(name)
            os.replace(temporary_link, current)
        finally:
            temporary_link.unlink(missing_ok=True)
    finally:
        _remove_incoming(incoming)
    return version


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull the latest exported search version")
    parser.add_argument("--export-root", required=True)
    parser.add_argument("--serving-root", required=True, type=Path)
    parser.add_argument("--reload-url")
    args = parser.parse_args()

    pull_latest(args.export_root, args.serving_root)
    if args.reload_url:
        request = urllib.request.Request(args.reload_url, method="POST")
        with urllib.request.urlopen(request, timeout=10):
            pass


if __name__ == "__main__":
    main()
