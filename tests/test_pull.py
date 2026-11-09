import json
import shutil
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dse.serving import pull
from dse.serving.store import ExportError, load_version


EXPORT_ROOT = "hdfs://namenode:9000/export dir"


def write_export(root: Path, version: int) -> Path:
    directory = root / f"v{version}"
    (directory / "docs").mkdir(parents=True)
    (directory / "index").mkdir()
    pq.write_table(
        pa.Table.from_pylist(
            [{"doc_id": f"d{version}", "url": "https://example.com", "title": "Title",
              "length": 1, "pagerank": 1.0}]
        ),
        directory / "docs" / "part-000.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist([{"term": "word", "doc_id": f"d{version}", "tf": 1}]),
        directory / "index" / "part-000.parquet",
    )
    (directory / "manifest.json").write_text(
        json.dumps({"version": version, "docs": 1, "postings": 1}), encoding="utf-8"
    )
    return directory


class FakeHdfs:
    def __init__(self, root: Path):
        self.root = root
        self.calls = []
        self.fail_get = False

    def __call__(self, args, **kwargs):
        self.calls.append(args)
        assert args[:2] == ["hdfs", "dfs"]
        assert kwargs == {"check": True, "capture_output": True, "text": True}
        operation, remote = args[2:4]
        assert remote == EXPORT_ROOT or remote.startswith(EXPORT_ROOT + "/")
        local = self.root / remote[len(EXPORT_ROOT):].lstrip("/")
        if operation == "-ls":
            if not local.is_dir():
                raise subprocess.CalledProcessError(1, args, stderr="missing directory")
            entries = sorted(local.iterdir())
            output = f"Found {len(entries)} items\n"
            for entry in entries:
                kind = "d" if entry.is_dir() else "-"
                output += (
                    f"{kind}rwxr-xr-x   - owner group 0 2026-01-01 12:00 "
                    f"{remote.rstrip('/')}/{entry.name}\n"
                )
            return subprocess.CompletedProcess(args, 0, stdout=output, stderr="")
        assert operation == "-get"
        destination = Path(args[4])
        if self.fail_get:
            destination.mkdir()
            (destination / "partial").write_text("partial", encoding="utf-8")
            raise subprocess.CalledProcessError(1, args, stderr="copy failed")
        shutil.copytree(local, destination)
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")


def test_pulls_newest_complete_version_and_switches_current(tmp_path):
    exports = tmp_path / "exports"
    exports.mkdir()
    serving = tmp_path / "serving"
    old = write_export(serving, 1)
    (serving / "current").symlink_to(old.name)
    write_export(exports, 2)
    write_export(exports, 10)
    (exports / "v11").mkdir()
    (exports / "v12").write_text("not a directory", encoding="utf-8")
    (exports / "v13extra").mkdir()
    run = FakeHdfs(exports)

    assert pull.pull_latest(EXPORT_ROOT, serving, run) == 10
    assert (serving / "current").is_symlink()
    assert (serving / "current").resolve() == (serving / "v10").resolve()
    assert load_version(serving / "current").search("word")[0]["doc_id"] == "d10"
    assert old.is_dir()
    assert not (serving / ".incoming-v10").exists()
    assert [call[3] for call in run.calls if call[2] == "-get"] == [EXPORT_ROOT + "/v10"]


def test_corrupt_export_keeps_current_and_removes_incoming(tmp_path):
    exports = tmp_path / "exports"
    serving = tmp_path / "serving"
    old = write_export(serving, 1)
    (serving / "current").symlink_to(old.name)
    write_export(exports, 2)
    (exports / "v2" / "manifest.json").write_text(
        json.dumps({"version": 2, "docs": 5, "postings": 1}), encoding="utf-8"
    )

    with pytest.raises(ExportError):
        pull.pull_latest(EXPORT_ROOT, serving, FakeHdfs(exports))
    assert (serving / "current").resolve() == old.resolve()
    assert load_version(serving / "current").version == 1
    assert not (serving / ".incoming-v2").exists()
    assert not (serving / "v2").exists()


def test_already_served_version_is_noop(tmp_path):
    exports = tmp_path / "exports"
    serving = tmp_path / "serving"
    write_export(exports, 2)
    version = write_export(serving, 2)
    (serving / "current").symlink_to(version.name)
    run = FakeHdfs(exports)

    assert pull.pull_latest(EXPORT_ROOT, serving, run) is None
    assert (serving / "current").resolve() == version.resolve()
    assert not any(call[2] == "-get" for call in run.calls)


def test_no_complete_export_does_not_change_serving(tmp_path):
    exports = tmp_path / "exports"
    serving = tmp_path / "serving"
    old = write_export(serving, 1)
    (serving / "current").symlink_to(old.name)
    (exports / "v2").mkdir(parents=True)

    assert pull.pull_latest(EXPORT_ROOT, serving, FakeHdfs(exports)) is None
    assert (serving / "current").resolve() == old.resolve()


def test_rejects_mismatched_manifest_version(tmp_path):
    exports = tmp_path / "exports"
    serving = tmp_path / "serving"
    old = write_export(serving, 1)
    (serving / "current").symlink_to(old.name)
    new = write_export(exports, 2)
    (new / "manifest.json").write_text(
        json.dumps({"version": 3, "docs": 1, "postings": 1}), encoding="utf-8"
    )

    with pytest.raises(ExportError):
        pull.pull_latest(EXPORT_ROOT, serving, FakeHdfs(exports))
    assert (serving / "current").resolve() == old.resolve()
    assert not (serving / ".incoming-v2").exists()
    assert not (serving / "v2").exists()


def test_failed_download_cleans_incoming_and_preserves_current(tmp_path):
    exports = tmp_path / "exports"
    serving = tmp_path / "serving"
    old = write_export(serving, 1)
    (serving / "current").symlink_to(old.name)
    write_export(exports, 2)
    run = FakeHdfs(exports)
    run.fail_get = True

    with pytest.raises(subprocess.CalledProcessError):
        pull.pull_latest(EXPORT_ROOT, serving, run)
    assert (serving / "current").resolve() == old.resolve()
    assert not (serving / ".incoming-v2").exists()


def test_missing_export_root_propagates_listing_failure(tmp_path):
    serving = tmp_path / "serving"
    old = write_export(serving, 1)
    (serving / "current").symlink_to(old.name)

    with pytest.raises(subprocess.CalledProcessError):
        pull.pull_latest(EXPORT_ROOT, serving, FakeHdfs(tmp_path / "missing"))
    assert (serving / "current").resolve() == old.resolve()


def test_first_pull_replaces_stale_incoming_directory(tmp_path):
    exports = tmp_path / "exports"
    serving = tmp_path / "serving"
    write_export(exports, 1)
    stale = serving / ".incoming-v1"
    stale.mkdir(parents=True)
    (stale / "partial").write_text("partial", encoding="utf-8")

    assert pull.pull_latest(EXPORT_ROOT, serving, FakeHdfs(exports)) == 1
    assert load_version(serving / "current").version == 1
    assert not stale.exists()
    assert not (serving / "v1" / "partial").exists()


def test_existing_local_version_can_be_reused_after_validation(tmp_path):
    exports = tmp_path / "exports"
    serving = tmp_path / "serving"
    old = write_export(serving, 1)
    (serving / "current").symlink_to(old.name)
    write_export(exports, 2)
    existing = write_export(serving, 2)

    assert pull.pull_latest(EXPORT_ROOT, serving, FakeHdfs(exports)) == 2
    assert (serving / "current").resolve() == existing.resolve()
    assert not (serving / ".incoming-v2").exists()


def test_existing_local_version_with_different_contents_is_rejected(tmp_path):
    exports = tmp_path / "exports"
    serving = tmp_path / "serving"
    old = write_export(serving, 1)
    (serving / "current").symlink_to(old.name)
    write_export(exports, 2)
    existing = write_export(serving, 2)
    (existing / "extra").write_text("different", encoding="utf-8")

    with pytest.raises(ExportError):
        pull.pull_latest(EXPORT_ROOT, serving, FakeHdfs(exports))
    assert (serving / "current").resolve() == old.resolve()
    assert not (serving / ".incoming-v2").exists()


def test_local_version_symlink_to_current_is_not_treated_as_served(tmp_path):
    exports = tmp_path / "exports"
    serving = tmp_path / "serving"
    old = write_export(serving, 1)
    (serving / "current").symlink_to(old.name)
    (serving / "v2").symlink_to(old.name)
    write_export(exports, 2)

    with pytest.raises(ExportError):
        pull.pull_latest(EXPORT_ROOT, serving, FakeHdfs(exports))
    assert (serving / "current").resolve() == old.resolve()
    assert not (serving / ".incoming-v2").exists()


def test_failed_atomic_switch_preserves_current_and_allows_retry(tmp_path, monkeypatch):
    exports = tmp_path / "exports"
    serving = tmp_path / "serving"
    old = write_export(serving, 1)
    (serving / "current").symlink_to(old.name)
    write_export(exports, 2)
    replace = pull.os.replace

    def fail_replace(source, destination):
        assert Path(source).is_symlink()
        assert Path(destination) == serving / "current"
        raise OSError("switch failed")

    monkeypatch.setattr(pull.os, "replace", fail_replace)
    with pytest.raises(OSError, match="switch failed"):
        pull.pull_latest(EXPORT_ROOT, serving, FakeHdfs(exports))
    assert (serving / "current").resolve() == old.resolve()
    assert not (serving / ".incoming-v2").exists()
    assert not list(serving.glob(".current-*"))

    monkeypatch.setattr(pull.os, "replace", replace)
    assert pull.pull_latest(EXPORT_ROOT, serving, FakeHdfs(exports)) == 2
    assert load_version(serving / "current").version == 2


def test_cli_posts_reload_after_every_successful_pull(tmp_path, monkeypatch):
    calls = []
    pull_calls = []
    results = iter([4, None])

    def pull_latest(root, serving):
        pull_calls.append((root, serving))
        return next(results)

    monkeypatch.setattr(pull, "pull_latest", pull_latest)

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def urlopen(request, **kwargs):
        calls.append((request.full_url, request.get_method()))
        return Response()

    monkeypatch.setattr(pull.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(
        sys, "argv",
        ["pull", "--export-root", EXPORT_ROOT, "--serving-root", str(tmp_path),
         "--reload-url", "http://localhost:8000/reload"],
    )
    pull.main()
    pull.main()
    assert pull_calls == [(EXPORT_ROOT, tmp_path), (EXPORT_ROOT, tmp_path)]
    assert calls == [
        ("http://localhost:8000/reload", "POST"),
        ("http://localhost:8000/reload", "POST"),
    ]


def test_cli_surfaces_reload_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(pull, "pull_latest", lambda root, serving: 1)

    def fail_reload(request, **kwargs):
        raise OSError("reload failed")

    monkeypatch.setattr(pull.urllib.request, "urlopen", fail_reload)
    monkeypatch.setattr(
        sys, "argv",
        ["pull", "--export-root", EXPORT_ROOT, "--serving-root", str(tmp_path),
         "--reload-url", "http://localhost:8000/reload"],
    )
    with pytest.raises(OSError, match="reload failed"):
        pull.main()


def test_module_cli_exposes_arguments():
    result = subprocess.run(
        [sys.executable, "-m", "dse.serving.pull", "--help"],
        check=True, capture_output=True, text=True,
    )
    assert "--export-root" in result.stdout
    assert "--serving-root" in result.stdout
    assert "--reload-url" in result.stdout
