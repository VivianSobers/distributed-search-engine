from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _load_checks_module():
    path = Path(__file__).resolve().parents[1] / "deploy" / "rehearsal" / "checks.py"
    spec = importlib.util.spec_from_file_location("rehearsal_checks", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("unable to load checks.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


FSCK_HEALTHY = """\
Status: HEALTHY
Number of data-nodes:\t2
Total blocks (validated):\t20 (avg. block size 1234 B)
Minimally replicated blocks:\t20 (100.0 %)
Over-replicated blocks:\t0 (0.0 %)
Under-replicated blocks:\t0 (0.0 %)
Mis-replicated blocks:\t\t0 (0.0 %)
Default replication factor:\t2
Average block replication:\t2.0
Missing blocks:\t\t0
Corrupt blocks:\t\t0
The filesystem under path '/warehouse' is HEALTHY
"""

FSCK_UNDER_REPLICATED = """\
Status: HEALTHY
Number of data-nodes:\t2
Total blocks (validated):\t20 (avg. block size 1234 B)
Minimally replicated blocks:\t18 (90.0 %)
Over-replicated blocks:\t0 (0.0 %)
Under-replicated blocks:\t2 (10.0 %)
Mis-replicated blocks:\t\t0 (0.0 %)
Default replication factor:\t2
Average block replication:\t1.8
Missing blocks:\t\t0
Corrupt blocks:\t\t0
The filesystem under path '/warehouse' is HEALTHY
"""

FSCK_TRUNCATED = """\
Status: HEALTHY
Under-replicated blocks:\t0 (0.0 %)
"""


def test_parse_fsck_healthy_text():
    checks = _load_checks_module()
    parsed = checks.parse_fsck(FSCK_HEALTHY)
    assert parsed == {
        "healthy": True,
        "avg_replication": 2.0,
        "under_replicated": 0,
        "missing": 0,
    }
    checks.check_replication(FSCK_HEALTHY, expected=2.0)


def test_parse_fsck_under_replicated_text_and_replication_failure():
    checks = _load_checks_module()
    parsed = checks.parse_fsck(FSCK_UNDER_REPLICATED)
    assert parsed["healthy"] is True
    assert parsed["avg_replication"] == pytest.approx(1.8)
    assert parsed["under_replicated"] == 2
    with pytest.raises(SystemExit):
        checks.check_replication(FSCK_UNDER_REPLICATED, expected=2.0)


def test_parse_fsck_truncated_text_raises():
    checks = _load_checks_module()
    with pytest.raises(ValueError):
        checks.parse_fsck(FSCK_TRUNCATED)


@pytest.mark.parametrize(
    ("node", "jps_text"),
    [
        (1, "1234 Jps\n"),
        (2, "100 NameNode\n101 DataNode\n102 Worker\n200 Jps\n"),
        (3, "100 Master\n101 Worker\n102 DataNode\n200 Jps\n"),
        (4, "777 Jps\n"),
    ],
)
def test_check_placement_passes(node: int, jps_text: str):
    checks = _load_checks_module()
    checks.check_placement(node, jps_text)


@pytest.mark.parametrize(
    ("node", "jps_text"),
    [
        (1, "100 DataNode\n"),
        (2, "100 NameNode\n101 Worker\n"),
        (3, "100 Master\n101 Worker\n"),
        (4, "100 Worker\n"),
    ],
)
def test_check_placement_fails(node: int, jps_text: str):
    checks = _load_checks_module()
    with pytest.raises(SystemExit):
        checks.check_placement(node, jps_text)


def test_check_health_mismatch_exits():
    checks = _load_checks_module()
    with pytest.raises(SystemExit):
        checks.check_health('{"status":"ok","version":1,"docs":1000}', version=2, docs=975)


def test_check_manifest_mismatch_exits():
    checks = _load_checks_module()
    with pytest.raises(SystemExit):
        checks.check_manifest('{"version":2,"docs":975,"postings":427998}', version=2, docs=975, postings=1)
