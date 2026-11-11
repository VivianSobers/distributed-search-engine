from __future__ import annotations

import json
import re
import sys
from typing import Any


_AVERAGE_RE = re.compile(r"Average block replication:\s*([0-9]+(?:\.[0-9]+)?)")
_UNDER_RE = re.compile(r"Under-replicated blocks:\s*(\d+)")
_MISSING_RE = re.compile(r"Missing blocks:\s*(\d+)")
_VERDICT_RE = re.compile(r"The filesystem under path .+ is (HEALTHY|CORRUPT)")


def parse_fsck(text: str) -> dict[str, Any]:
    average = _AVERAGE_RE.search(text)
    under = _UNDER_RE.search(text)
    missing = _MISSING_RE.search(text)
    verdict = _VERDICT_RE.search(text)
    if average is None or under is None or missing is None or verdict is None:
        raise ValueError("missing fsck field")
    return {
        "healthy": verdict.group(1) == "HEALTHY",
        "avg_replication": float(average.group(1)),
        "under_replicated": int(under.group(1)),
        "missing": int(missing.group(1)),
    }


def check_replication(text: str, expected: float = 2.0) -> None:
    parsed = parse_fsck(text)
    if not parsed["healthy"]:
        raise SystemExit("fsck not healthy")
    if parsed["avg_replication"] <= 0.0:
        raise SystemExit("empty directory cannot prove replication")
    if abs(float(parsed["avg_replication"]) - float(expected)) > 1e-9:
        raise SystemExit(
            f"avg replication mismatch: got {parsed['avg_replication']}, expected {expected}"
        )
    if int(parsed["under_replicated"]) != 0:
        raise SystemExit(f"under-replicated blocks: {parsed['under_replicated']}")
    if int(parsed["missing"]) != 0:
        raise SystemExit(f"missing blocks: {parsed['missing']}")


def _jps_roles(jps_text: str) -> set[str]:
    roles: set[str] = set()
    mapping = {
        "namenode": "NameNode",
        "datanode": "DataNode",
        "master": "Master",
        "worker": "Worker",
    }
    for raw in jps_text.splitlines():
        line = raw.strip()
        if not line:
            continue
        lower = line.lower()
        for needle, role in mapping.items():
            if needle in lower:
                roles.add(role)
    return roles


def check_placement(node: int, jps_text: str) -> None:
    expected = {
        1: set(),
        2: {"NameNode", "DataNode", "Worker"},
        3: {"Master", "Worker", "DataNode"},
        4: set(),
    }
    if node not in expected:
        raise SystemExit(f"unsupported node: {node}")
    got = _jps_roles(jps_text)
    if got != expected[node]:
        raise SystemExit(f"node {node} placement mismatch: expected {sorted(expected[node])}, got {sorted(got)}")


def _read_last_json(text: str) -> dict[str, Any]:
    for raw in reversed(text.splitlines()):
        line = raw.strip()
        if not line:
            continue
        if line.startswith("{") and line.endswith("}"):
            value = json.loads(line)
            if isinstance(value, dict):
                return value
    raise SystemExit("no JSON object found")


def check_health(text: str, version: int, docs: int) -> None:
    payload = _read_last_json(text)
    got_version = payload.get("version")
    got_docs = payload.get("docs")
    got_status = payload.get("status")
    if got_status != "ok" or got_version != int(version) or got_docs != int(docs):
        raise SystemExit(
            f"health mismatch: status={got_status}, version={got_version}, docs={got_docs}, expected version={version}, docs={docs}"
        )


def check_manifest(text: str, version: int, docs: int, postings: int) -> None:
    payload = _read_last_json(text)
    if payload.get("version") != int(version):
        raise SystemExit(f"manifest version mismatch: {payload.get('version')} != {version}")
    if payload.get("docs") != int(docs):
        raise SystemExit(f"manifest docs mismatch: {payload.get('docs')} != {docs}")
    if payload.get("postings") != int(postings):
        raise SystemExit(f"manifest postings mismatch: {payload.get('postings')} != {postings}")


def _usage() -> SystemExit:
    return SystemExit(
        "usage: python checks.py <parse-fsck|replication|placement|health|manifest> <args> < stdin"
    )


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        raise _usage()
    text = sys.stdin.read()
    name = args[0]
    if name == "parse-fsck":
        if len(args) != 1:
            raise _usage()
        print(json.dumps(parse_fsck(text), sort_keys=True))
        return 0
    if name == "replication":
        if len(args) > 2:
            raise _usage()
        expected = float(args[1]) if len(args) == 2 else 2.0
        check_replication(text, expected=expected)
        return 0
    if name == "placement":
        if len(args) != 2:
            raise _usage()
        check_placement(int(args[1]), text)
        return 0
    if name == "health":
        if len(args) != 3:
            raise _usage()
        check_health(text, version=int(args[1]), docs=int(args[2]))
        return 0
    if name == "manifest":
        if len(args) != 4:
            raise _usage()
        check_manifest(text, version=int(args[1]), docs=int(args[2]), postings=int(args[3]))
        return 0
    raise _usage()


if __name__ == "__main__":
    raise SystemExit(main())
