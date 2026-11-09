import argparse
import json
from typing import Sequence

from pyspark.sql import functions as F

from dse.config import CATALOG, DOCUMENTS_TABLE
from dse.jobs.ingest import ensure_tables, rebuild_links
from dse.jobs.state import current_snapshot
from dse.spark_session import build_session


def _emit(obj) -> None:
    print(json.dumps(obj, separators=(",", ":"), default=str))


def _live_count(spark, snapshot_id: int) -> int:
    return int(
        spark.sql(
            f"SELECT count(*) AS n FROM {DOCUMENTS_TABLE} VERSION AS OF {int(snapshot_id)} "
            "WHERE is_deleted = false"
        ).collect()[0]["n"]
    )


def _list(spark) -> int:
    rows = spark.sql(
        f"""
        SELECT h.snapshot_id, h.made_current_at, s.operation
        FROM {DOCUMENTS_TABLE}.history h
        LEFT JOIN {DOCUMENTS_TABLE}.snapshots s ON h.snapshot_id = s.snapshot_id
        ORDER BY h.made_current_at DESC
        """
    ).collect()
    current = current_snapshot(spark, DOCUMENTS_TABLE)
    for row in rows:
        sid = int(row["snapshot_id"])
        _emit(
            {
                "snapshot_id": sid,
                "made_current_at": row["made_current_at"].isoformat(),
                "operation": row["operation"],
                "is_current": sid == current,
                "live_documents": _live_count(spark, sid),
            }
        )
    return 0


def _show(spark, snapshot_id: int, doc_id: str) -> int:
    rows = (
        spark.sql(f"SELECT * FROM {DOCUMENTS_TABLE} VERSION AS OF {int(snapshot_id)}")
        .where(F.col("doc_id") == doc_id)
        .collect()
    )
    if not rows:
        _emit({"found": False})
        return 0
    out = rows[0].asDict()
    out["found"] = True
    _emit(out)
    return 0


def _rollback(spark, snapshot_id: int) -> int:
    current_ancestors = {
        int(r["snapshot_id"])
        for r in spark.sql(
            f"SELECT snapshot_id FROM {DOCUMENTS_TABLE}.history WHERE is_current_ancestor = true"
        ).collect()
    }
    if snapshot_id not in current_ancestors:
        _emit({"status": "error", "error": f"snapshot {snapshot_id} is not a current ancestor"})
        return 1
    spark.sql(
        f"CALL {CATALOG}.system.rollback_to_snapshot('search.documents', {int(snapshot_id)})"
    )
    link_count = rebuild_links(spark)
    _emit(
        {
            "status": "ok",
            "current_snapshot": current_snapshot(spark, DOCUMENTS_TABLE),
            "live_documents": _live_count(spark, snapshot_id),
            "link_count": link_count,
        }
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Iceberg time travel and rollback")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list")
    show = sub.add_parser("show")
    show.add_argument("--snapshot", type=int, required=True)
    show.add_argument("--doc-id", required=True)
    rb = sub.add_parser("rollback")
    rb.add_argument("--snapshot", type=int, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)

    spark = build_session("dse-snapshots")
    try:
        ensure_tables(spark)
        if args.command == "list":
            return _list(spark)
        if args.command == "show":
            return _show(spark, args.snapshot, args.doc_id)
        return _rollback(spark, args.snapshot)
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
