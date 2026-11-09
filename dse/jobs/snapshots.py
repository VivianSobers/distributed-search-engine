import argparse
import json
from typing import Sequence

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
        .where(f"doc_id = '{doc_id.replace(chr(39), chr(39) * 2)}'")
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
    known = {
        int(r["snapshot_id"])
        for r in spark.sql(f"SELECT snapshot_id FROM {DOCUMENTS_TABLE}.snapshots").collect()
    }
    if snapshot_id not in known:
        _emit({"status": "error", "error": f"unknown snapshot {snapshot_id}"})
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
