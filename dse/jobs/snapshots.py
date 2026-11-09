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
