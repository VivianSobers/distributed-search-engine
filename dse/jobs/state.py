from dse.config import PIPELINE_STATE_TABLE


def _ensure_state_table(spark) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {PIPELINE_STATE_TABLE} (
            job STRING,
            source_snapshot BIGINT,
            updated_at TIMESTAMP
        ) USING iceberg
        """
    )


def current_snapshot(spark, table: str) -> int | None:
    rows = spark.sql(
        f"SELECT snapshot_id FROM {table}.history ORDER BY made_current_at DESC LIMIT 1"
    ).collect()
    if not rows:
        return None
    return int(rows[0]["snapshot_id"])


def last_snapshot(spark, job: str) -> int | None:
    _ensure_state_table(spark)
    rows = spark.sql(
        f"""
        SELECT source_snapshot
        FROM {PIPELINE_STATE_TABLE}
        WHERE job = '{job}'
        ORDER BY updated_at DESC
        LIMIT 1
        """
    ).collect()
    if not rows:
        return None
    return int(rows[0]["source_snapshot"])


def record_snapshot(spark, job: str, snapshot: int) -> None:
    _ensure_state_table(spark)
    spark.sql(
        f"""
        INSERT INTO {PIPELINE_STATE_TABLE}
        VALUES ('{job}', {int(snapshot)}, current_timestamp())
        """
    )
