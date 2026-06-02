"""``soft_delete`` load mode: merge upsert plus soft-delete absent rows."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from engine.errors import LoadError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

_DELETED_AT_COL = "_deleted_at"


def _sql_timestamp_literal(ts: datetime) -> str:
    """Format ``ts`` (UTC) as a Spark SQL expression for merge ``set`` clauses."""
    ts = ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)
    formatted = ts.strftime("%Y-%m-%d %H:%M:%S.%f")
    return f"CAST('{formatted}' AS TIMESTAMP)"


class SoftDeleteLoader:
    """Retain absent source rows in the target and stamp ``_deleted_at``."""

    def __init__(self, primary_keys: list[str]) -> None:
        """Create a loader bound to ``primary_keys``."""
        if not primary_keys:
            raise LoadError("SoftDeleteLoader requires at least one primary key.")
        self.primary_keys = primary_keys

    def _merge_condition(self) -> str:
        return " AND ".join(f"target.{k} = source.{k}" for k in self.primary_keys)

    def _ensure_deleted_at_column(self, spark: SparkSession, target_path: str) -> None:
        """Add ``_deleted_at`` to the target table if not already present.

        Zero-downtime migration for tables created before this mode existed.
        """
        schema = DeltaTable.forPath(spark, target_path).toDF().schema
        existing = {f.name for f in schema.fields}
        if _DELETED_AT_COL not in existing:
            spark.sql(
                f"ALTER TABLE delta.`{target_path}` "
                f"ADD COLUMN {_DELETED_AT_COL} TIMESTAMP"
            )

    def run(self, source: DataFrame, target_path: str) -> None:
        """Load ``source`` into the Delta table at ``target_path``."""
        spark = source.sparkSession
        deleted_at_now = datetime.now(UTC)

        self._ensure_deleted_at_column(spark, target_path)

        condition = self._merge_condition()

        source_with_marker = source.withColumn(
            _DELETED_AT_COL, F.lit(None).cast("timestamp")
        )

        (
            DeltaTable.forPath(spark, target_path)
            .alias("target")
            .merge(source_with_marker.alias("source"), condition)
            .whenMatchedUpdate(
                set={
                    **{col: f"source.{col}" for col in source.columns},
                    _DELETED_AT_COL: "NULL",
                }
            )
            .whenNotMatchedInsert(
                values={
                    **{col: f"source.{col}" for col in source.columns},
                    _DELETED_AT_COL: "NULL",
                }
            )
            .execute()
        )

        source_keys = source.select(*self.primary_keys)

        (
            DeltaTable.forPath(spark, target_path)
            .alias("target")
            .merge(source_keys.alias("source"), condition)
            .whenNotMatchedBySourceUpdate(
                condition=f"target.{_DELETED_AT_COL} IS NULL",
                set={_DELETED_AT_COL: _sql_timestamp_literal(deleted_at_now)},
            )
            .execute()
        )
