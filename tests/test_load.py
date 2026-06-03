"""Reference tests for the existing load modes.

These tests show the shape of a Delta-backed test: build a source
DataFrame, seed the target once, run the loader, then assert on the
contents of the Delta table. Use them as a reference when testing a new
load mode.
"""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from engine.config.enums import LoadMode
from engine.load import get_loader


def _read(spark, path: str):
    return spark.read.format("delta").load(path)


def _rows(df, *cols):
    return sorted(tuple(r[c] for c in cols) for r in df.collect())


def test_full_overwrites_target(spark, delta_path):
    initial = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    initial.write.format("delta").save(delta_path)

    replacement = spark.createDataFrame([(3, "c")], ["id", "name"])
    get_loader(LoadMode.FULL, primary_keys=[]).run(replacement, delta_path)

    assert _rows(_read(spark, delta_path), "id", "name") == [(3, "c")]


@pytest.fixture
def seeded_target(spark, delta_path):
    """Seed the target Delta table with three customer rows."""
    seed = spark.createDataFrame(
        [
            (1, "alice@x.com", "NL"),
            (2, "bob@x.com", "BE"),
            (3, "carol@x.com", "NL"),
        ],
        ["customer_id", "email", "country"],
    )
    seed.write.format("delta").save(delta_path)
    return delta_path


def test_full_compare_inserts_updates_and_deletes(spark, seeded_target):
    source = spark.createDataFrame(
        [
            (1, "alice@x.com", "NL"),  # unchanged
            (2, "bob@new.com", "BE"),  # updated
            (4, "dave@x.com", "DE"),  # new — row 3 is absent
        ],
        ["customer_id", "email", "country"],
    )

    loader = get_loader(LoadMode.FULL_COMPARE, primary_keys=["customer_id"])
    loader.run(source, seeded_target)

    result = _rows(_read(spark, seeded_target), "customer_id", "email", "country")
    assert result == [
        (1, "alice@x.com", "NL"),
        (2, "bob@new.com", "BE"),
        (4, "dave@x.com", "DE"),
    ]


def test_full_compare_is_idempotent(spark, seeded_target):
    source = spark.createDataFrame(
        [(1, "alice@x.com", "NL"), (2, "bob@x.com", "BE"), (3, "carol@x.com", "NL")],
        ["customer_id", "email", "country"],
    )
    loader = get_loader(LoadMode.FULL_COMPARE, primary_keys=["customer_id"])

    loader.run(source, seeded_target)
    loader.run(source, seeded_target)

    assert _read(spark, seeded_target).count() == 3


def test_soft_delete_inserts_updates_and_marks_absent(spark, seeded_target):
    """Inserts, updates, soft-deletes absent rows; ``_deleted_at`` only when absent."""
    source = spark.createDataFrame(
        [
            (1, "alice@x.com", "NL"),
            (2, "bob@new.com", "BE"),
            (4, "dave@x.com", "DE"),
        ],
        ["customer_id", "email", "country"],
    )
    loader = get_loader(LoadMode.SOFT_DELETE, primary_keys=["customer_id"])
    loader.run(source, seeded_target)

    out = _read(spark, seeded_target)
    assert out.count() == 4

    active = {
        (r["customer_id"], r["email"], r["country"])
        for r in out.filter(F.col("_deleted_at").isNull()).collect()
    }
    assert active == {
        (1, "alice@x.com", "NL"),
        (2, "bob@new.com", "BE"),
        (4, "dave@x.com", "DE"),
    }

    deleted = out.filter(
        (F.col("customer_id") == 3) & F.col("_deleted_at").isNotNull()
    ).collect()
    assert len(deleted) == 1


def test_soft_delete_reappearance_clears_deleted_at(spark, seeded_target):
    """A row that was soft-deleted becomes active again when it returns to source."""
    shrink = spark.createDataFrame(
        [(1, "alice@x.com", "NL"), (2, "bob@x.com", "BE")],
        ["customer_id", "email", "country"],
    )
    restore = spark.createDataFrame(
        [
            (1, "alice@x.com", "NL"),
            (2, "bob@x.com", "BE"),
            (3, "carol@restored.com", "NL"),
        ],
        ["customer_id", "email", "country"],
    )
    loader = get_loader(LoadMode.SOFT_DELETE, primary_keys=["customer_id"])

    loader.run(shrink, seeded_target)
    loader.run(restore, seeded_target)

    row3 = (
        _read(spark, seeded_target)
        .filter("customer_id = 3")
        .select("email", "_deleted_at")
        .collect()[0]
    )
    assert row3["email"] == "carol@restored.com"
    assert row3["_deleted_at"] is None


def test_soft_delete_idempotent_for_deleted_timestamp(spark, seeded_target):
    """Re-running with the same source does not move ``_deleted_at`` on soft deletes."""
    shrink = spark.createDataFrame(
        [(1, "alice@x.com", "NL"), (2, "bob@x.com", "BE")],
        ["customer_id", "email", "country"],
    )
    loader = get_loader(LoadMode.SOFT_DELETE, primary_keys=["customer_id"])

    loader.run(shrink, seeded_target)
    first = (
        _read(spark, seeded_target)
        .filter("customer_id = 3")
        .select("_deleted_at")
        .collect()[0]["_deleted_at"]
    )

    loader.run(shrink, seeded_target)
    second = (
        _read(spark, seeded_target)
        .filter("customer_id = 3")
        .select("_deleted_at")
        .collect()[0]["_deleted_at"]
    )

    assert first == second


def test_soft_delete_idempotent_for_active_rows(spark, seeded_target):
    """Re-running with the full source leaves active rows unchanged."""
    source = spark.createDataFrame(
        [(1, "alice@x.com", "NL"), (2, "bob@x.com", "BE"), (3, "carol@x.com", "NL")],
        ["customer_id", "email", "country"],
    )
    loader = get_loader(LoadMode.SOFT_DELETE, primary_keys=["customer_id"])

    loader.run(source, seeded_target)
    after_first = _rows(
        _read(spark, seeded_target).filter(F.col("_deleted_at").isNull()),
        "customer_id",
        "email",
        "country",
    )

    loader.run(source, seeded_target)
    after_second = _rows(
        _read(spark, seeded_target).filter(F.col("_deleted_at").isNull()),
        "customer_id",
        "email",
        "country",
    )

    expected = [
        (1, "alice@x.com", "NL"),
        (2, "bob@x.com", "BE"),
        (3, "carol@x.com", "NL"),
    ]
    assert after_first == expected
    assert after_second == expected
    assert after_first == after_second
