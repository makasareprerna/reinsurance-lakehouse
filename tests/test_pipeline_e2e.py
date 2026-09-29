"""End to end: generate 4 days, run the pipeline daily, then check the results."""

from datetime import date, timedelta

from pyspark.sql import functions as F

from relake.cli import cmd_generate, run_pipeline


def _silver(spark, cfg, entity):
    return spark.read.format("delta").load(cfg.table("silver", entity))


def test_daily_runs_backfill_and_replay(spark, cfg):
    day = date(2026, 1, 1)
    for _ in range(4):
        cmd_generate(cfg, day)
        results = run_pipeline(cfg, day.isoformat())
        assert all(r["status"] in ("SUCCESS", "NO_NEW_DATA") for r in results)
        day += timedelta(days=1)

    claims = _silver(spark, cfg, "claims")
    assert claims.count() > 0
    # one row per key in silver
    assert claims.groupBy("claim_id").count().filter("count > 1").count() == 0
    assert claims.filter(F.col("claim_id").isNull()).count() == 0
    # defects were caught, not loaded
    assert spark.read.format("delta").load(cfg.table("quarantine", "claims")).count() > 0

    # Running again with no new files changes nothing.
    before = claims.count()
    again = run_pipeline(cfg, "2026-01-04")
    assert {r["status"] for r in again} == {"NO_NEW_DATA"}
    assert _silver(spark, cfg, "claims").count() == before

    # A backfill over the whole range is idempotent too.
    run_pipeline(cfg, None, backfill=("2026-01-01", "2026-01-04"))
    assert _silver(spark, cfg, "claims").count() == before

    # Exports exist for dbt.
    for entity in ("cedents", "quotes", "treaties", "claims"):
        assert list((cfg.exports / entity).glob("*.parquet")), entity
