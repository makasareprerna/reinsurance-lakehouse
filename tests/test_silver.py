from datetime import datetime

from pyspark.sql import functions as F

from relake.silver import clean_claims, deduplicate_latest, merge_into_silver

LINEAGE = dict(_source_file="f.csv", _ingest_date="2026-01-01", _run_id="r1")


def _claims(spark, rows):
    cols = ["claim_id", "treaty_id", "loss_date", "reported_date", "paid_amount",
            "reserve_amount", "currency", "claim_status", "updated_at"]
    df = spark.createDataFrame(rows, ", ".join(f"{c} string" for c in cols))
    for k, v in LINEAGE.items():
        df = df.withColumn(k, F.lit(v))
    return df.withColumn("_ingested_at", F.current_timestamp())


def test_clean_trims_uppercases_and_nulls_bad_dates(spark):
    raw = _claims(spark, [("C1", "T1", "2026-02-31", "2026-03-01", "10", "5", "  usd ", "open", "2026-03-01 10:00:00")])
    row = clean_claims(raw).first()
    assert row.currency == "USD" and row.claim_status == "OPEN"
    assert row.loss_date is None  # invalid date becomes NULL, then fails a quality rule


def test_deduplicate_keeps_latest_version(spark):
    raw = _claims(spark, [
        ("C1", "T1", "2026-01-01", "2026-01-02", "0", "100", "USD", "OPEN", "2026-01-02 09:00:00"),
        ("C1", "T1", "2026-01-01", "2026-01-02", "0", "100", "USD", "OPEN", "2026-01-02 09:00:00"),  # exact dup
        ("C1", "T1", "2026-01-01", "2026-01-02", "60", "40", "USD", "OPEN", "2026-01-05 09:00:00"),  # newer
    ])
    out = deduplicate_latest(clean_claims(raw).withColumn("_dq_warnings", F.array()), "claim_id").collect()
    assert len(out) == 1 and float(out[0].paid_amount) == 60


def test_merge_is_idempotent_and_ignores_older_versions(spark, cfg):
    def batch(paid, ts):
        raw = _claims(spark, [("C1", "T1", "2026-01-01", "2026-01-02", paid, "10", "USD", "OPEN", ts)])
        return clean_claims(raw).withColumn("_dq_warnings", F.array().cast("array<string>"))

    merge_into_silver(spark, cfg, "claims", batch("50", "2026-01-05 09:00:00"))
    merge_into_silver(spark, cfg, "claims", batch("50", "2026-01-05 09:00:00"))   # replay
    merge_into_silver(spark, cfg, "claims", batch("10", "2026-01-03 09:00:00"))   # late, older file
    rows = spark.read.format("delta").load(cfg.table("silver", "claims")).collect()
    assert len(rows) == 1
    assert float(rows[0].paid_amount) == 50
    assert rows[0].updated_at == datetime(2026, 1, 5, 9, 0, 0)
