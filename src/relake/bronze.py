"""Bronze: land raw files as-is, exactly once.

Bronze keeps every column as a string, plus lineage columns, so nothing is
lost if a source changes format. A file log (Delta table) records which files
were already loaded. That makes the load incremental and safe to re-run:
a file is picked up the first time the pipeline sees it, even if it arrives
days late into an older ingest_date folder.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from relake.config import Config

FILE_LOG_SCHEMA = StructType([
    StructField("file_path", StringType(), False),
    StructField("entity", StringType(), False),
    StructField("run_id", StringType(), False),
    StructField("loaded_at", TimestampType(), False),
])
INGEST_DATE = re.compile(r"ingest_date=(\d{4}-\d{2}-\d{2})")


def _file_log_path(cfg: Config) -> str:
    return cfg.table("control", "file_log")


def loaded_files(spark: SparkSession, cfg: Config, entity: str) -> set[str]:
    path = _file_log_path(cfg)
    if not DeltaTable.isDeltaTable(spark, path):
        return set()
    rows = spark.read.format("delta").load(path).filter(F.col("entity") == entity).select("file_path").collect()
    return {r.file_path for r in rows}


def discover_new_files(spark: SparkSession, cfg: Config, entity: str) -> list[str]:
    folder = cfg.landing / entity
    if not folder.exists():
        return []
    candidates = sorted(str(p.resolve()) for p in folder.rglob("*") if p.suffix in (".csv", ".jsonl"))
    already = loaded_files(spark, cfg, entity)
    return [f for f in candidates if f not in already]


def read_raw(spark: SparkSession, files: list[str]) -> DataFrame:
    """Read CSV or JSON-lines files with every column as string."""
    if files[0].endswith(".jsonl"):
        df = spark.read.option("primitivesAsString", "true").json(files)
    else:
        df = spark.read.option("header", "true").option("inferSchema", "false").csv(files)
    return df.withColumn("_source_file", F.col("_metadata.file_path"))


def ingest_entity(spark: SparkSession, cfg: Config, entity: str, run_id: str) -> int:
    """Append new landing files for `entity` to bronze. Returns rows loaded."""
    files = discover_new_files(spark, cfg, entity)
    if not files:
        return 0
    df = (
        read_raw(spark, files)
        .withColumn("_ingest_date", F.to_date(F.regexp_extract("_source_file", INGEST_DATE.pattern, 1)))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
    )
    rows = df.count()
    (df.write.format("delta").mode("append").option("mergeSchema", "true")
       .save(cfg.table("bronze", entity)))

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    log = spark.createDataFrame([(f, entity, run_id, now) for f in files], FILE_LOG_SCHEMA)
    log.write.format("delta").mode("append").save(_file_log_path(cfg))
    return rows


def bronze_batch(spark: SparkSession, cfg: Config, entity: str, *, run_id: str | None = None,
                 start: str | None = None, end: str | None = None) -> DataFrame | None:
    """Bronze rows for one run (normal load) or an ingest-date range (backfill)."""
    path = cfg.table("bronze", entity)
    if not Path(path).exists() or not DeltaTable.isDeltaTable(spark, path):
        return None
    df = spark.read.format("delta").load(path)
    if run_id:
        df = df.filter(F.col("_run_id") == run_id)
    if start and end:
        df = df.filter(F.col("_ingest_date").between(start, end))
    return df
