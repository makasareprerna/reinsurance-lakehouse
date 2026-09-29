"""Run log: one row per entity per pipeline run, queryable like any table.

Answers "what loaded, how much was rejected, did it fail, and when?"
without digging through console output.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType, TimestampType)

from relake.config import Config

RUN_LOG_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("mode", StringType(), False),          # run | backfill
    StructField("business_date", StringType(), True),
    StructField("entity", StringType(), False),
    StructField("bronze_rows", LongType(), True),
    StructField("silver_rows_merged", LongType(), True),
    StructField("quarantined_rows", LongType(), True),
    StructField("error_rate", DoubleType(), True),
    StructField("status", StringType(), False),        # SUCCESS | FAILED | NO_NEW_DATA
    StructField("message", StringType(), True),
    StructField("started_at", TimestampType(), False),
    StructField("finished_at", TimestampType(), False),
])


def now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def record(spark: SparkSession, cfg: Config, **row) -> None:
    values = tuple(row.get(f.name) for f in RUN_LOG_SCHEMA.fields)
    (spark.createDataFrame([values], RUN_LOG_SCHEMA)
          .write.format("delta").mode("append").save(cfg.table("control", "run_log")))
