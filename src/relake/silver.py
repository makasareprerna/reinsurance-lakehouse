"""Silver: clean, validate, deduplicate and MERGE into one current row per key.

The steps for every entity:

1. clean      - trim text, upper-case codes, cast to real types
2. validate   - apply quality rules; error rows go to quarantine
3. deduplicate- drop exact duplicates, keep the latest version per key
4. merge      - upsert into silver; an update only wins if it is newer
                (so late or replayed files can never overwrite fresher data)
5. export     - write a Parquet snapshot that dbt reads

Step 4 is what makes the pipeline idempotent: running the same batch twice,
or backfilling a date range, leaves silver unchanged unless something newer
arrived.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from relake.config import Config
from relake.quality import RULES, apply_rules, check_error_rate, split_valid

LINEAGE = ["_source_file", "_ingest_date", "_ingested_at", "_run_id"]


def _trim_all(df: DataFrame) -> DataFrame:
    """Trim every string column and turn empty strings into NULL."""
    for name, dtype in df.dtypes:
        if dtype == "string" and not name.startswith("_"):
            df = df.withColumn(name, F.when(F.trim(name) == "", None).otherwise(F.trim(name)))
    return df


def _upper(df: DataFrame, *cols: str) -> DataFrame:
    for c in cols:
        df = df.withColumn(c, F.upper(c))
    return df


def _date(col: str) -> F.Column:
    return F.to_date(col, "yyyy-MM-dd")  # invalid dates such as 2026-02-31 become NULL


def clean_cedents(df: DataFrame) -> DataFrame:
    df = _upper(_trim_all(df), "country")
    return df.select("cedent_id", "cedent_name", "country",
                     F.to_timestamp("updated_at").alias("updated_at"), *LINEAGE)


def clean_quotes(df: DataFrame) -> DataFrame:
    df = _upper(_trim_all(df), "treaty_type", "line_of_business", "currency", "status")
    return df.select(
        "quote_id", "cedent_id", "treaty_type", "line_of_business", "currency",
        F.col("quoted_premium").cast("decimal(18,2)").alias("quoted_premium"),
        _date("quote_date").alias("quote_date"),
        "status",
        F.col("version").cast("int").alias("version"),
        F.to_timestamp("updated_at").alias("updated_at"),
        *LINEAGE,
    )


def clean_treaties(df: DataFrame) -> DataFrame:
    df = _upper(_trim_all(df), "treaty_type", "line_of_business", "currency", "status")
    return df.select(
        "treaty_id", "quote_id", "cedent_id", "treaty_type", "line_of_business", "currency",
        F.col("underwriting_year").cast("int").alias("underwriting_year"),
        _date("inception_date").alias("inception_date"),
        _date("expiry_date").alias("expiry_date"),
        F.col("premium").cast("decimal(18,2)").alias("premium"),
        F.col("cession_pct").cast("decimal(5,4)").alias("cession_pct"),
        F.col("retention").cast("decimal(18,2)").alias("retention"),
        F.col("limit_amount").cast("decimal(18,2)").alias("limit_amount"),
        "status",
        F.to_timestamp("updated_at").alias("updated_at"),
        *LINEAGE,
    )


def clean_claims(df: DataFrame) -> DataFrame:
    df = _upper(_trim_all(df), "currency", "claim_status")
    return df.select(
        "claim_id", "treaty_id",
        _date("loss_date").alias("loss_date"),
        _date("reported_date").alias("reported_date"),
        F.col("paid_amount").cast("decimal(18,2)").alias("paid_amount"),
        F.col("reserve_amount").cast("decimal(18,2)").alias("reserve_amount"),
        "currency", "claim_status",
        F.to_timestamp("updated_at").alias("updated_at"),
        *LINEAGE,
    )


@dataclass(frozen=True)
class EntitySpec:
    key: str
    clean: Callable[[DataFrame], DataFrame]


SPECS: dict[str, EntitySpec] = {
    "cedents": EntitySpec("cedent_id", clean_cedents),
    "quotes": EntitySpec("quote_id", clean_quotes),
    "treaties": EntitySpec("treaty_id", clean_treaties),
    "claims": EntitySpec("claim_id", clean_claims),
}


def deduplicate_latest(df: DataFrame, key: str) -> DataFrame:
    """Drop exact duplicates, then keep the newest version of each key.

    Ties on updated_at are broken by the most recently ingested file, so the
    result is deterministic.
    """
    business_cols = [c for c in df.columns if not c.startswith("_")]
    w = Window.partitionBy(key).orderBy(F.col("updated_at").desc(), F.col("_ingested_at").desc(),
                                        F.col("_source_file").desc())
    return (df.dropDuplicates(business_cols)
              .withColumn("_rn", F.row_number().over(w))
              .filter("_rn = 1").drop("_rn"))


def flag_unknown_treaties(spark: SparkSession, cfg: Config, claims: DataFrame) -> DataFrame:
    """Claims can arrive before their treaty (a late-arriving dimension).

    We keep them, tagged `treaty_not_yet_known`, instead of dropping real losses.
    dbt excludes them from loss ratios until the treaty shows up.
    """
    path = cfg.table("silver", "treaties")
    if not DeltaTable.isDeltaTable(spark, path):
        known = spark.createDataFrame([], "treaty_id string")
    else:
        known = spark.read.format("delta").load(path).select("treaty_id")
    known = known.withColumn("_treaty_known", F.lit(True))
    return (claims.join(F.broadcast(known), "treaty_id", "left")
                  .withColumn("_dq_warnings", F.when(F.col("_treaty_known").isNull(),
                                                     F.array_union("_dq_warnings", F.array(F.lit("treaty_not_yet_known"))))
                                               .otherwise(F.col("_dq_warnings")))
                  .drop("_treaty_known"))


def merge_into_silver(spark: SparkSession, cfg: Config, entity: str, df: DataFrame) -> int:
    """Upsert `df` into silver. Newer updated_at wins; older versions are ignored."""
    key = SPECS[entity].key
    path = cfg.table("silver", entity)
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").save(path)
        return df.count()
    (DeltaTable.forPath(spark, path).alias("t")
        .merge(df.alias("s"), f"t.{key} = s.{key}")
        .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
        .whenNotMatchedInsertAll()
        .execute())
    history = DeltaTable.forPath(spark, path).history(1).select("operationMetrics").first()
    metrics = history.operationMetrics or {}
    return int(metrics.get("numTargetRowsInserted", 0)) + int(metrics.get("numTargetRowsUpdated", 0))


def write_quarantine(cfg: Config, entity: str, rows: DataFrame) -> None:
    (rows.withColumn("_quarantined_at", F.current_timestamp())
         .write.format("delta").mode("append").option("mergeSchema", "true")
         .save(cfg.table("quarantine", entity)))


def export_for_dbt(spark: SparkSession, cfg: Config, entity: str) -> None:
    """Parquet snapshot of the current silver table for dbt/DuckDB to read."""
    path = cfg.table("silver", entity)
    if DeltaTable.isDeltaTable(spark, path):
        (spark.read.format("delta").load(path).coalesce(1)
              .write.mode("overwrite").parquet(str(cfg.exports / entity)))


def process(spark: SparkSession, cfg: Config, entity: str, batch: DataFrame,
            quarantine: bool = True) -> dict:
    """Run clean -> validate -> dedupe -> merge for one bronze batch.

    Backfills pass quarantine=False: those bad rows were already quarantined
    on their first load, so writing them again would only duplicate them.
    """
    spec = SPECS[entity]
    checked = apply_rules(spec.clean(batch), RULES[entity]).cache()
    total = checked.count()
    valid, quarantined = split_valid(checked)
    n_quarantined = quarantined.count()
    if n_quarantined and quarantine:
        write_quarantine(cfg, entity, quarantined)
    rate = check_error_rate(total, n_quarantined, cfg.max_error_rate, entity,
                            cfg.min_bad_rows_to_halt)  # may raise

    if entity == "claims":
        valid = flag_unknown_treaties(spark, cfg, valid)
    latest = deduplicate_latest(valid, spec.key)
    merged = merge_into_silver(spark, cfg, entity, latest)
    checked.unpersist()
    export_for_dbt(spark, cfg, entity)
    return {"silver_rows_merged": merged, "quarantined_rows": n_quarantined, "error_rate": rate}
