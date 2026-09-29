"""One place to build the local Spark session with Delta Lake enabled."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "reinsurance-lakehouse") -> SparkSession:
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Small data on a laptop: the default of 200 shuffle partitions only adds overhead.
        .config("spark.sql.shuffle.partitions", "4")
        # Keep Spark 3.x behaviour: an invalid cast returns NULL instead of failing the job.
        # Quality rules then catch the NULLs explicitly.
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
