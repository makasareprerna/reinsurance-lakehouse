"""Command-line entry point.

    python -m relake.cli generate --date 2026-01-01          # sources drop one day of files
    python -m relake.cli run --date 2026-01-01               # load new files: bronze -> silver
    python -m relake.cli simulate --days 14                  # generate + run, day by day
    python -m relake.cli backfill --start 2026-01-03 --end 2026-01-05
    python -m relake.cli dbt                                 # silver -> gold (dbt build)
    python -m relake.cli status                              # row counts and the last runs
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import uuid
from datetime import date, timedelta

from relake.config import REPO_ROOT, Config, load_config
from relake.generate import SourceSimulator


def cmd_generate(cfg: Config, day: date) -> None:
    written = SourceSimulator(cfg.landing, cfg.seed, cfg.scale).generate_day(day)
    print(f"[generate] {day}: {written}")


def run_pipeline(cfg: Config, business_date: str | None = None, *, backfill: tuple[str, str] | None = None) -> list[dict]:
    """Bronze -> silver for every entity. Returns the run-log rows written."""
    from relake import bronze, run_log, silver
    from relake.quality import DataQualityError
    from relake.spark import get_spark

    spark = get_spark()
    run_id = str(uuid.uuid4())
    mode = "backfill" if backfill else "run"
    results = []
    for entity in cfg.entities:
        started = run_log.now()
        row = dict(run_id=run_id, mode=mode, business_date=business_date, entity=entity,
                   started_at=started, bronze_rows=0, silver_rows_merged=0, quarantined_rows=0)
        try:
            if backfill:
                batch = bronze.bronze_batch(spark, cfg, entity, start=backfill[0], end=backfill[1])
            else:
                loaded = bronze.ingest_entity(spark, cfg, entity, run_id)
                row["bronze_rows"] = loaded
                batch = bronze.bronze_batch(spark, cfg, entity, run_id=run_id) if loaded else None
            if batch is None or batch.isEmpty():
                row.update(status="NO_NEW_DATA")
            else:
                if backfill:
                    row["bronze_rows"] = batch.count()
                row.update(silver.process(spark, cfg, entity, batch, quarantine=not backfill), status="SUCCESS")
        except DataQualityError as exc:
            row.update(status="FAILED", message=str(exc), finished_at=run_log.now())
            run_log.record(spark, cfg, **row)
            print(f"[{mode}] {entity}: FAILED - {exc}")
            raise
        row["finished_at"] = run_log.now()
        run_log.record(spark, cfg, **row)
        results.append(row)
        print(f"[{mode}] {entity}: {row['status']} bronze={row['bronze_rows']} "
              f"merged={row['silver_rows_merged']} quarantined={row['quarantined_rows']}")
    return results


def cmd_dbt(command: str = "build") -> None:
    """Run dbt against the silver exports (snapshot + models + tests)."""
    subprocess.run(["dbt", command, "--profiles-dir", "."], cwd=REPO_ROOT / "dbt", check=True)


def cmd_status(cfg: Config) -> None:
    from delta.tables import DeltaTable

    from relake.spark import get_spark

    spark = get_spark()
    for layer in ("bronze", "silver", "quarantine"):
        for entity in cfg.entities:
            path = cfg.table(layer, entity)
            if DeltaTable.isDeltaTable(spark, path):
                print(f"{layer:<11}{entity:<10}{spark.read.format('delta').load(path).count():>10,} rows")
    log = cfg.table("control", "run_log")
    if DeltaTable.isDeltaTable(spark, log):
        (spark.read.format("delta").load(log).orderBy("started_at", ascending=False)
              .select("mode", "business_date", "entity", "status", "bronze_rows",
                      "silver_rows_merged", "quarantined_rows").show(12, truncate=False))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="relake")
    parser.add_argument("--config", help="path to pipeline.yml")
    sub = parser.add_subparsers(dest="command", required=True)
    g = sub.add_parser("generate")
    g.add_argument("--date", required=True)
    r = sub.add_parser("run")
    r.add_argument("--date", required=True)
    s = sub.add_parser("simulate")
    s.add_argument("--start")
    s.add_argument("--days", type=int, default=14)
    s.add_argument("--scale", type=float)
    s.add_argument("--with-dbt", action="store_true",
                   help="run dbt build after every day, so the SCD2 snapshot records daily changes")
    b = sub.add_parser("backfill")
    b.add_argument("--start", required=True)
    b.add_argument("--end", required=True)
    sub.add_parser("status")
    sub.add_parser("dbt")
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    if args.command == "generate":
        cmd_generate(cfg, date.fromisoformat(args.date))
    elif args.command == "run":
        run_pipeline(cfg, args.date)
    elif args.command == "simulate":
        if args.scale:
            cfg = load_config(args.config, scale=args.scale)
        last = SourceSimulator(cfg.landing, cfg.seed, cfg.scale).state["last_day"]
        if args.start:
            day = date.fromisoformat(args.start)
        elif last:  # continue where the previous simulation stopped
            day = date.fromisoformat(last) + timedelta(days=1)
        else:
            day = date.fromisoformat(cfg.start_date)
        for _ in range(args.days):
            cmd_generate(cfg, day)
            run_pipeline(cfg, day.isoformat())
            if args.with_dbt:
                cmd_dbt()
            day += timedelta(days=1)
    elif args.command == "backfill":
        run_pipeline(cfg, None, backfill=(args.start, args.end))
    elif args.command == "dbt":
        cmd_dbt()
    elif args.command == "status":
        cmd_status(cfg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
