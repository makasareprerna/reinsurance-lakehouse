"""Layered data-quality rules.

Every rule is a SQL expression that is TRUE for a good row. Severity decides
what happens when it is not:

- error: the row goes to quarantine and never reaches silver.
- warn:  the row goes to silver, tagged in `_dq_warnings`, so analysts can
         filter it and engineers can chase the root cause.

If the error share of a batch exceeds `max_error_rate`, the whole run stops
before silver is touched. That protects downstream reports from a broken feed
(for example, a source system that suddenly sends every amount as text).
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

VALID_CURRENCIES = "('USD','EUR','GBP','CHF','JPY')"
VALID_LOB = "('PROPERTY','CASUALTY','MARINE','SPECIALTY','ENERGY')"


@dataclass(frozen=True)
class Rule:
    name: str
    condition: str  # SQL, TRUE when the row is valid
    severity: str = "error"  # "error" or "warn"


RULES: dict[str, list[Rule]] = {
    "cedents": [
        Rule("cedent_id_present", "cedent_id IS NOT NULL"),
        Rule("cedent_name_present", "cedent_name IS NOT NULL", "warn"),
    ],
    "quotes": [
        Rule("quote_id_present", "quote_id IS NOT NULL"),
        Rule("quote_date_valid", "quote_date IS NOT NULL"),
        Rule("premium_positive", "quoted_premium > 0"),
        Rule("currency_known", f"currency IN {VALID_CURRENCIES}"),
        Rule("status_known", "status IN ('QUOTED','BOUND','DECLINED')"),
        Rule("lob_known", f"line_of_business IN {VALID_LOB}", "warn"),
    ],
    "treaties": [
        Rule("treaty_id_present", "treaty_id IS NOT NULL"),
        Rule("inception_date_valid", "inception_date IS NOT NULL"),
        Rule("premium_positive", "premium > 0"),
        Rule("currency_known", f"currency IN {VALID_CURRENCIES}"),
        Rule("expiry_after_inception", "expiry_date > inception_date"),
        Rule("xol_has_layer", "treaty_type <> 'EXCESS_OF_LOSS' OR (retention IS NOT NULL AND limit_amount IS NOT NULL)", "warn"),
    ],
    "claims": [
        Rule("claim_id_present", "claim_id IS NOT NULL"),
        Rule("treaty_id_present", "treaty_id IS NOT NULL"),
        Rule("loss_date_valid", "loss_date IS NOT NULL"),
        Rule("loss_not_after_report", "loss_date <= reported_date"),
        Rule("paid_non_negative", "paid_amount >= 0"),
        Rule("reserve_non_negative", "reserve_amount >= 0"),
        Rule("currency_known", f"currency IN {VALID_CURRENCIES}"),
        # treaty_known is added in silver.py because it needs a lookup against silver treaties.
    ],
}


class DataQualityError(RuntimeError):
    """Raised when a batch is too broken to load."""


def _failed(rule: Rule) -> Column:
    # NULL comparisons count as failures: coalesce(NULL, False) -> False.
    return F.when(~F.coalesce(F.expr(rule.condition), F.lit(False)), F.lit(rule.name))


def apply_rules(df: DataFrame, rules: list[Rule]) -> DataFrame:
    """Add `_dq_errors` and `_dq_warnings` (arrays of failed rule names)."""
    errors = [_failed(r) for r in rules if r.severity == "error"] or [F.lit(None).cast("string")]
    warns = [_failed(r) for r in rules if r.severity == "warn"] or [F.lit(None).cast("string")]
    return df.withColumn("_dq_errors", F.filter(F.array(*errors), lambda x: x.isNotNull())).withColumn(
        "_dq_warnings", F.filter(F.array(*warns), lambda x: x.isNotNull())
    )


def split_valid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (valid rows, quarantined rows). Input must have `_dq_errors`."""
    valid = df.filter(F.size("_dq_errors") == 0).drop("_dq_errors")
    quarantined = df.filter(F.size("_dq_errors") > 0)
    return valid, quarantined


def check_error_rate(total: int, quarantined: int, max_rate: float, entity: str,
                     min_bad_rows: int = 5) -> float:
    """Stop the run if too much of the batch is bad.

    `min_bad_rows` stops tiny batches from halting the pipeline: 1 bad row out
    of 4 is 25%, but it is noise, not a broken feed.
    """
    rate = quarantined / total if total else 0.0
    if rate > max_rate and quarantined >= min_bad_rows:
        raise DataQualityError(
            f"{entity}: {quarantined}/{total} rows ({rate:.1%}) failed error rules, "
            f"above the {max_rate:.0%} limit. Silver was not updated; see the quarantine table."
        )
    return rate
