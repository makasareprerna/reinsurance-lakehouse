import pytest

from relake.quality import DataQualityError, Rule, apply_rules, check_error_rate, split_valid

RULES = [
    Rule("id_present", "id IS NOT NULL"),
    Rule("amount_positive", "amount > 0"),
    Rule("name_present", "name IS NOT NULL", "warn"),
]


def test_errors_quarantine_and_warnings_pass_through(spark):
    df = spark.createDataFrame(
        [("a", 10.0, "x"), (None, 5.0, "y"), ("c", -1.0, None), ("d", 3.0, None)],
        "id string, amount double, name string",
    )
    valid, bad = split_valid(apply_rules(df, RULES))
    assert sorted(r.id for r in valid.collect()) == ["a", "d"]
    warned = {r.id: r._dq_warnings for r in valid.collect()}
    assert warned["d"] == ["name_present"] and warned["a"] == []
    reasons = {r.amount: r._dq_errors for r in bad.collect()}
    assert reasons[5.0] == ["id_present"]
    assert reasons[-1.0] == ["amount_positive"]


def test_null_comparison_counts_as_failure(spark):
    df = spark.createDataFrame([("a", None, "x")], "id string, amount double, name string")
    _, bad = split_valid(apply_rules(df, RULES))
    assert bad.count() == 1


def test_error_rate_limit():
    assert check_error_rate(100, 5, 0.05, "claims") == 0.05
    with pytest.raises(DataQualityError):
        check_error_rate(100, 6, 0.05, "claims")


def test_tiny_batches_do_not_halt():
    # 1 bad row out of 4 is 25%, but below the minimum bad-row count
    assert check_error_rate(4, 1, 0.05, "treaties", min_bad_rows=5) == 0.25
