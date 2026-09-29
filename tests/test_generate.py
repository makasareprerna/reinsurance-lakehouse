"""Generator tests: pure Python, no Spark needed."""

import csv
from datetime import date, timedelta

import pytest

from relake.generate import SourceSimulator


def _run(tmp_path, days=5, seed=7):
    sim = SourceSimulator(tmp_path / "landing", seed=seed, scale=0.2)
    d = date(2026, 1, 1)
    for _ in range(days):
        sim.generate_day(d)
        d += timedelta(days=1)
    return tmp_path / "landing"


def _files(landing):
    return sorted(p.relative_to(landing).as_posix() for p in landing.rglob("*") if p.is_file() and p.name != "_state.json")


def test_same_seed_same_files(tmp_path):
    a = _run(tmp_path / "a")
    b = _run(tmp_path / "b")
    assert _files(a) == _files(b)
    for rel in _files(a):
        assert (a / rel).read_text() == (b / rel).read_text()


def test_every_entity_lands_in_dated_folders(tmp_path):
    landing = _run(tmp_path)
    for entity in ("cedents", "quotes", "treaties", "claims"):
        assert list((landing / entity).glob("ingest_date=*/*")), entity


def test_days_must_move_forward(tmp_path):
    sim = SourceSimulator(tmp_path / "landing", seed=1, scale=0.1)
    sim.generate_day(date(2026, 1, 2))
    with pytest.raises(ValueError):
        sim.generate_day(date(2026, 1, 1))


def test_state_survives_restart(tmp_path):
    landing = tmp_path / "landing"
    SourceSimulator(landing, seed=3, scale=0.1).generate_day(date(2026, 1, 1))
    sim = SourceSimulator(landing, seed=3, scale=0.1)  # new process, same state file
    assert sim.state["last_day"] == "2026-01-01"
    assert sim.state["treaties"], "in-force book should be restored"


def test_feed_contains_defects_the_pipeline_must_handle(tmp_path):
    landing = _run(tmp_path, days=10)
    rows = []
    for f in (landing / "claims").rglob("*.csv"):
        with open(f) as fh:
            rows.extend(csv.DictReader(fh))
    ids = [r["claim_id"] for r in rows]
    assert len(ids) != len(set(ids)), "expected duplicates or updated versions"
    assert any(r["claim_id"] == "" for r in rows), "expected missing keys"
    assert any(r["treaty_id"].startswith("TRT9") for r in rows), "expected orphan treaties"
