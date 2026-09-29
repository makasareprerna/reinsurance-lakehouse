"""Synthetic reinsurance source systems.

Simulates one business day at a time and drops files into the landing zone,
the way upstream systems would:

- cedents   (CSV, reference data, delivered once)
- quotes    (CSV, new quotes + status changes: QUOTED -> BOUND / DECLINED)
- treaties  (JSON lines, new treaties from bound quotes + endorsements)
- claims    (CSV loss bordereaux, new claims + development of open claims)

It deliberately injects the problems real feeds have: exact duplicates,
whitespace and lower-case codes, missing keys, negative amounts, invalid
dates, claims for treaties we have not received yet, and files that arrive
days late. The pipeline has to handle all of them.

State (the "source system databases") is kept in data/landing/_state.json so
each day builds on the previous one. Same seed + same dates = same files.
"""

from __future__ import annotations

import csv
import json
import math
import random
from datetime import date, datetime, timedelta
from pathlib import Path

LINES_OF_BUSINESS = ["PROPERTY", "CASUALTY", "MARINE", "SPECIALTY", "ENERGY"]
TREATY_TYPES = ["QUOTA_SHARE", "EXCESS_OF_LOSS", "SURPLUS"]
CURRENCIES = ["USD", "EUR", "GBP", "CHF", "JPY"]
COUNTRIES = ["US", "GB", "DE", "CH", "FR", "JP", "BM", "CA"]
NAME_A = ["Atlas", "Harbor", "Summit", "Keystone", "Pioneer", "Meridian", "Beacon", "Granite"]
NAME_B = ["Mutual", "General", "Casualty", "Assurance", "Insurance", "Indemnity"]

QUOTE_COLUMNS = [
    "quote_id", "cedent_id", "treaty_type", "line_of_business", "currency",
    "quoted_premium", "quote_date", "status", "version", "updated_at",
]
CLAIM_COLUMNS = [
    "claim_id", "treaty_id", "loss_date", "reported_date", "paid_amount",
    "reserve_amount", "currency", "claim_status", "updated_at",
]
CEDENT_COLUMNS = ["cedent_id", "cedent_name", "country", "updated_at"]

# Share of outgoing rows that get each defect. Tuned so the pipeline sees
# problems every day but stays under the 5% halt threshold.
DEFECT_RATES = {
    "duplicate": 0.015,
    "messy_codes": 0.03,
    "missing_key": 0.004,
    "negative_amount": 0.003,
    "bad_date": 0.003,
    "orphan_treaty": 0.006,
}
LATE_FILE_PROBABILITY = 0.15  # chance that part of a day's claims arrive 2 days late


def _ts(day: date, rng: random.Random) -> str:
    """A timestamp during business hours on `day`."""
    moment = datetime(day.year, day.month, day.day, 8) + timedelta(seconds=rng.randint(0, 10 * 3600))
    return moment.strftime("%Y-%m-%d %H:%M:%S")


def _money(rng: random.Random, median: float, spread: float = 0.9) -> float:
    return round(math.exp(rng.gauss(math.log(median), spread)), 2)


class SourceSimulator:
    def __init__(self, landing: Path, seed: int = 42, scale: float = 1.0):
        self.landing = Path(landing)
        self.seed = seed
        self.scale = scale
        self.state_file = self.landing / "_state.json"
        self.state = self._load_state()

    # ------------------------------------------------------------------ state
    def _load_state(self) -> dict:
        if self.state_file.exists():
            return json.loads(self.state_file.read_text())
        return {
            "last_day": None,
            "counters": {"quote": 0, "treaty": 0, "claim": 0},
            "cedents": [],
            "quotes": {},
            "treaties": {},
            "claims": {},
            "late_files": [],  # [{"deliver_on": d, "ingest_date": d, "rows": [...]}]
        }

    def _save_state(self) -> None:
        self.landing.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(json.dumps(self.state))

    def _next_id(self, kind: str, prefix: str) -> str:
        self.state["counters"][kind] += 1
        return f"{prefix}{self.state['counters'][kind]:07d}"

    # ------------------------------------------------------------- one day
    def generate_day(self, day: date) -> dict[str, int]:
        """Simulate `day` and write its files. Returns rows written per entity."""
        last = self.state["last_day"]
        if last and date.fromisoformat(last) >= day:
            raise ValueError(f"{day} already generated (last day was {last}); days must move forward")
        rng = random.Random(f"{self.seed}-{day.isoformat()}")
        written: dict[str, int] = {}

        if not self.state["cedents"]:
            written["cedents"] = self._write_cedents(day, rng)
            self._seed_existing_book(day, rng)

        quotes_out = self._simulate_quotes(day, rng)
        treaties_out = self._simulate_treaties(day, rng)
        claims_out = self._simulate_claims(day, rng)

        written["quotes"] = self._write_csv("quotes", day, f"quotes_{day}.csv", QUOTE_COLUMNS,
                                            self._inject_defects(quotes_out, "quote_id", "quoted_premium", "quote_date", rng))
        written["treaties"] = self._write_jsonl("treaties", day, f"treaties_{day}.jsonl",
                                                self._inject_defects(treaties_out, "treaty_id", "premium", "inception_date", rng))

        claims_out = self._inject_defects(claims_out, "claim_id", "paid_amount", "loss_date", rng, orphan=True)
        if claims_out and rng.random() < LATE_FILE_PROBABILITY:
            # Hold back ~10% of today's bordereau; it lands in today's folder two days late.
            cut = max(1, len(claims_out) // 10)
            self.state["late_files"].append(
                {"deliver_on": (day + timedelta(days=2)).isoformat(), "ingest_date": day.isoformat(), "rows": claims_out[:cut]}
            )
            claims_out = claims_out[cut:]
        written["claims"] = self._write_csv("claims", day, f"claims_bordereau_{day}.csv", CLAIM_COLUMNS, claims_out)
        written["claims_late"] = self._deliver_late_files(day)

        self.state["last_day"] = day.isoformat()
        self._save_state()
        return written

    # ------------------------------------------------------------ entities
    def _write_cedents(self, day: date, rng: random.Random) -> int:
        rows = []
        for i in range(1, 41):
            rows.append({
                "cedent_id": f"CED{i:04d}",
                "cedent_name": f"{rng.choice(NAME_A)} {rng.choice(NAME_B)} {i}",
                "country": rng.choice(COUNTRIES),
                "updated_at": _ts(day, rng),
            })
        self.state["cedents"] = [r["cedent_id"] for r in rows]
        return self._write_csv("cedents", day, "cedents.csv", CEDENT_COLUMNS, rows)

    def _new_treaty(self, day: date, rng: random.Random, inception: date, quote: dict | None) -> dict:
        treaty_type = quote["treaty_type"] if quote else rng.choice(TREATY_TYPES)
        premium = round((quote["quoted_premium"] if quote else _money(rng, 1_500_000)) * rng.uniform(0.9, 1.05), 2)
        is_xol = treaty_type == "EXCESS_OF_LOSS"
        return {
            "treaty_id": self._next_id("treaty", "TRT"),
            "quote_id": quote["quote_id"] if quote else None,
            "cedent_id": quote["cedent_id"] if quote else rng.choice(self.state["cedents"]),
            "treaty_type": treaty_type,
            "line_of_business": quote["line_of_business"] if quote else rng.choice(LINES_OF_BUSINESS),
            "currency": quote["currency"] if quote else rng.choice(CURRENCIES),
            "underwriting_year": inception.year,
            "inception_date": inception.isoformat(),
            "expiry_date": (inception + timedelta(days=364)).isoformat(),
            "premium": premium,
            "cession_pct": None if is_xol else round(rng.uniform(0.1, 0.6), 2),
            "retention": _money(rng, 5_000_000) if is_xol else None,
            "limit_amount": _money(rng, 25_000_000) if is_xol else None,
            "status": "ACTIVE",
            "updated_at": _ts(day, rng),
        }

    def _seed_existing_book(self, day: date, rng: random.Random) -> None:
        """Treaties already in force before the simulation starts (the in-force book)."""
        for _ in range(int(300 * self.scale)):
            inception = day - timedelta(days=rng.randint(30, 330))
            t = self._new_treaty(day, rng, inception, None)
            t["_new"] = True
            self.state["treaties"][t["treaty_id"]] = t

    def _simulate_quotes(self, day: date, rng: random.Random) -> list[dict]:
        out = []
        for q in self.state["quotes"].values():
            age = (day - date.fromisoformat(q["quote_date"])).days
            if q["status"] == "QUOTED" and age >= 3 and rng.random() < 0.25:
                q["status"] = "BOUND" if rng.random() < 0.35 else "DECLINED"
                q["version"] += 1
                q["updated_at"] = _ts(day, rng)
                out.append(dict(q))
                if q["status"] == "BOUND":
                    t = self._new_treaty(day, rng, day + timedelta(days=rng.randint(0, 30)), q)
                    t["_new"] = True
                    self.state["treaties"][t["treaty_id"]] = t
        for _ in range(int(120 * self.scale)):
            q = {
                "quote_id": self._next_id("quote", "QTE"),
                "cedent_id": rng.choice(self.state["cedents"]),
                "treaty_type": rng.choice(TREATY_TYPES),
                "line_of_business": rng.choice(LINES_OF_BUSINESS),
                "currency": rng.choice(CURRENCIES),
                "quoted_premium": _money(rng, 1_500_000),
                "quote_date": day.isoformat(),
                "status": "QUOTED",
                "version": 1,
                "updated_at": _ts(day, rng),
            }
            self.state["quotes"][q["quote_id"]] = q
            out.append(dict(q))
        return out

    def _simulate_treaties(self, day: date, rng: random.Random) -> list[dict]:
        out = []
        for t in self.state["treaties"].values():
            if t.pop("_new", False):
                out.append(dict(t))
            elif t["status"] == "ACTIVE" and rng.random() < 0.01:
                # Endorsement: premium adjusted mid-term. Same treaty_id, newer updated_at.
                t["premium"] = round(t["premium"] * rng.uniform(0.9, 1.1), 2)
                t["updated_at"] = _ts(day, rng)
                out.append(dict(t))
        return out

    def _simulate_claims(self, day: date, rng: random.Random) -> list[dict]:
        out = []
        for c in self.state["claims"].values():
            if c["claim_status"] == "OPEN" and rng.random() < 0.04:
                payment = round(c["reserve_amount"] * rng.uniform(0.2, 0.8), 2)
                c["paid_amount"] = round(c["paid_amount"] + payment, 2)
                c["reserve_amount"] = round(max(c["reserve_amount"] - payment, 0), 2)
                if c["reserve_amount"] < 1000:
                    c["reserve_amount"] = 0.0
                    c["claim_status"] = "CLOSED"
                c["updated_at"] = _ts(day, rng)
                out.append(dict(c))
        in_force = [t for t in self.state["treaties"].values() if date.fromisoformat(t["inception_date"]) <= day]
        for _ in range(int(250 * self.scale) if in_force else 0):
            t = rng.choice(in_force)
            earliest = max(date.fromisoformat(t["inception_date"]), day - timedelta(days=365))
            loss_date = earliest + timedelta(days=rng.randint(0, (day - earliest).days))
            c = {
                "claim_id": self._next_id("claim", "CLM"),
                "treaty_id": t["treaty_id"],
                "loss_date": loss_date.isoformat(),
                "reported_date": day.isoformat(),
                "paid_amount": 0.0 if rng.random() < 0.7 else _money(rng, 20_000),
                "reserve_amount": _money(rng, 150_000, 1.2),
                "currency": t["currency"],
                "claim_status": "OPEN",
                "updated_at": _ts(day, rng),
            }
            self.state["claims"][c["claim_id"]] = c
            out.append(dict(c))
        return out

    # -------------------------------------------------------------- defects
    def _inject_defects(self, rows: list[dict], key: str, amount: str, date_col: str,
                        rng: random.Random, orphan: bool = False) -> list[dict]:
        out = []
        for row in rows:
            r = dict(row)
            if rng.random() < DEFECT_RATES["messy_codes"]:
                r["currency"] = f"  {str(r['currency']).lower()} "
                if "line_of_business" in r:
                    r["line_of_business"] = str(r["line_of_business"]).title()
            if rng.random() < DEFECT_RATES["missing_key"]:
                r[key] = None
            if rng.random() < DEFECT_RATES["negative_amount"]:
                r[amount] = -abs(r[amount] or 1.0)
            if rng.random() < DEFECT_RATES["bad_date"]:
                r[date_col] = "2026-02-31"  # not a real date
            if orphan and rng.random() < DEFECT_RATES["orphan_treaty"]:
                r["treaty_id"] = "TRT9" + str(rng.randint(100000, 999999))  # treaty we never receive
            out.append(r)
            if rng.random() < DEFECT_RATES["duplicate"]:
                out.append(dict(r))  # exact duplicate row, as when a feed re-sends
        return out

    def _deliver_late_files(self, day: date) -> int:
        delivered, pending = 0, []
        for late in self.state["late_files"]:
            if late["deliver_on"] <= day.isoformat():
                ingest = date.fromisoformat(late["ingest_date"])
                delivered += self._write_csv("claims", ingest, f"claims_bordereau_{ingest}_late.csv",
                                             CLAIM_COLUMNS, late["rows"])
            else:
                pending.append(late)
        self.state["late_files"] = pending
        return delivered

    # --------------------------------------------------------------- writers
    def _folder(self, entity: str, day: date) -> Path:
        folder = self.landing / entity / f"ingest_date={day.isoformat()}"
        folder.mkdir(parents=True, exist_ok=True)
        return folder

    def _write_csv(self, entity: str, day: date, name: str, columns: list[str], rows: list[dict]) -> int:
        if not rows:
            return 0
        with open(self._folder(entity, day) / name, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        return len(rows)

    def _write_jsonl(self, entity: str, day: date, name: str, rows: list[dict]) -> int:
        if not rows:
            return 0
        with open(self._folder(entity, day) / name, "w") as fh:
            for r in rows:
                fh.write(json.dumps(r) + "\n")
        return len(rows)
