"""Load pipeline configuration and resolve paths."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

REPO_ROOT = Path(os.environ.get("RELAKE_HOME", Path(__file__).resolve().parents[2]))


@dataclass(frozen=True)
class Config:
    seed: int
    start_date: str
    scale: float
    landing: Path
    bronze: Path
    silver: Path
    quarantine: Path
    control: Path
    exports: Path
    max_error_rate: float
    min_bad_rows_to_halt: int
    entities: tuple[str, ...]

    def table(self, layer: str, name: str) -> str:
        """Filesystem path of a Delta table, e.g. table('silver', 'claims')."""
        return str(getattr(self, layer) / name)


def load_config(path: str | Path | None = None, root: str | Path | None = None, **overrides) -> Config:
    """Read conf/pipeline.yml. `root` moves every data path (used by tests)."""
    cfg_path = Path(path) if path else REPO_ROOT / "conf" / "pipeline.yml"
    raw = yaml.safe_load(cfg_path.read_text())
    base = Path(root) if root else REPO_ROOT
    paths = {k: (base / v).resolve() for k, v in raw["paths"].items()}
    values = dict(
        seed=raw["seed"],
        start_date=str(raw["start_date"]),
        scale=float(raw["scale"]),
        max_error_rate=float(raw["quality"]["max_error_rate"]),
        min_bad_rows_to_halt=int(raw["quality"]["min_bad_rows_to_halt"]),
        entities=tuple(raw["entities"]),
        **paths,
    )
    values.update(overrides)
    return Config(**values)
