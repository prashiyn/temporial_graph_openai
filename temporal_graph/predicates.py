from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class PredicateConfig(BaseModel):
    predicates: dict[str, str] = Field(default_factory=dict)


def load_predicates(path: Path) -> dict[str, str]:
    with Path(path).open(encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    cfg = PredicateConfig.model_validate(raw)
    return dict(cfg.predicates)
