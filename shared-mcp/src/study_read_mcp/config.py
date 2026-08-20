from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class RepositoryConfig:
    math_root: Path = Path("/Users/xiazhibin/Documents/kaoyan-math")
    cs408_root: Path = Path("/Users/xiazhibin/Documents/kaoyan-408")
    english_root: Path = Path("/Users/xiazhibin/Documents/kaoyan-english")
    preprocessor_root: Path = Path("/Users/xiazhibin/.codex/study-intake-preprocessor")

    @classmethod
    def production(cls) -> "RepositoryConfig":
        return cls()
