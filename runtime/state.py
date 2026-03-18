"""
Small JSON state store shared between worker and API processes.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


class RuntimeStateStore:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, state: Dict[str, Any]) -> None:
        tmp_path = self.path.with_suffix(f"{self.path.suffix}.tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(state, handle, sort_keys=True)
        tmp_path.replace(self.path)

    def read(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {}
        with self.path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

