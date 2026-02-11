from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class FileStore:
    """A tiny file store for JSON-serializable objects."""

    def __init__(self, file_path: str | Path):
        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)

    def save_json(self, obj: Any) -> None:
        tmp = self.file_path.with_suffix(self.file_path.suffix + ".tmp")
        tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.file_path)

    def load_json(self) -> Any:
        if not self.file_path.exists():
            return None
        return json.loads(self.file_path.read_text(encoding="utf-8"))

    def exists(self) -> bool:
        return self.file_path.exists()
