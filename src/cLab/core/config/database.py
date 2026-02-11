from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# Default path from user requirement (Windows drive). Override via env when needed.
CRYPTOSDATABASEPATH = os.getenv("CRYPTOSDATABASEPATH", "G:/database/crypto/")


class FileDatabase:
    """File-based database using a folder as storage root."""

    def __init__(self, file_path: str = CRYPTOSDATABASEPATH):
        self.databasePath = str(file_path)

    def ensure(self) -> None:
        Path(self.databasePath).mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class DatabaseConfig:
    """Database configuration.

    - file_db_root: base folder for file-based storage
    - sqlite_path: small SQLite store for metadata/time-series
    """

    file_db_root: str
    sqlite_path: str

    @staticmethod
    def from_env() -> "DatabaseConfig":
        file_db_root = os.getenv("CLAB_FILE_DB_ROOT", CRYPTOSDATABASEPATH)
        sqlite_path = os.getenv("CLAB_SQLITE_PATH", "./data/clab.sqlite")
        return DatabaseConfig(file_db_root=file_db_root, sqlite_path=sqlite_path)
