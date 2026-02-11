from __future__ import annotations

from pathlib import Path

from cLab.core.config.database import FileDatabase
from cLab.infra.stores.fileDB import FileStore
from cLab.infra.stores.pathlayout import PathLayout


def test_file_database_ensure(tmp_path: Path) -> None:
    db = FileDatabase(str(tmp_path / "crypto"))
    db.ensure()
    assert Path(db.databasePath).exists()


def test_pathlayout_paths(tmp_path: Path) -> None:
    layout = PathLayout(str(tmp_path))
    p = layout.file_path("klines", "BTCUSDT", "2026-01-01", "part-0000.json")
    assert str(p).endswith("klines/BTCUSDT/2026-01-01/part-0000.json")


def test_filestore_json_roundtrip(tmp_path: Path) -> None:
    p = tmp_path / "a" / "b.json"
    s = FileStore(p)
    s.save_json({"x": 1})
    assert s.exists()
    assert s.load_json()["x"] == 1
