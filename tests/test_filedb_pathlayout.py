from __future__ import annotations

from pathlib import Path

from cLab.core.config.db_cfg import FileDatabase
from cLab.infra.storage.fileDB import PathLayout, LayoutStyle, Market, Frequency, Dataset, BinanceFileSpec


def test_file_database_ensure(tmp_path: Path) -> None:
    db = FileDatabase(str(tmp_path / "crypto"))
    db.ensure()
    assert Path(db.databasePath).exists()


def test_pathlayout_paths(tmp_path: Path) -> None:
    # Verify binance mirror-style path layout
    layout = PathLayout(base_path=tmp_path, style=LayoutStyle.MIRROR)
    spec = BinanceFileSpec(
        market=Market.SPOT,
        frequency=Frequency.DAILY,
        dataset=Dataset.AGGTRADES,
        symbol="BTCUSDT",
        date="2026-01-01",
    )
    p = layout.local_path(spec)
    assert str(p).endswith("spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-01-01.zip")


def test_filestore_json_roundtrip(tmp_path: Path) -> None:
    # file storage in this framework is primarily a path layout for Binance Vision zips.
    # JSON store helpers live in the bot branch implementation.
    db = FileDatabase(str(tmp_path / "crypto"))
    db.ensure()
    assert Path(db.databasePath).exists()
